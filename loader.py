#!/usr/bin/env python3
"""loader.py - copia i CSV del collettore (spool/) nel database Postgres/TimescaleDB.

    python loader.py                 # gira per sempre: ogni pochi secondi carica le righe nuove
    python loader.py --once          # un solo passaggio e poi esce
    python loader.py --init-schema   # crea le tabelle (sql/schema.sql) e poi parte; con --timescale anche le ipertabelle

Come funziona (esattamente una volta): per ogni file si ricorda fino a che byte si e' arrivati (tabella loader_state),
e l'avanzamento viene scritto NELLA STESSA transazione dei dati. Se il DB cade o il loader viene fermato, alla ripartenza
si riprende dal punto giusto, senza buchi e senza righe doppie. Si leggono solo righe complete (fino all'ultimo a capo)
Le tabelle con versioni (tx, blocks) si caricano con upsert: vince la riga con rev_ms piu' alto

Configurazione: DATABASE_URL (es. postgresql://utente:password@localhost:5432/mempool) o le variabili PG* standard;
vedi anche notify.py per gli avvisi
"""
import argparse
import calendar
import hashlib
import glob
import gzip
import logging
import os
import shutil
import sys
import time

import psycopg

import notify
import tables

log = logging.getLogger("loader")

ORDER = ["blocks", "tx", "unseen", "quality", "obs_gaps", "conn_accepted", "stats"]
CHUNK_BYTES = 32 * 1024 * 1024
DONE_GRACE_S = 180          # un file e' chiuso quando l'ora e' finita da almeno tanto
LAG_ALERT_MB = 200
LAG_ALERT_S = 600
DB_ALERT_S = 180


def q(name):
    return '"' + name.replace('"', '""') + '"'


def insertSql(table):
    """INSERT ... SELECT dalla tabella di appoggio `stg` alla tabella vera, con upsert."""
    cols = tables.TABLES[table]
    meta = tables.LOAD[table]
    names = [q(c) for c in cols]
    sel = ", ".join(names)
    target = list(names)
    expr = sel
    if meta["time"]:
        tcol, texpr = meta["time"]
        target.append(q(tcol))
        expr = f"{sel}, {texpr} AS {q(tcol)}"
    key = [q(k) for k in meta["key"]]
    keyList = ", ".join(key)
    # DISTINCT ON: dentro lo stesso blocco di file tiene una sola riga per chiave (la piu' recente se c'e' rev)
    order = f"ORDER BY {keyList}" + (f", {q(meta['rev'])} DESC" if meta["rev"] else "")
    sql = (f"INSERT INTO {q(table)} ({', '.join(target)}) "
           f"SELECT DISTINCT ON ({keyList}) * FROM (SELECT {expr} FROM stg) s {order} ")
    if meta["rev"]:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in target if c not in key)
        sql += (f"ON CONFLICT ({keyList}) DO UPDATE SET {sets} "
                f"WHERE EXCLUDED.{q(meta['rev'])} >= {q(table)}.{q(meta['rev'])}")
    else:
        sql += f"ON CONFLICT ({keyList}) DO NOTHING"
    return sql


class Loader:
    def __init__(self, dsn, spool, interval=5, gzipAfterH=48, deleteAfterDays=0, note=None):
        self.dsn, self.spool, self.interval = dsn, spool, interval
        self.gzipAfterH, self.deleteAfterDays = gzipAfterH, deleteAfterDays
        self.note = note or notify.Notifier("loader", hcEnv="HEALTHCHECK_LOADER_URL")
        self.conn = None
        self.state = {}                 # file -> (offset, done)
        self.badFiles = set()
        self.sql = {t: insertSql(t) for t in tables.TABLES}
        self.cols = {t: ", ".join(q(c) for c in tables.TABLES[t]) for t in tables.TABLES}
        self.dbDownSince = None
        self.lastDbLog = 0
        self.lagSince = None
        self.rows = 0
        self.sigs = {}
        self.lastFail = 0

    # ---------- connessione e stato ----------
    def connect(self):
        self.conn = psycopg.connect(self.dsn, autocommit=False, connect_timeout=10)
        with self.conn.cursor() as cur:
            cur.execute("SELECT file, byte_offset, done, sig FROM loader_state")
            rows = cur.fetchall()
            self.state = {f: (o, d) for f, o, d, _ in rows}
            self.sigs = {f: g for f, _, _, g in rows if g}
        self.conn.commit()

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def initSchema(self, timescale=False):
        here = os.path.dirname(os.path.abspath(__file__))
        files = ["schema.sql"] + (["schema_timescale.sql"] if timescale else [])
        conn = psycopg.connect(self.dsn, autocommit=True)
        for name in files:
            with open(os.path.join(here, "sql", name), encoding="utf-8") as f:
                conn.execute(f.read())
            log.info("schema applicato: %s", name)
        conn.close()

    # ---------- un file ----------
    def hourOf(self, path):
        stamp = os.path.basename(path).rsplit("_", 2)
        return calendar.timegm(time.strptime(stamp[-2] + stamp[-1].split(".")[0], "%Y%m%d%H"))

    def loadChunk(self, table, rel, data, newOffset, nrows):
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE stg ON COMMIT DROP AS SELECT {self.cols[table]} FROM {q(table)} WITH NO DATA")
                with cur.copy(f"COPY stg ({self.cols[table]}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
                    cp.write(data)
                cur.execute(self.sql[table])
                cur.execute("INSERT INTO loader_state (file, byte_offset, rows_loaded) VALUES (%s, %s, %s) "
                            "ON CONFLICT (file) DO UPDATE SET byte_offset = EXCLUDED.byte_offset, "
                            "rows_loaded = loader_state.rows_loaded + EXCLUDED.rows_loaded, updated_at = now()",
                            (rel, newOffset, nrows))

    def loadLines(self, table, rel, data, newOffset, path):
        """Ripiego quando un blocco ha righe malformate: riga per riga, le cattive vanno in spool/rejected/."""
        good = bad = 0
        badPath = os.path.join(self.spool, "rejected", os.path.basename(path) + ".bad")
        for line in data.splitlines(keepends=True):
            try:
                with self.conn.transaction():
                    with self.conn.cursor() as cur:
                        cur.execute(f"CREATE TEMP TABLE stg ON COMMIT DROP AS SELECT {self.cols[table]} FROM {q(table)} WITH NO DATA")
                        with cur.copy(f"COPY stg ({self.cols[table]}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as cp:
                            cp.write(line)
                        cur.execute(self.sql[table])
                good += 1
            except psycopg.DataError:
                bad += 1
                os.makedirs(os.path.dirname(badPath), exist_ok=True)
                with open(badPath, "ab") as f:
                    f.write(line)
        with self.conn.transaction():
            self.conn.execute("INSERT INTO loader_state (file, byte_offset, rows_loaded) VALUES (%s, %s, %s) "
                              "ON CONFLICT (file) DO UPDATE SET byte_offset = EXCLUDED.byte_offset, "
                              "rows_loaded = loader_state.rows_loaded + EXCLUDED.rows_loaded, updated_at = now()",
                              (rel, newOffset, good))
        self.note.sendSync(f"{bad} righe rifiutate in {os.path.basename(path)} (salvate in rejected/)", "badrows", 3600)
        return good

    @staticmethod
    def firstRowSig(path):
        """Impronta della prima riga dati (dopo l'intestazione): None se non e' ancora completa."""
        with open(path, "rb") as f:
            f.readline()
            row = f.readline()
        return hashlib.md5(row).hexdigest() if row.endswith(b"\n") else None

    def processFile(self, table, path):
        rel = os.path.relpath(path, self.spool)
        offset, done = self.state.get(rel, (0, False))
        size = os.path.getsize(path)
        if offset > 0:
            # stesso nome ma file diverso (spool svuotato o sostituito, DB riusato): si riparte da zero.
            # E' sicuro: gli upsert rendono innocuo ricaricare righe gia' presenti.
            cur = self.firstRowSig(path)
            known = self.sigs.get(rel)
            if size < offset or (known and cur and known != cur):
                log.warning("%s non corrisponde allo stato salvato (file sostituito?): lo rileggo da capo", rel)
                self.note.sendSync(f"{rel}: file diverso da quello gia' caricato, riletto da capo", "replaced:" + rel, 3600)
                with self.conn.transaction():
                    self.conn.execute("UPDATE loader_state SET byte_offset = 0, done = false, sig = NULL WHERE file = %s", (rel,))
                self.sigs.pop(rel, None)
                offset, done = 0, False
                self.state[rel] = (0, False)
        if done or rel in self.badFiles:
            return 0
        total = 0
        with open(path, "rb") as f:
            while True:
                if offset == 0:
                    head = f.readline()
                    if not head.endswith(b"\n"):
                        return total                                 # intestazione ancora incompleta
                    if head.decode("utf-8").rstrip("\n").split(",") != tables.TABLES[table]:
                        self.badFiles.add(rel)
                        self.note.sendSync(f"intestazione inattesa in {rel}: file saltato", "header:" + rel, 3600)
                        log.error("intestazione inattesa in %s", rel)
                        return total
                    offset = len(head)
                f.seek(offset)
                data = f.read(CHUNK_BYTES)
                cut = data.rfind(b"\n")
                if cut < 0:
                    break
                data = data[:cut + 1]
                newOffset = offset + len(data)
                nrows = data.count(b"\n")
                try:
                    self.loadChunk(table, rel, data, newOffset, nrows)
                    n = nrows
                except psycopg.OperationalError:
                    raise
                except psycopg.Error as e:
                    log.warning("blocco rifiutato in %s (%s): provo riga per riga", rel, type(e).__name__)
                    n = self.loadLines(table, rel, data, newOffset, path)
                offset = newOffset
                self.state[rel] = (offset, False)
                total += n
                if len(data) < CHUNK_BYTES:
                    break
        if offset > 0 and rel not in self.sigs:
            sig = self.firstRowSig(path)
            if sig:
                with self.conn.transaction():
                    self.conn.execute("UPDATE loader_state SET sig = %s WHERE file = %s", (sig, rel))
                self.sigs[rel] = sig
        # file chiuso: l'ora e' finita e tutto e' stato letto
        if offset >= os.path.getsize(path) and time.time() > self.hourOf(path) + 3600 + DONE_GRACE_S:
            with self.conn.transaction():
                self.conn.execute("UPDATE loader_state SET done = true WHERE file = %s", (rel,))
            self.state[rel] = (offset, True)
        return total

    # ---------- un passaggio completo ----------
    def passOnce(self):
        if self.conn is None:
            self.connect()
        total = lag = 0
        for table in ORDER:
            for path in sorted(glob.glob(os.path.join(self.spool, table, f"{table}_*.csv"))):
                total += self.processFile(table, path)
                rel = os.path.relpath(path, self.spool)
                off, done = self.state.get(rel, (0, False))
                if not done:
                    lag += max(0, os.path.getsize(path) - off)
        self.rows += total
        self.checkLag(lag)
        self.housekeeping()
        return total, lag

    def checkLag(self, lag):
        if lag > LAG_ALERT_MB * 1e6:
            self.lagSince = self.lagSince or time.time()
            if time.time() - self.lagSince > LAG_ALERT_S:
                self.note.sendSync(f"il caricatore e' indietro di {lag / 1e6:.0f} MB", "lag", 3600)
        else:
            self.lagSince = None

    def housekeeping(self):
        """Comprime i CSV vecchi gia' caricati (e, se richiesto, cancella quelli molto vecchi)."""
        if not self.gzipAfterH and not self.deleteAfterDays:
            return
        now = time.time()
        for rel, (off, done) in list(self.state.items()):
            if not done:
                continue
            path = os.path.join(self.spool, rel)
            if not os.path.exists(path):
                continue
            age = now - self.hourOf(path)
            if self.deleteAfterDays and age > self.deleteAfterDays * 86400:
                os.remove(path)
            elif self.gzipAfterH and age > self.gzipAfterH * 3600:
                with open(path, "rb") as src, gzip.open(path + ".gz", "wb", compresslevel=6) as dst:
                    shutil.copyfileobj(src, dst)
                os.remove(path)

    # ---------- ciclo ----------
    def run(self, once=False):
        lastPing = 0
        while True:
            try:
                total, lag = self.passOnce()
                if self.dbDownSince is not None:
                    self.dbDownSince = None
                    self.note.sendSync("database di nuovo raggiungibile, riprendo", "dbup", 0)
                if total:
                    log.info("caricate %d righe (in coda %.1f MB)", total, lag / 1e6)
                if time.time() - lastPing > 120:
                    lastPing = time.time()
                    self.note.pingSync()
            except psycopg.OperationalError as e:
                self.close()
                self.dbDownSince = self.dbDownSince or time.time()
                down = time.time() - self.dbDownSince
                if time.time() - self.lastDbLog > 30:                  # un solo messaggio ogni 30 s, in una riga
                    self.lastDbLog = time.time()
                    log.error("database non raggiungibile da %.0f s: %s", down, str(e).strip().splitlines()[0][:150])
                if down > DB_ALERT_S:
                    self.note.sendSync(f"database non raggiungibile da {down / 60:.0f} minuti", "dbdown", 3600)
                    if time.time() - self.lastFail > 60:                   # non una raffica di /fail a ogni giro
                        self.lastFail = time.time()
                        self.note.pingSync("/fail")
            if once:
                return
            time.sleep(self.interval)


def main(argv=None):
    notify.loadEnv()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=os.environ.get("DATABASE_URL", ""), help="stringa di connessione Postgres")
    ap.add_argument("--spool", default=os.environ.get("SPOOL_DIR", "spool"))
    ap.add_argument("--interval", type=float, default=5)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--init-schema", action="store_true")
    ap.add_argument("--timescale", action="store_true", help="con --init-schema: crea anche le ipertabelle")
    ap.add_argument("--gzip-after-hours", type=float, default=48, help="comprime i CSV gia' caricati dopo N ore (0 = mai)")
    ap.add_argument("--delete-after-days", type=float, default=0, help="cancella i CSV gia' caricati dopo N giorni (0 = mai)")
    a = ap.parse_args(argv)
    fmt = logging.Formatter("%(asctime)sZ %(message)s", "%Y-%m-%d %H:%M:%S")
    fmt.converter = time.gmtime
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(fmt)
    log.addHandler(h)
    log.setLevel(logging.INFO)
    ld = Loader(a.dsn, a.spool, a.interval, a.gzip_after_hours, a.delete_after_days)
    if a.init_schema:
        ld.initSchema(a.timescale)
    try:
        ld.run(once=a.once)
    except KeyboardInterrupt:
        pass
    finally:
        ld.close()


if __name__ == "__main__":
    main()
