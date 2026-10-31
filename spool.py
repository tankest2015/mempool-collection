"""spool.py - scrittura dei CSV per tabella, un file all'ora (UTC), con flush e fsync periodici

Il collettore scrive sempre qui; un processo separato (loader.py) copia i file nel database.
Se il disco da' errore le righe restano in un buffer limitato in memoria e si riprova (e si avvisa):
la raccolta non perde dati finche' il guasto e' breve
"""
import csv
import io
import os
import time
from collections import deque

import tables

NULL = "\\N"
BACKLOG_MAX = 500_000       # righe tenute in memoria se il disco non risponde


def fmt(v):
    if v is None:
        return NULL
    if v is True:
        return "t"
    if v is False:
        return "f"
    return v


class Spool:
    def __init__(self, root, flush_s=5, fsync_s=60, on_error=None, clock=time.time):
        self.root = root
        self.flush_s, self.fsync_s = flush_s, fsync_s
        self.on_error = on_error or (lambda msg: None)
        self.clock = clock
        self.files = {}                         # tabella -> (stamp, file, writer)
        self.rows = {t: 0 for t in tables.TABLES}
        self.bytes = 0
        self.backlog = deque()
        self.failed = False
        self.dropped = 0
        self.lastFlush = self.lastFsync = time.monotonic()
        os.makedirs(root, exist_ok=True)

    # ---------- file ----------
    def path(self, table, stamp):
        return os.path.join(self.root, table, f"{table}_{stamp}.csv")

    def _open(self, table):
        stamp = time.strftime("%Y%m%d_%H", time.gmtime(self.clock()))
        cur = self.files.get(table)
        if cur and cur[0] == stamp:
            return cur
        if cur:
            self._close(table)
        path = self.path(table, stamp)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f = open(path, "a+b")
        # dopo un crash l'ultima riga puo' essere troncata: la si taglia (il loader non l'ha mai letta)
        size = f.seek(0, os.SEEK_END)
        if size:
            tail = min(size, 1 << 16)
            f.seek(size - tail)
            data = f.read(tail)
            if not data.endswith(b"\n"):
                cut = data.rfind(b"\n")
                keep = size - tail + cut + 1 if cut >= 0 else 0
                f.truncate(keep)
                size = keep
        f.seek(0, os.SEEK_END)
        text = io.TextIOWrapper(f, encoding="utf-8", newline="", write_through=False)
        writer = csv.writer(text, lineterminator="\n")
        if size == 0:
            writer.writerow(tables.TABLES[table])
            text.flush()
        self.files[table] = (stamp, text, writer)
        return self.files[table]

    def _close(self, table):
        cur = self.files.pop(table, None)
        if cur:
            try:
                cur[1].flush()
                os.fsync(cur[1].buffer.fileno())
            except (OSError, ValueError):
                pass
            try:
                cur[1].close()
            except (OSError, ValueError):
                pass

    # ---------- scrittura ----------
    def write(self, table, row):
        """row = lista di valori nell'ordine di tables.TABLES[table]."""
        if len(row) != len(tables.TABLES[table]):
            raise ValueError(f"{table}: {len(row)} valori invece di {len(tables.TABLES[table])}")
        if self.failed:
            self._queue(table, row)
            return
        self._write(table, row)

    def _write(self, table, row):
        try:
            _, text, writer = self._open(table)
            writer.writerow([fmt(v) for v in row])
            self.rows[table] += 1
        except OSError as e:
            self._fail(e)
            self._queue(table, row)

    def _queue(self, table, row):
        if len(self.backlog) >= BACKLOG_MAX:
            self.backlog.popleft()
            self.dropped += 1
        self.backlog.append((table, row))

    def _fail(self, e):
        if not self.failed:
            self.failed = True
            self.on_error(f"errore di scrittura sul disco: {e}")

    def tick(self):
        """Da chiamare ogni secondo circa: svuota il buffer, forza il disco, riprova se c'e' stato un errore."""
        now = time.monotonic()
        if self.failed:
            try:
                self._drain()
            except OSError:
                return
        try:
            if now - self.lastFlush >= self.flush_s:
                for _, text, _ in self.files.values():
                    text.flush()
                self.lastFlush = now
            if now - self.lastFsync >= self.fsync_s:
                for _, text, _ in self.files.values():
                    text.flush()
                    os.fsync(text.buffer.fileno())
                self.lastFsync = now
        except OSError as e:
            self._fail(e)

    def _drain(self):
        """Riprova a scrivere le righe in coda. Se tutto va a buon fine il guasto e' finito."""
        # prova di scrittura: se questa fallisce, resta in errore
        probe = os.path.join(self.root, ".probe")
        with open(probe, "w") as f:
            f.write("x")
        os.remove(probe)
        self.failed = False
        n = 0
        while self.backlog and not self.failed:
            table, row = self.backlog.popleft()
            self._write(table, row)
            n += 1
        if not self.failed:
            self.on_error(f"disco di nuovo disponibile: scritte {n} righe in coda" + (
                f", {self.dropped} perse" if self.dropped else ""))

    def close(self):
        for t in list(self.files):
            self._close(t)
