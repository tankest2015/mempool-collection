#!/usr/bin/env python3
"""collector.py - raccolta continua della mempool (endpoint WebSocket pubblico) con etichette di esito

Ascolta le tx pending (newPendingTransactions) e i blocchi (newHeads), scarica ogni blocco completo e abbina
le due cose. Scrive SOLO file CSV orari (spool/<tabella>/...): li copia nel database loader.py
Le tx non incluse vengono controllate sul nodo a 10 minuti, 2 ore e 24 ore
La corsa non ha limite di blocchi: si ferma con Ctrl-C / SIGTERM (systemctl stop).

    python collector.py                       # usa .env o i valori predefiniti
    python collector.py --blocks 600          # per prove: si ferma dopo 600 blocchi
"""
import argparse
import asyncio
import datetime
import heapq
import json
import logging
import logging.handlers
import os
import shutil
import signal
import sys
import time
import tracemalloc
from collections import Counter, deque

import websockets

import notify
from spool import Spool

DEFAULT_URL = "wss://polygon-bor-rpc.publicnode.com"

# ---- controllo della connessione ----
CHECK_S = 15            # tempo per verificare una connessione nuova
MIN_RATE = 15           # tx/s minime se la catena non e' ancora misurabile (all'inizio)
MIN_COV = 0.50          # copertura stimata (tx/s feed / tx/s catena) richiesta a una connessione nuova
COV_FLOOR = 0.05        # sotto questa copertura una connessione non si accetta mai
COV_DECAY = 0.7         # a ogni connessione scartata di fila la soglia si moltiplica per questo
REEVAL_S = 300          # ogni quanto si ricontrolla la copertura di una connessione accettata
DEGRADE_FRAC = 0.4      # copertura sotto questa frazione di quella di riferimento
DEGRADE_WINDOWS = 2     # per tante finestre di fila: si riconnette
OVERESTIMATE_FRAC = 0.6 # prima finestra completa sotto questa frazione della soglia: la stima iniziale era gonfiata
DEGRADE_COOLDOWN_S = 900
QUALITY_EVERY_S = 60    # una riga "quality" al minuto
CHAIN_WINDOW_BLOCKS = 60
STALL_S = 5             # secondi senza messaggi prima di riconnettere
CONFIRM_S = 3
TX_STALL_S = 6          # secondi senza nessuna tx pending prima di riconnettere
DEAD_PAUSE_S = 1
POOR_PAUSE_S = 5
MAX_BACKOFF = 30
DEAD_REASONS = ("no_confirm", "stall", "tx_stall")

# ---- blocchi mancanti ----
MAX_RECOVER_PER_HEAD = 20
MAX_GAP_RECOVER = 1000
FINAL_TRIES, FINAL_MAX_S, FINAL_WINDOW = 3, 60, 20
STATE_MAX_AGE_S = 3600  # l'ultimo blocco di una corsa precedente si usa per recuperare il buco solo se piu' recente di cosi'

# ---- memoria ----
MINED_KEEP_BLOCKS = 600     # per quanti blocchi ricordare gli hash minati
BLOCKS_KEEP = 700           # blocchi completi tenuti in memoria
RETIRED_KEEP_S = 2 * 86400  # per quanto ricordare le tx "chiuse" (per non contarle di nuovo se il feed le ripete)

# ---- recheck integrato ----
CHECK_AGES_S = (600, 7200, 86400)   # eta' della tx alla 1a, 2a e 3a (ultima) verifica
RESOLVE_BATCH = 100
RPC_WINDOW = 20
RPC_TIMEOUT_S = 15
RESOLVE_RETRY_S = 30

# ---- monitoraggio ----
STATE_EVERY_S = 60
STATS_EVERY_S = 3600
NOBLOCK_ALERT_S = 180       # nessun blocco per tanto: avviso
PING_EVERY_S = 120
DISK_MIN_GB = 10
RSS_ALERT_MB = 800          # avviso se il processo supera tanta memoria

log = logging.getLogger("collector")


def hexToInt(h):
    return None if h is None else int(h, 16)


def chainRateOf(window):
    """tx/s della catena. `window` = lista di (numero, timestamp, n_tx). Usa solo l'ultimo tratto di blocchi consecutivi."""
    run = []
    for item in reversed(window):
        if run and item[0] != run[-1][0] - 1:
            break
        run.append(item)
    if len(run) < 4:
        return None
    run.reverse()
    span = run[-1][1] - run[0][1]
    if span <= 0:
        return None
    return sum(n for _, _, n in run[1:]) / span


def rssMb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        pass
    return None


class Rpc:
    """Richieste JSON-RPC a finestre su una connessione WebSocket."""

    def __init__(self, ws):
        self.ws = ws
        self.nextId = 1

    async def many(self, calls):
        results = [None] * len(calls)
        for start in range(0, len(calls), RPC_WINDOW):
            chunk = calls[start:start + RPC_WINDOW]
            pending = {}
            for k, (method, params) in enumerate(chunk):
                rid = self.nextId
                self.nextId += 1
                pending[rid] = start + k
                await self.ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}))
            while pending:
                msg = json.loads(await asyncio.wait_for(self.ws.recv(), timeout=RPC_TIMEOUT_S))
                rid = msg.get("id")
                if rid in pending:
                    idx = pending.pop(rid)
                    results[idx] = {"error": msg["error"]} if "error" in msg else msg.get("result")
        return results


async def main(cfg):
    run = int(time.time())                      # identifica questa esecuzione (i conn_id ripartono da 1 a ogni avvio)
    spool = Spool(cfg.spool, on_error=lambda m: note.fire(f"ATTENZIONE: {m}", "spool", 600))
    note = notify.Notifier("collector")
    statePath = os.path.join(cfg.spool, "state.json")
    url = cfg.url

    # ===== stato che PERSISTE tra le connessioni =====
    pool = {}                       # tx pending o non ancora chiuse
    blocks = {}                     # blocchi recenti (intestazione + dati)
    minedHash, minedByBlock = {}, {}
    requests = {}                   # richieste in volo
    attempts = {}
    missing = set()
    retired = {}                    # hash -> istante di chiusura (tx "chiuse" dal recheck)
    pendingByNonce = {}             # (mittente, nonce) -> hash pending: serve a riconoscere le sostituzioni
    openQueue = deque()             # (seen_mono, hash): coda per la prima verifica
    resolveHeap = []                # (istante, hash, n): verifiche in programma
    chainBlocks = deque(maxlen=CHAIN_WINDOW_BLOCKS)
    passedConns = set()
    reasons = Counter()             # motivi di interruzione delle connessioni (cumulativo)
    outcomes = Counter()            # esiti del recheck (cumulativo)

    rejects = 0
    lastDegrade = -1e9
    blockCounter = totalSeen = includedCount = unseenCount = 0
    duplicates = notInNode = notSeen = lateAnnounced = recovered = gaps = reannounced = gaveUp = finalRecovered = 0
    obsGapBlocks = 0
    connId = failures = deadStreak = 0
    backoff = 2
    lastHead = None
    lastBlockMono = time.monotonic()
    done = False
    stop = {"why": None}
    hourQ = [0.0, 0.0, 0]           # somma feed, somma catena, righe (per il riepilogo orario)
    resolverErrors = 0

    # ultimo blocco della corsa precedente: serve a recuperare i blocchi persi durante un riavvio
    try:
        with open(statePath, encoding="utf-8") as f:
            st = json.load(f)
        if time.time() - st.get("saved_at", 0) < STATE_MAX_AGE_S and st.get("last_head"):
            lastHead = int(st["last_head"])
            log.info("ripresa: ultimo blocco della corsa precedente %s", lastHead)
    except (OSError, ValueError):
        pass

    def chainRate():
        return chainRateOf(list(chainBlocks))

    def nowMs():
        return int(time.time() * 1000)

    # ---------- scrittura righe ----------
    def writeTx(h, r, outcome=None, n=None, nodeBlock=None):
        t = nowMs()
        spool.write("tx", [h, r["seen_ms"], run, r["conn_id"], r["probation"], r["from"], r["to"], r["nonce"],
                           r["type"], r["value"], r["gas"], r["gas_price"], r["tip_cap"], r["fee_cap"],
                           r["data_size"], r["selector"], r.get("block_number"), r.get("block_ts_ms"),
                           r.get("position"), r.get("delay_ms"), r.get("head_delay_ms"),
                           r.get("effective_gas_price"), r.get("effective_tip"), r.get("replaced_by"),
                           outcome, n, t if outcome else None, nodeBlock, t])

    def forget(h, r):
        k = (r["from"], r["nonce"])
        lst = pendingByNonce.get(k)
        if lst:
            try:
                lst.remove(h)
            except ValueError:
                pass
            if not lst:
                del pendingByNonce[k]

    def retire(h, r):
        pool.pop(h, None)
        forget(h, r)
        retired[h] = time.time()

    def tagReplaced(sender, nonce, winner):
        """La tx `winner` e' stata inclusa: le altre pending con lo stesso mittente e nonce sono state sostituite."""
        sibs = pendingByNonce.pop((sender, nonce), None)
        for sh in sibs or ():
            if sh != winner:
                r = pool.get(sh)
                if r is not None and "block_number" not in r:
                    r["replaced_by"] = winner

    def pruneMined(head):
        thr = head - MINED_KEEP_BLOCKS
        for n in [n for n in minedByBlock if n < thr]:
            for h in minedByBlock.pop(n):
                if minedHash.get(h) == n:
                    del minedHash[h]

    def pruneBlocks(head):
        thr = head - BLOCKS_KEEP
        for n in [n for n, b in blocks.items() if n < thr and "n_tx" in b]:
            del blocks[n]
        for n in [n for n in attempts if n < thr]:
            del attempts[n]

    # ---------- elaborazione di un blocco ----------
    def processBlock(num, fullBlock, srcConn):
        nonlocal notSeen, recovered, includedCount, unseenCount
        seenInBlock = 0
        blockRecord = blocks[num]
        reprocess = "n_tx" in blockRecord
        if "timestamp" not in blockRecord:              # blocco recuperato dopo un buco: l'intestazione viene da qui
            blockRecord["timestamp"] = int(fullBlock["timestamp"], 16)
            blockRecord["base_fee"] = hexToInt(fullBlock.get("baseFeePerGas"))
            blockRecord["gas_used"] = hexToInt(fullBlock.get("gasUsed"))
            blockRecord["gas_limit"] = hexToInt(fullBlock.get("gasLimit"))
            blockRecord["head_recv_ms"] = None
            recovered += 1

        txs = fullBlock["transactions"]
        hashesHere = []
        for pos, minedTx in enumerate(txs):
            h = minedTx["hash"]
            if reprocess and h in minedHash:
                continue
            minedHash[h] = num
            hashesHere.append(h)
            record = pool.get(h)
            if record is not None:
                seenInBlock += 1
                if "block_number" not in record:
                    record["block_number"] = num
                    record["block_ts_ms"] = blockRecord["timestamp"] * 1000
                    record["position"] = pos
                    record["delay_ms"] = record["block_ts_ms"] - record["seen_ms"]
                    record["head_delay_ms"] = (None if blockRecord["head_recv_ms"] is None
                                               else blockRecord["head_recv_ms"] - record["seen_ms"])
                    record["effective_gas_price"] = hexToInt(minedTx.get("gasPrice"))
                    if record["effective_gas_price"] is not None and blockRecord["base_fee"] is not None:
                        record["effective_tip"] = record["effective_gas_price"] - blockRecord["base_fee"]
                    includedCount += 1
                    tagReplaced(record["from"], record["nonce"], h)
                    writeTx(h, record, "included")
                    del pool[h]                         # e' su disco: non serve piu' in memoria
                    forget(h, record)
            else:
                notSeen += 1
                unseenCount += 1
                sender = minedTx["from"].lower()
                nonce = int(minedTx["nonce"], 16)
                gp = hexToInt(minedTx.get("gasPrice"))
                tip = None if gp is None or blockRecord["base_fee"] is None else gp - blockRecord["base_fee"]
                tagReplaced(sender, nonce, h)
                spool.write("unseen", [h, num, blockRecord["timestamp"], pos, len(txs), sender, nonce,
                                       minedTx["to"].lower() if minedTx.get("to") else None,
                                       minedTx["input"][:10], tip, run, srcConn])
        minedByBlock.setdefault(num, []).extend(hashesHere)

        blockRecord["n_tx"] = len(txs)
        if not reprocess and (not chainBlocks or blockRecord["timestamp"] > chainBlocks[-1][1]):
            chainBlocks.append((num, blockRecord["timestamp"], len(txs)))
        blockRecord["n_seen"] = blockRecord.get("n_seen", 0) + seenInBlock if reprocess else seenInBlock
        spool.write("blocks", [num, blockRecord["timestamp"], blockRecord["head_recv_ms"], blockRecord["base_fee"],
                               blockRecord["gas_used"], blockRecord["gas_limit"], run, blockRecord.get("conn_id"),
                               blockRecord.get("conn_seq"), blockRecord.get("observed"), blockRecord["n_tx"],
                               blockRecord["n_seen"], nowMs()])

    # ---------- recupero finale dei blocchi mancanti ----------
    async def finalRecovery():
        nonlocal finalRecovered
        todo = sorted(n for n in missing if n in blocks and "n_tx" not in blocks[n])
        if not todo:
            return
        log.info("recupero finale: %d blocchi mancanti (%d..%d)", len(todo), todo[0], todo[-1])
        t0 = time.monotonic()
        for attempt in range(1, FINAL_TRIES + 1):
            if not todo or time.monotonic() - t0 > FINAL_MAX_S:
                break
            try:
                async with websockets.connect(url, max_size=None, close_timeout=2) as ws:
                    rid = 1
                    for start in range(0, len(todo), FINAL_WINDOW):
                        pend = {}
                        for num in todo[start:start + FINAL_WINDOW]:
                            await ws.send(json.dumps({"id": rid, "jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                                                      "params": [hex(num), True]}))
                            pend[rid] = num
                            rid += 1
                        while pend:
                            left = FINAL_MAX_S - (time.monotonic() - t0)
                            if left <= 0:
                                raise asyncio.TimeoutError
                            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=min(10, left)))
                            num = pend.pop(msg.get("id"), None)
                            if num is None or msg.get("result") is None:
                                continue
                            processBlock(num, msg["result"], 0)
                            missing.discard(num)
                            finalRecovered += 1
            except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
                log.info("recupero finale, tentativo %d: %s", attempt, type(e).__name__)
            todo = [n for n in todo if "n_tx" not in blocks[n]]
            if todo and attempt < FINAL_TRIES:
                await asyncio.sleep(3)
        log.info("recupero finale: scaricati %d, ancora mancanti %d", finalRecovered, len(todo))

    # ---------- recheck integrato ----------
    def finish(h, r, n, outcome, nodeBlock=None):
        """Scrive l'esito di una verifica; chiude la tx se l'esito e' definitivo o se era l'ultima verifica."""
        outcomes[outcome] += 1
        r["_last"] = (outcome, n, nodeBlock)                    # ultimo esito noto: serve se la corsa si ferma
        writeTx(h, r, outcome, n, nodeBlock)
        if outcome in ("replaced", "nonce_consumed") or n >= len(CHECK_AGES_S):
            retire(h, r)
        else:
            due = max(time.time(), r["seen_ms"] / 1000 + CHECK_AGES_S[n])
            heapq.heappush(resolveHeap, (due, h, n + 1))

    async def resolver():
        nonlocal resolverErrors
        ws = rpc = None
        while True:
            try:
                if not resolveHeap or resolveHeap[0][0] > time.time():
                    await asyncio.sleep(1)
                    continue
                batch = []
                while resolveHeap and resolveHeap[0][0] <= time.time() and len(batch) < RESOLVE_BATCH:
                    batch.append(heapq.heappop(resolveHeap))
                work = []
                for _, h, n in batch:
                    r = pool.get(h)
                    if r is None or "block_number" in r:
                        continue                                # nel frattempo e' stata minata o chiusa
                    if r.get("replaced_by"):
                        finish(h, r, n, "replaced")
                    else:
                        work.append((h, r, n))
                if not work:
                    continue
                done_items = set()
                try:
                    if ws is None:
                        ws = await websockets.connect(url, max_size=None, close_timeout=2)
                        rpc = Rpc(ws)
                    resA = await rpc.many([("eth_getTransactionByHash", [h]) for h, _, _ in work])
                    need = [i for i, res in enumerate(resA) if not (isinstance(res, dict) and res.get("blockNumber"))]
                    resB = await rpc.many([("eth_getTransactionCount", [work[i][1]["from"], "latest"]) for i in need])
                    nonceNow = {i: hexToInt(res) if isinstance(res, str) else None for i, res in zip(need, resB)}
                    for i, (h, r, n) in enumerate(work):
                        res = resA[i]
                        done_items.add(h)
                        if h not in pool or "block_number" in r:
                            continue
                        if isinstance(res, dict) and res.get("blockNumber"):
                            finish(h, r, n, "mined_late", hexToInt(res["blockNumber"]))
                        elif isinstance(res, dict) and res.get("hash"):
                            nn = nonceNow.get(i)
                            finish(h, r, n, "pending_nonce_gap" if nn is not None and nn < r["nonce"]
                                   else "pending_executable")
                        else:
                            nn = nonceNow.get(i)
                            finish(h, r, n, "nonce_consumed" if nn is not None and nn > r["nonce"]
                                   else "dropped_unknown")
                    resolverErrors = 0
                except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
                    resolverErrors += 1
                    if resolverErrors in (10, 100):
                        note.fire(f"recheck: {resolverErrors} errori di fila ({type(e).__name__})", "resolver", 1800)
                    if ws is not None:
                        try:
                            await ws.close()
                        except Exception:
                            pass
                        ws = rpc = None
                    for _, h, n in batch:                       # le non finite si riprovano tra poco
                        if h not in done_items and h in pool:
                            heapq.heappush(resolveHeap, (time.time() + RESOLVE_RETRY_S, h, n))
                    await asyncio.sleep(RESOLVE_RETRY_S)
            except asyncio.CancelledError:
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                raise

    # ---------- stato su disco, riepilogo orario, avvisi ----------
    def diskFreeGb():
        try:
            return round(shutil.disk_usage(cfg.spool).free / 1e9, 1)
        except OSError:
            return None

    def writeState():
        try:
            tmp = statePath + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"run": run, "last_head": lastHead, "saved_at": time.time(), "pool": len(pool),
                           "blocks": blockCounter}, f)
            os.replace(tmp, statePath)
        except OSError as e:
            log.warning("state.json non scritto: %s", e)

    prev = {}

    def writeStats(final=False):
        nonlocal hourQ
        snap = {"blocks": blockCounter, "tx_seen": totalSeen, "included": includedCount, "unseen": unseenCount,
                "not_in_node": notInNode, "late_announced": lateAnnounced, "duplicates": duplicates, "gaps": gaps,
                "recovered": recovered, "reannounced": reannounced, "gave_up": gaveUp, "connections": connId,
                "accepted_conns": len(passedConns), "obs_gap_blocks": obsGapBlocks}
        rss, free = rssMb(), diskFreeGb()
        spool.write("stats", [nowMs(), run] + list(snap.values()) + [len(pool), rss, free, json.dumps(dict(reasons))])
        d = {k: v - prev.get(k, 0) for k, v in snap.items()}
        cov = "n/d" if not hourQ[2] or not hourQ[1] else f"{100 * hourQ[0] / hourQ[1]:.0f}%"
        feed = "n/d" if not hourQ[2] else f"{hourQ[0] / hourQ[2]:.1f}"
        chain = "n/d" if not hourQ[2] else f"{hourQ[1] / hourQ[2]:.1f}"
        text = (f"{'RIEPILOGO FINALE' if final else 'Riepilogo orario'} ({time.strftime('%Y-%m-%d %H:%M', time.gmtime())} UTC)\n"
                f"blocchi +{d['blocks']} | tx viste +{d['tx_seen']} | incluse +{d['included']} | non viste +{d['unseen']}\n"
                f"copertura media {cov} (feed {feed}/s, catena {chain}/s)\n"
                f"connessioni +{d['connections']} (accettate in totale {len(passedConns)}) | buchi +{d['obs_gap_blocks']} blocchi | "
                f"recuperati +{d['recovered']}\n"
                f"esiti recheck (totale): {dict(outcomes)}\n"
                f"RAM {rss} MB | pool {len(pool)} | disco libero {free} GB | righe CSV in coda {len(spool.backlog)}")
        prev.update(snap)
        hourQ = [0.0, 0.0, 0]
        log.info(text.replace("\n", " | "))
        return text

    def scanUnresolved(now):
        while openQueue and now - openQueue[0][0] > CHECK_AGES_S[0]:
            _, h = openQueue.popleft()
            r = pool.get(h)
            if r is not None and "block_number" not in r:
                heapq.heappush(resolveHeap, (time.time(), h, 1))

    def memReport(prevSnap):
        """Diagnostica opzionale (--trace-mem): dove cresce la memoria rispetto all'ultimo controllo."""
        snap = tracemalloc.take_snapshot()
        if prevSnap is not None:
            for st in snap.compare_to(prevSnap, "lineno")[:8]:
                log.info("MEM %+.1f KB  %s", st.size_diff / 1024, st.traceback[0])
        log.info("MEM contenuti: pool %d, retired %d, pendingByNonce %d, minedHash %d, blocks %d, resolveHeap %d, "
                 "openQueue %d, missing %d", len(pool), len(retired), len(pendingByNonce), len(minedHash),
                 len(blocks), len(resolveHeap), len(openQueue), len(missing))
        return snap

    async def ticker():
        t0 = time.monotonic()
        lastScan = lastState = lastStats = lastPing = lastMem = t0
        noBlocks = False
        memSnap = None
        if cfg.trace_mem:
            tracemalloc.start(10)
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            spool.tick()
            if now - lastScan >= 10:
                lastScan = now
                scanUnresolved(now)
                cutoff = time.time() - RETIRED_KEEP_S
                if len(retired) > 1000:
                    for h in [h for h, t in retired.items() if t < cutoff]:
                        del retired[h]
            if now - lastState >= STATE_EVERY_S:
                lastState = now
                writeState()
                age = now - lastBlockMono
                if age > NOBLOCK_ALERT_S and not noBlocks:
                    noBlocks = True
                    note.fire(f"nessun blocco da {age / 60:.0f} minuti (connessioni provate: {connId})", "noblocks", 900)
                elif age <= NOBLOCK_ALERT_S and noBlocks:
                    noBlocks = False
                    note.fire("blocchi ripresi", "resumed", 0)
                rss = rssMb()
                if rss is not None and rss > RSS_ALERT_MB:
                    note.fire(f"memoria alta: {rss} MB (pool {len(pool)}, retired {len(retired)})", "rss", 6 * 3600)
                free = diskFreeGb()
                if free is not None and free < DISK_MIN_GB:
                    note.fire(f"spazio su disco quasi finito: {free} GB liberi", "disk", 6 * 3600)
            if now - lastPing >= PING_EVERY_S:
                lastPing = now
                if blockCounter > 0 and now - lastBlockMono <= NOBLOCK_ALERT_S:      # il segnale di vita parte solo se i blocchi arrivano
                    await note.ping()
            if cfg.trace_mem and now - lastMem >= cfg.trace_mem:
                lastMem = now
                memSnap = memReport(memSnap)
            if now - lastStats >= STATS_EVERY_S:
                lastStats = now
                note.fire(writeStats())

    log.info("avvio: run %d, endpoint %s, spool %s, verifiche a %s s, %s", run, url, cfg.spool, CHECK_AGES_S,
             "senza limite di blocchi" if not cfg.blocks else f"{cfg.blocks} blocchi")
    note.fire(f"avviato (run {run}, endpoint {url})", "start", 120)      # in un ciclo di riavvii, al massimo un avviso ogni 2 minuti
    tickTask = asyncio.create_task(ticker())
    resTask = asyncio.create_task(resolver())

    try:
        while not done:
            connId += 1

            # ===== stato PER-CONNESSIONE =====
            types = {}
            requests.clear()
            nextId = 3
            checking = True
            reason = None
            gotMsg = False
            connTx = connBlocks = 0
            connStart = last_msg = lastTx = time.monotonic()
            attemptStart = connStart
            connCov = connThr = None
            winStart = qStart = time.monotonic()
            winTx = qTx = 0
            baseCov = None
            badWin = 0

            try:
                async with websockets.connect(url, max_size=None, close_timeout=2) as ws:
                    connStart = last_msg = lastTx = time.monotonic()
                    await ws.send(json.dumps({"id": 1, "jsonrpc": "2.0", "method": "eth_subscribe",
                                              "params": ["newPendingTransactions", True]}))
                    await ws.send(json.dumps({"id": 2, "jsonrpc": "2.0", "method": "eth_subscribe",
                                              "params": ["newHeads"]}))

                    while True:
                        now = time.monotonic()
                        elapsed = now - connStart

                        # --- verifica iniziale della connessione ---
                        if checking and elapsed > CHECK_S:
                            rate = connTx / elapsed
                            cr = chainRate()
                            connCov = None if not cr else rate / cr
                            connThr = max(COV_FLOOR, MIN_COV * COV_DECAY ** rejects)
                            ok = (rate >= MIN_RATE) if connCov is None else (connCov >= connThr)
                            if not ok:
                                log.info("conn %d: %.1f tx/s, copertura stimata %s sotto la soglia %.0f%%, scartata",
                                         connId, rate, "n/d" if connCov is None else f"{connCov * 100:.0f}%", connThr * 100)
                                reason = "low_rate"
                                rejects += 1
                                break
                            checking = False
                            rejects = 0
                            winStart, winTx = now, 0
                            passedConns.add(connId)
                            spool.write("conn_accepted", [run, connId, nowMs()])
                            log.info("conn %d accettata: %.1f tx/s, copertura stimata %s", connId, rate,
                                     "n/d" if connCov is None else f"{connCov * 100:.0f}% (soglia {connThr * 100:.0f}%)")

                        # --- riga di qualita' e rivalutazione periodica ---
                        if now - qStart >= QUALITY_EVERY_S:
                            cr = chainRate()
                            feed = qTx / (now - qStart)
                            spool.write("quality", [nowMs(), run, connId, round(feed, 2),
                                                    None if cr is None else round(cr, 2),
                                                    None if not cr else round(feed / cr, 3), not checking])
                            if cr is not None:
                                hourQ[0] += feed
                                hourQ[1] += cr
                                hourQ[2] += 1
                            qStart, qTx = now, 0
                        if not checking and now - winStart >= REEVAL_S:
                            cr = chainRate()
                            winCov = None if not cr else (winTx / (now - winStart)) / cr
                            winStart, winTx = now, 0
                            if winCov is not None:
                                if baseCov is None:
                                    baseCov = winCov
                                    if connCov is not None and connThr is not None and winCov < OVERESTIMATE_FRAC * connThr:
                                        log.info("conn %d: copertura reale %.0f%% contro stima iniziale %.0f%%, riconnetto",
                                                 connId, winCov * 100, connCov * 100)
                                        reason = "overestimated"
                                        rejects += 1
                                        break
                                else:
                                    badWin = badWin + 1 if (winCov < DEGRADE_FRAC * baseCov and winCov < MIN_COV) else 0
                                    if badWin >= DEGRADE_WINDOWS and now - lastDegrade > DEGRADE_COOLDOWN_S:
                                        log.info("conn %d: copertura scesa a %.0f%% (riferimento %.0f%%), riconnetto",
                                                 connId, winCov * 100, baseCov * 100)
                                        lastDegrade = now
                                        reason = "degraded"
                                        break

                        if not gotMsg and elapsed > CONFIRM_S:
                            reason = "no_confirm"
                            break
                        if now - last_msg > STALL_S:
                            reason = "stall"
                            break
                        if now - lastTx > TX_STALL_S:
                            reason = "tx_stall"
                            break

                        # --- lettura con scadenza ---
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=min(1, STALL_S / 2))
                        except asyncio.TimeoutError:
                            continue
                        last_msg = time.monotonic()
                        gotMsg = True
                        msg = json.loads(raw)

                        if msg.get("id"):
                            if msg["id"] in (1, 2) and "result" not in msg:
                                log.warning("errore dal server sulla sottoscrizione: %s", str(msg)[:200])
                                reason = "rpc_error"
                                break
                            if msg["id"] == 1:
                                types[msg["result"]] = "tx"
                            elif msg["id"] == 2:
                                types[msg["result"]] = "block"
                            else:
                                num = requests.pop(msg["id"])
                                fullBlock = msg.get("result")
                                if fullBlock is None:
                                    notInNode += 1
                                    missing.add(num)
                                    continue
                                processBlock(num, fullBlock, connId)
                            continue

                        if msg.get("method") == "eth_subscription":
                            event = types.get(msg["params"]["subscription"])

                            if event == "tx":
                                connTx += 1
                                winTx += 1
                                qTx += 1
                                lastTx = time.monotonic()
                                tx = msg["params"]["result"]
                                h = tx["hash"]
                                if h in pool or h in retired:
                                    duplicates += 1
                                    continue
                                if h in minedHash:
                                    lateAnnounced += 1
                                    continue
                                sender = tx["from"].lower()
                                nonce = int(tx["nonce"], 16)
                                pool[h] = {"seen_ms": nowMs(), "from": sender,
                                           "to": tx["to"].lower() if tx.get("to") else None,
                                           "nonce": nonce, "type": int(tx["type"], 16),
                                           "value": str(int(tx["value"], 16)), "gas": int(tx["gas"], 16),
                                           "gas_price": hexToInt(tx.get("gasPrice")),
                                           "tip_cap": hexToInt(tx.get("maxPriorityFeePerGas")),
                                           "fee_cap": hexToInt(tx.get("maxFeePerGas")),
                                           "data_size": (len(tx["input"]) - 2) // 2, "selector": tx["input"][:10],
                                           "conn_id": connId, "probation": checking}
                                totalSeen += 1
                                pendingByNonce.setdefault((sender, nonce), []).append(h)
                                openQueue.append((time.monotonic(), h))

                            elif event == "block":
                                header = msg["params"]["result"]
                                headRecvMs = nowMs()
                                num = int(header["number"], 16)
                                connBlocks += 1
                                lastBlockMono = time.monotonic()

                                if lastHead is not None:
                                    if num > lastHead + 1:
                                        gapSize = num - lastHead - 1
                                        gaps += gapSize
                                        recoverTo = min(num, lastHead + 1 + MAX_GAP_RECOVER)
                                        log.info("salto: dopo %d arriva %d (%d blocchi mancanti), recupero i primi %d",
                                                 lastHead, num, gapSize, recoverTo - lastHead - 1)
                                        if recoverTo < num:
                                            spool.write("obs_gaps", [recoverTo, num - 1, num - recoverTo, headRecvMs, run, connId])
                                            obsGapBlocks += num - recoverTo
                                        for n in range(lastHead + 1, recoverTo):
                                            if n not in blocks:
                                                blocks[n] = {"observed": False, "conn_id": connId}
                                                missing.add(n)
                                    elif num <= lastHead:
                                        reannounced += 1
                                lastHead = num if lastHead is None else max(lastHead, num)
                                pruneMined(lastHead)
                                pruneBlocks(lastHead)

                                blocks[num] = {"timestamp": int(header["timestamp"], 16), "head_recv_ms": headRecvMs,
                                               "base_fee": int(header["baseFeePerGas"], 16),
                                               "gas_used": int(header["gasUsed"], 16),
                                               "gas_limit": int(header["gasLimit"], 16), "conn_id": connId,
                                               "conn_seq": connBlocks, "observed": True}
                                await ws.send(json.dumps({"id": nextId, "jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                                                          "params": [header["number"], True]}))
                                requests[nextId] = num
                                nextId += 1
                                blockCounter += 1

                                sent = 0
                                for n in sorted(missing):
                                    if sent >= MAX_RECOVER_PER_HEAD:
                                        break
                                    missing.discard(n)
                                    attempts[n] = attempts.get(n, 0) + 1
                                    if attempts[n] > 5:
                                        gaveUp += 1
                                        continue
                                    await ws.send(json.dumps({"id": nextId, "jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                                                              "params": [hex(n), True]}))
                                    requests[nextId] = n
                                    nextId += 1
                                    sent += 1

                                if cfg.blocks and blockCounter >= cfg.blocks:
                                    done = True
                                    reason = "done"
                                    break

            except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
                reason = f"error: {type(e).__name__}"

            # ===== dopo la connessione =====
            for n in requests.values():                     # richieste senza risposta: da rifare
                missing.add(n)

            if reason != "done":
                reasons[reason] += 1
                connElapsed = time.monotonic() - connStart
                log.info("conn %d chiusa: %s dopo %.1f s", connId, reason, connElapsed)
                if not checking:
                    deadStreak = failures = 0
                    backoff = 2
                if reason is not None and (reason.startswith("error") or reason == "rpc_error"):
                    failures += 1
                    wait = backoff
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    if failures in (20, 100, 500):
                        note.fire(f"{failures} errori di connessione di fila (ultimo: {reason})", "failures", 1800)
                elif reason in DEAD_REASONS:
                    failures = 0
                    backoff = 2
                    wait = min(DEAD_PAUSE_S * 2 ** min(deadStreak, 6), MAX_BACKOFF)
                    deadStreak += 1
                else:
                    failures = deadStreak = 0
                    backoff = 2
                    wait = POOR_PAUSE_S if reason == "low_rate" else DEAD_PAUSE_S
                await asyncio.sleep(wait)
    except (KeyboardInterrupt, asyncio.CancelledError):
        stop["why"] = "interrotto"
        log.info("interrotto: salvo i dati")
        for n in requests.values():
            missing.add(n)
    finally:
        try:
            await finalRecovery()
        except BaseException as e:                          # il recupero non deve mai impedire il salvataggio
            log.info("recupero finale interrotto (%s)", type(e).__name__)
        for t in (tickTask, resTask):
            t.cancel()
        await asyncio.gather(tickTask, resTask, return_exceptions=True)
        for h, r in pool.items():                           # tx ancora aperte: si conserva l'ultimo stato noto
            if "block_number" not in r:
                if r.get("replaced_by"):
                    writeTx(h, r, "replaced")
                elif "_last" in r:
                    writeTx(h, r, *r["_last"][:1], r["_last"][1], r["_last"][2])
                else:
                    writeTx(h, r, "open_at_stop")
        finalText = writeStats(final=True)
        writeState()
        spool.close()
        why = stop["why"] or ("completato" if done else "terminato")
        log.info("fine: %s (blocchi %d, tx viste %d, incluse %d, non viste %d)", why, blockCounter, totalSeen,
                 includedCount, unseenCount)
        await note.send(f"fermato ({why}) dopo {blockCounter} blocchi\n{finalText}")


def parseArgs(argv=None):
    notify.loadEnv()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("RPC_URL", DEFAULT_URL), help="endpoint WebSocket")
    ap.add_argument("--spool", default=os.environ.get("SPOOL_DIR", "spool"), help="cartella dei CSV")
    ap.add_argument("--blocks", type=int, default=0, help="si ferma dopo N blocchi (0 = senza limite)")
    ap.add_argument("--check-ages", default=os.environ.get("CHECK_AGES"),
                    help="eta' in secondi delle 3 verifiche, es. 600,7200,86400 (per le prove)")
    ap.add_argument("--trace-mem", type=float, default=0, metavar="SECONDI",
                    help="diagnostica: ogni N secondi scrive nel log dove cresce la memoria (rallenta un po')")
    ap.add_argument("--log", default=os.environ.get("LOG_FILE"), help="file di log con rotazione (oltre che a video)")
    return ap.parse_args(argv)


def setupLogging(path):
    fmt = logging.Formatter("%(asctime)sZ %(message)s", "%Y-%m-%d %H:%M:%S")
    fmt.converter = time.gmtime
    handlers = [logging.StreamHandler(sys.stdout)]
    if path:
        handlers.append(logging.handlers.RotatingFileHandler(path, maxBytes=5_000_000, backupCount=5, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)


def entry(argv=None):
    global CHECK_AGES_S
    cfg = parseArgs(argv)
    if cfg.check_ages:
        CHECK_AGES_S = tuple(float(x) for x in cfg.check_ages.split(","))
    setupLogging(cfg.log)

    async def runner():
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
        await main(cfg)

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    entry()
