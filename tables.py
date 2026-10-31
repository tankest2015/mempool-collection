"""Definizione delle tabelle: colonne dei file CSV (nello stesso ordine in cui le scrive il collettore
e le legge il caricatore). sql/schema.sql deve avere le stesse colonne

Convenzioni dei CSV:
  - un file per tabella e per ora UTC:  spool/<tabella>/<tabella>_AAAAMMGG_HH.csv  (con intestazione)
  - valore assente = \\N      booleani = t / f      tempi = millisecondi Unix (UTC)
"""

# ---- tx: una riga per ogni VERSIONE dello stato di una tx (vale la piu' recente: rev_ms) ----
#   outcome: included | replaced | mined_late | pending_executable | pending_nonce_gap |
#            nonce_consumed | dropped_unknown | open_at_stop
#   outcome_n: numero della verifica (1 = dopo 10 min, 2 = dopo 2 h, 3 = dopo 24 h)
TX = ["hash", "seen_ms", "run", "conn_id", "probation", "sender", "receiver", "nonce", "type", "value",
      "gas", "gas_price", "tip_cap", "fee_cap", "data_size", "selector",
      "block_number", "block_ts_ms", "position", "delay_ms", "head_delay_ms", "effective_gas_price", "effective_tip",
      "replaced_by", "outcome", "outcome_n", "outcome_ms", "node_block", "rev_ms"]

# ---- blocks: una riga per blocco (vale la piu' recente: rev_ms) ----
BLOCKS = ["n", "timestamp", "head_recv_ms", "base_fee", "gas_used", "gas_limit", "run", "conn_id", "conn_seq",
          "observed", "n_tx", "n_seen", "rev_ms"]

# ---- unseen: tx incluse in un blocco ma mai viste nel feed ----
UNSEEN = ["hash", "block", "block_ts", "pos", "n_tx_block", "sender", "nonce", "receiver", "selector", "tip",
          "run", "conn_id"]

# ---- quality: una riga al minuto (ritmo del feed, della catena, copertura) ----
QUALITY = ["t_ms", "run", "conn_id", "feed", "chain", "cov", "accepted"]

# ---- obs_gaps: blocchi saltati e non recuperati ----
OBS_GAPS = ["from_block", "to_block", "n_blocks", "detected_ms", "run", "conn_id"]

# ---- conn_accepted: connessioni che hanno superato il controllo iniziale ----
CONN_ACCEPTED = ["run", "conn_id", "accepted_ms"]

# ---- stats: una riga all'ora con i contatori cumulativi della corsa (reasons = JSON) ----
STATS = ["t_ms", "run", "blocks", "tx_seen", "included", "unseen", "not_in_node", "late_announced", "duplicates",
         "gaps", "recovered", "reannounced", "gave_up", "connections", "accepted_conns", "obs_gap_blocks",
         "pool", "rss_mb", "disk_free_gb", "reasons"]

TABLES = {"tx": TX, "blocks": BLOCKS, "unseen": UNSEEN, "quality": QUALITY, "obs_gaps": OBS_GAPS,
          "conn_accepted": CONN_ACCEPTED, "stats": STATS}

# Come si carica ciascuna tabella nel DB (lo usa loader.py).
#   time = (colonna del DB, espressione SQL calcolata dal loader a partire dalle colonne del CSV): e' la colonna del tempo
#          che serve da chiave di partizione se la tabella diventa una ipertabella Timescale
#   key  = chiave primaria nel DB (colonne del CSV + la colonna `time`)
#   rev  = colonna che decide quale versione di una riga vince (None = la prima basta, ON CONFLICT DO NOTHING)
LOAD = {
    "tx": {"time": ("seen_at", "to_timestamp(seen_ms / 1000.0)"), "key": ["hash", "seen_at"], "rev": "rev_ms"},
    "blocks": {"time": None, "key": ["n"], "rev": "rev_ms"},
    "unseen": {"time": ("block_time", "to_timestamp(block_ts)"), "key": ["hash", "block_time"], "rev": None},
    "quality": {"time": ("t", "to_timestamp(t_ms / 1000.0)"), "key": ["t", "run", "conn_id"], "rev": None},
    "obs_gaps": {"time": ("detected_at", "to_timestamp(detected_ms / 1000.0)"),
                 "key": ["from_block", "run", "conn_id"], "rev": None},
    "conn_accepted": {"time": ("accepted_at", "to_timestamp(accepted_ms / 1000.0)"),
                      "key": ["run", "conn_id"], "rev": None},
    "stats": {"time": ("t", "to_timestamp(t_ms / 1000.0)"), "key": ["t", "run"], "rev": None},
}
