-- schema.sql - tabelle della mempool. Funziona su Postgres semplice (>= 14); le ipertabelle Timescale
-- si aggiungono dopo con sql/schema_timescale.sql. Idempotente: si puo' rilanciare
-- Le colonne coincidono con tables.py. Tempi: *_ms = millisecondi Unix UTC,
-- colonne timestamptz (seen_at, t, ...) calcolate dal loader

-- Una riga per tx e per versione "vincente" (rev_ms piu' alto): lo stato piu' recente della tx
CREATE TABLE IF NOT EXISTS tx (
    hash                text        NOT NULL,
    seen_ms             bigint      NOT NULL,
    seen_at             timestamptz NOT NULL,
    run                 bigint,
    conn_id             integer,
    probation           boolean,      -- vista nei primi secondi di una connessione, prima della verifica
    sender              text,
    receiver            text,
    nonce               bigint,
    type                smallint,     -- 0 legacy, 2 EIP-1559
    value               numeric,
    gas                 bigint,
    gas_price           bigint,       -- tx legacy
    tip_cap             bigint,       -- maxPriorityFeePerGas (tipo 2)
    fee_cap             bigint,       -- maxFeePerGas (tipo 2)
    data_size           integer,
    selector            text,
    block_number        bigint,       -- se inclusa
    block_ts_ms         bigint,
    position            integer,
    delay_ms            bigint,       -- dal primo avvistamento al timestamp del blocco
    head_delay_ms       bigint,       -- dal primo avvistamento alla ricezione dell'header
    effective_gas_price bigint,
    effective_tip       bigint,
    replaced_by         text,         -- hash della tx con lo stesso (mittente, nonce) poi minata
    outcome             text,         -- included | replaced | mined_late | pending_executable | pending_nonce_gap |
                                      -- nonce_consumed | dropped_unknown | open_at_stop
    outcome_n           smallint,     -- verifica: 1 = 10 min, 2 = 2 h, 3 = 24 h
    outcome_ms          bigint,
    node_block          bigint,
    rev_ms              bigint      NOT NULL,
    PRIMARY KEY (hash, seen_at)
);
CREATE INDEX IF NOT EXISTS tx_sender_nonce ON tx (sender, nonce);
CREATE INDEX IF NOT EXISTS tx_block ON tx (block_number) WHERE block_number IS NOT NULL;

CREATE TABLE IF NOT EXISTS blocks (
    n             bigint  PRIMARY KEY,
    "timestamp"   bigint,
    head_recv_ms  bigint,
    base_fee      bigint,
    gas_used      bigint,
    gas_limit     bigint,
    run           bigint,
    conn_id       integer,
    conn_seq      integer,
    observed      boolean,          -- false = blocco recuperato dopo un buco, header non visto in tempo reale
    n_tx          integer,
    n_seen        integer,          -- tx del blocco gia' viste pending
    rev_ms        bigint  NOT NULL
);

CREATE INDEX IF NOT EXISTS blocks_ts ON blocks ("timestamp");

-- Tx incluse in un blocco ma mai viste nel feed.
CREATE TABLE IF NOT EXISTS unseen (
    hash        text        NOT NULL,
    block       bigint      NOT NULL,
    block_ts    bigint,
    block_time  timestamptz NOT NULL,
    pos         integer,
    n_tx_block  integer,
    sender      text,
    nonce       bigint,
    receiver    text,
    selector    text,
    tip         bigint,
    run         bigint,
    conn_id     integer,
    PRIMARY KEY (hash, block_time)
);
CREATE INDEX IF NOT EXISTS unseen_sender_nonce ON unseen (sender, nonce);
CREATE INDEX IF NOT EXISTS unseen_block ON unseen (block);

CREATE TABLE IF NOT EXISTS quality (
    t_ms     bigint      NOT NULL,
    t        timestamptz NOT NULL,
    run      bigint      NOT NULL,
    conn_id  integer     NOT NULL,
    feed     double precision,       -- tx/s del feed
    chain    double precision,       -- tx/s della catena
    cov      double precision,       -- feed / catena
    accepted boolean,
    PRIMARY KEY (t, run, conn_id)
);

CREATE TABLE IF NOT EXISTS obs_gaps (
    from_block  bigint      NOT NULL,
    to_block    bigint,
    n_blocks    integer,
    detected_ms bigint,
    detected_at timestamptz NOT NULL,
    run         bigint      NOT NULL,
    conn_id     integer     NOT NULL,
    PRIMARY KEY (from_block, run, conn_id)
);

CREATE TABLE IF NOT EXISTS conn_accepted (
    run         bigint      NOT NULL,
    conn_id     integer     NOT NULL,
    accepted_ms bigint,
    accepted_at timestamptz NOT NULL,
    PRIMARY KEY (run, conn_id)
);

CREATE TABLE IF NOT EXISTS stats (
    t_ms           bigint      NOT NULL,
    t              timestamptz NOT NULL,
    run            bigint      NOT NULL,
    blocks         bigint, tx_seen bigint, included bigint, unseen bigint, not_in_node bigint,
    late_announced bigint, duplicates bigint, gaps bigint, recovered bigint, reannounced bigint,
    gave_up        bigint, connections bigint, accepted_conns bigint, obs_gap_blocks bigint,
    pool           bigint,
    rss_mb         double precision,
    disk_free_gb   double precision,
    reasons        jsonb,
    PRIMARY KEY (t, run)
);

-- Stato del caricatore: fino a che byte di ogni file CSV si e' arrivati (aggiornato nella stessa
-- transazione dei dati: ogni riga viene caricata una volta sola)
CREATE TABLE IF NOT EXISTS loader_state (
    file         text        PRIMARY KEY,
    byte_offset  bigint      NOT NULL,
    done         boolean     NOT NULL DEFAULT false,
    rows_loaded  bigint      NOT NULL DEFAULT 0,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    sig          text
);
ALTER TABLE loader_state ADD COLUMN IF NOT EXISTS sig text;   -- per database creati prima

-- Percentili con la stessa regola dei vecchi report: elemento in posizione floor(q * n) dei valori ordinati
-- (0-based; con q = 1 il massimo). Uso: pcts(array_agg(x ORDER BY x), ARRAY[0.5, 0.9])
CREATE OR REPLACE FUNCTION pcts(a bigint[], qs numeric[]) RETURNS bigint[] AS $$
    SELECT CASE WHEN a IS NULL OR array_length(a, 1) IS NULL THEN NULL
           ELSE ARRAY(SELECT a[least(array_length(a, 1), floor(q * array_length(a, 1))::int + 1)]
                      FROM unnest(qs) WITH ORDINALITY AS u(q, i) ORDER BY i) END
$$ LANGUAGE sql IMMUTABLE;
