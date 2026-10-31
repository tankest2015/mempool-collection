-- schema_timescale.sql - trasforma le tabelle grandi in ipertabelle TimescaleDB (2.x) e attiva la compressione
-- Da lanciare DOPO schema.sql e solo se l'estensione e' installata
CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('tx',       'seen_at',     chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('unseen',   'block_time',  chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('quality',  't',           chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, migrate_data => TRUE);

ALTER TABLE tx     SET (timescaledb.compress, timescaledb.compress_orderby = 'seen_at DESC');
ALTER TABLE unseen SET (timescaledb.compress, timescaledb.compress_orderby = 'block_time DESC');

-- Le righe di tx si aggiornano fino a 24 h dopo la prima vista: si comprime solo oltre 3 giorni
SELECT add_compression_policy('tx',     INTERVAL '3 days', if_not_exists => TRUE);
SELECT add_compression_policy('unseen', INTERVAL '3 days', if_not_exists => TRUE);
