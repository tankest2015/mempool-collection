# mempool-collector

Raccoglie la mempool di Polygon (PublicNode, WebSocket) e la mette in Postgres

`collector.py` scrive CSV in `spool/` (un file all'ora, UTC). `loader.py` li copia nel DB.
Se il DB cade il collettore continua, il loader recupera da solo.

## File
- `collector.py` raccolta (gira all'infinito, recheck tx non incluse integrato a 10 min / 2 h / 24 h)
- `loader.py` CSV -> Postgres
- `notify.py`, `spool.py`, `tables.py` roba di supporto (avvisi, scrittura CSV, colonne)
- `sql/` tabelle
- `deploy/` i due servizi systemd

## Installazione sulla VM
```bash
sudo apt update && sudo apt install -y git python3-venv postgresql
sudo useradd -r -d /opt/mempool-collector -s /bin/bash mempool      # senza -m: la cartella deve restare vuota per il clone
sudo mkdir /opt/mempool-collector && sudo chown mempool: /opt/mempool-collector
sudo -u mempool git clone <URL-REPO> /opt/mempool-collector
cd /opt/mempool-collector
sudo -u mempool python3 -m venv .venv
sudo -u mempool .venv/bin/pip install -r requirements.txt

# DB (UTF8!)
sudo -u postgres psql -c "CREATE ROLE mempool LOGIN PASSWORD 'PASSWORD'"
sudo -u postgres psql -c "CREATE DATABASE mempool OWNER mempool ENCODING 'UTF8' TEMPLATE template0"

# .env: DATABASE_URL, TELEGRAM_*, HEALTHCHECK_URL, HEALTHCHECK_LOADER_URL, NOTIFY_NAME
sudo -u mempool nano .env && sudo chmod 600 .env

sudo -u mempool .venv/bin/python loader.py --init-schema --once     # tabelle
sudo cp deploy/*.service /etc/systemd/system/ && sudo systemctl daemon-reload
sudo systemctl enable --now mempool-collector mempool-loader
```
Nel `.env` niente spazi attorno a `=` e `#` solo su righe intere.

## Comandi utili
```bash
systemctl status mempool-collector mempool-loader
journalctl -u mempool-collector -f
ls -la spool/tx/                              # i file devono crescere
sudo systemctl stop mempool-collector         # stop pulito, MAI kill -9 (a meno di test)
sudo systemctl restart mempool-collector mempool-loader   # dopo ogni modifica al .env
```
Per fermare tutto a fine raccolta: stop del collettore, loader acceso ancora 1-2 minuti, poi stop del loader.

## Ripartire da zero
Con tutto fermo: `DROP DATABASE` + `CREATE DATABASE` (come sopra), `sudo rm -rf spool` (anche `state.json`), poi `--init-schema --once` e start dei servizi.
Non cancellare mai `spool/` con il collettore acceso.

## Da ricordare
- Tutto in UTC, VM compresa.
- `spool/rejected/` deve restare vuota: se compare qualcosa, qualche riga e' stata scartata.
- I CSV chiusi vengono compressi dopo 48 h.
- Arrivano avvisi di errori di vario genere su Telegram e healthchecks.io
