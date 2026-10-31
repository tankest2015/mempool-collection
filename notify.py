"""notify.py - avvisi e riepiloghi (Telegram) e segnale di vita (healthchecks.io)

Configurazione (variabili d'ambiente o file .env nella cartella corrente):
    TELEGRAM_TOKEN        token del bot (BotFather)
    TELEGRAM_CHAT_ID      id della chat a cui scrivere
    HEALTHCHECK_URL       URL di ping di healthchecks.io per il collettore (https://hc-ping.com/<uuid>)
    HEALTHCHECK_LOADER_URL  idem per il caricatore (un secondo controllo)
    NOTIFY_NAME           nome mostrato nei messaggi (default: nome della macchina)

L'invio avviene in un thread con timeout
Ogni errore viene ignorato (e contato). Gli avvisi con la stessa `key` rispettano un intervallo minimo
"""
import asyncio
import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT_S = 10


def loadEnv(path=".env"):
    """Legge un file .env semplice (CHIAVE=valore) senza sovrascrivere le variabili gia' presenti."""
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                v = v.strip()
                if v[:1] not in ("'", '"'):
                    v = v.split(" #")[0].split("\t#")[0].strip()      # commento sulla stessa riga
                os.environ.setdefault(k.strip(), v.strip('"').strip("'"))
    except OSError:
        pass


class Notifier:
    def __init__(self, role="collector", hcEnv="HEALTHCHECK_URL"):
        loadEnv()
        self.role = role
        self.token = os.environ.get("TELEGRAM_TOKEN")
        self.chat = os.environ.get("TELEGRAM_CHAT_ID")
        self.api = os.environ.get("TELEGRAM_API", "https://api.telegram.org").rstrip("/")
        self.hc = os.environ.get(hcEnv)
        self.name = os.environ.get("NOTIFY_NAME") or socket.gethostname()
        self.lastSent = {}
        self.errors = 0
        self.lastError = None
        self.sent = 0
        self.history = []                       # ultimi messaggi (utile nei test)

    @property
    def enabled(self):
        return bool(self.token and self.chat)

    # ---------- invio (bloccante: gira in un thread) ----------
    def _post(self, text):
        url = f"{self.api}/bot{self.token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": self.chat, "text": text[:3900]}).encode()
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=TIMEOUT_S) as r:
            r.read()

    def _get(self, url):
        with urllib.request.urlopen(url, timeout=TIMEOUT_S) as r:
            r.read()

    # ---------- API asincrona ----------
    async def send(self, text, key=None, min_interval_s=0):
        """Invia un messaggio; con `key` non ne invia piu' di uno ogni `min_interval_s`. Non solleva mai eccezioni."""
        try:
            now = time.monotonic()
            if key is not None and now - self.lastSent.get(key, -1e18) < min_interval_s:
                return False
            if key is not None:
                self.lastSent[key] = now
            full = f"[{self.name}/{self.role}] {text}"
            self.history.append(full)
            del self.history[:-50]
            if not self.enabled:
                return False
            await asyncio.to_thread(self._post, full)
            self.sent += 1
            return True
        except Exception:
            self.errors += 1
            return False

    def fire(self, text, key=None, min_interval_s=0):
        """Come send(), ma da codice sincrono: lancia l'invio in background e non aspetta."""
        try:
            asyncio.get_running_loop().create_task(self.send(text, key, min_interval_s))
        except RuntimeError:
            pass

    async def ping(self, suffix=""):
        """Segnale di vita a healthchecks.io (suffix='/fail' per segnalare un guasto)."""
        if not self.hc:
            return False
        try:
            await asyncio.to_thread(self._get, self.hc.rstrip("/") + suffix)
            return True
        except Exception:
            self.errors += 1
            return False

    # ---------- versione sincrona (per loader.py) ----------
    def sendSync(self, text, key=None, min_interval_s=0):
        try:
            now = time.monotonic()
            if key is not None and now - self.lastSent.get(key, -1e18) < min_interval_s:
                return False
            if key is not None:
                self.lastSent[key] = now
            full = f"[{self.name}/{self.role}] {text}"
            self.history.append(full)
            del self.history[:-50]
            if not self.enabled:
                return False
            self._post(full)
            self.sent += 1
            return True
        except Exception as e:
            self.errors += 1
            self.lastError = f"{type(e).__name__}: {e}"
            return False

    def pingSync(self, suffix=""):
        if not self.hc:
            return False
        try:
            self._get(self.hc.rstrip("/") + suffix)
            return True
        except Exception:
            self.errors += 1
            return False
