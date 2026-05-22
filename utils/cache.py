# NOUVEAU FICHIER — à créer
import time
import threading
from typing import Any, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class InMemoryCache:
    """
    Cache TTL thread-safe en mémoire.
    Évite les appels répétés à Binance et les recalculs d'indicateurs.
    """

    def __init__(self):
        self._store: dict = {}
        self._lock = threading.Lock()

    def set(self, key: str, value: Any, ttl_seconds: int = 60) -> None:
        with self._lock:
            self._store[key] = {
                "value": value,
                "expires_at": time.monotonic() + ttl_seconds,
            }

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            if time.monotonic() > item["expires_at"]:
                del self._store[key]
                return None
            return item["value"]

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def invalidate_prefix(self, prefix: str) -> int:
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]
            return len(keys)

    def clear_expired(self) -> int:
        with self._lock:
            now = time.monotonic()
            expired = [k for k, v in self._store.items() if now > v["expires_at"]]
            for k in expired:
                del self._store[k]
            return len(expired)

    def size(self) -> int:
        with self._lock:
            return len(self._store)

    def stats(self) -> dict:
        with self._lock:
            now = time.monotonic()
            alive = sum(1 for v in self._store.values() if now <= v["expires_at"])
            return {"total": len(self._store), "alive": alive, "expired": len(self._store) - alive}


# Interval en secondes → TTL du cache klines
KLINE_TTL = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "1h":  3600,
    "4h":  14400,
    "1d":  86400,
}

PRICE_TTL = 5      # prix courant : 5 secondes
INDICATOR_TTL = {  # indicateurs calculés
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "1h":  3600,
    "4h":  14400,
}

# Singleton global
cache = InMemoryCache()
