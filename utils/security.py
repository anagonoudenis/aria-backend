# NOUVEAU FICHIER — Utilitaires de sécurité production
"""
Rate limiter in-memory thread-safe + helpers sécurité.
Pas de dépendance externe supplémentaire.
"""
import asyncio
import time
import re
from collections import defaultdict
from typing import Optional
from fastapi import Request, HTTPException, status
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Regex ─────────────────────────────────────────────────────────────────────
# Mot de passe : 8+ chars, 1 majuscule, 1 chiffre, 1 caractère spécial
PASSWORD_RE = re.compile(
    r"^(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]).{8,}$"
)

# Email strict
EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def validate_password_strength(password: str) -> tuple[bool, str]:
    """
    Retourne (ok, error_message).
    Règles : 8+ chars, 1 majuscule, 1 chiffre, 1 caractère spécial.
    """
    if len(password) < 8:
        return False, "Le mot de passe doit contenir au moins 8 caractères"
    if not re.search(r"[A-Z]", password):
        return False, "Le mot de passe doit contenir au moins une majuscule"
    if not re.search(r"\d", password):
        return False, "Le mot de passe doit contenir au moins un chiffre"
    if not re.search(r"[!@#$%^&*()\-_=+\[\]{};:'\",.<>/?\\|`~]", password):
        return False, "Le mot de passe doit contenir au moins un caractère spécial (!@#$%...)"
    return True, ""


def validate_email(email: str) -> bool:
    return bool(EMAIL_RE.match(email)) and len(email) <= 254


# ── Rate Limiter (sliding window) ─────────────────────────────────────────────
class RateLimiter:
    """
    Rate limiter sliding-window en mémoire, thread-safe via asyncio.Lock.
    Clé = combinaison IP + endpoint.
    """

    def __init__(self):
        self._windows: dict = defaultdict(list)
        self._lock = asyncio.Lock()

    async def is_allowed(
        self,
        key: str,
        max_requests: int,
        window_seconds: int,
    ) -> bool:
        async with self._lock:
            now = time.monotonic()
            window = self._windows[key]
            # Purge les entrées hors fenêtre
            self._windows[key] = [t for t in window if now - t < window_seconds]
            if len(self._windows[key]) >= max_requests:
                return False
            self._windows[key].append(now)
            return True

    async def cleanup(self, max_age_seconds: int = 3600) -> int:
        """Purge les clés inactives depuis > max_age_seconds."""
        async with self._lock:
            now = time.monotonic()
            stale = [k for k, v in self._windows.items()
                     if not v or now - max(v) > max_age_seconds]
            for k in stale:
                del self._windows[k]
            return len(stale)


# Singleton global
_rate_limiter = RateLimiter()


def get_client_ip(request: Request) -> str:
    """Extrait l'IP réelle du client en tenant compte des proxies."""
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Prendre la première IP (celle du client, pas du proxy)
        ip = forwarded_for.split(",")[0].strip()
        # Validation basique pour éviter l'injection de header
        if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
            return ip
    real_ip = request.headers.get("X-Real-IP")
    if real_ip and re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", real_ip):
        return real_ip
    return request.client.host if request.client else "unknown"


async def check_rate_limit(
    request: Request,
    max_requests: int,
    window_seconds: int,
    endpoint_name: str = "",
) -> None:
    """
    Dépendance FastAPI à injecter dans les routes sensibles.
    Lève HTTP 429 si la limite est dépassée.
    """
    ip = get_client_ip(request)
    key = f"{ip}:{endpoint_name}"
    allowed = await _rate_limiter.is_allowed(key, max_requests, window_seconds)
    if not allowed:
        logger.warning(f"Rate limit exceeded: {ip} → {endpoint_name}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Trop de requêtes. Réessayez dans {window_seconds} secondes.",
            headers={"Retry-After": str(window_seconds)},
        )


# Dépendances préconfigurées pour les endpoints sensibles
async def auth_rate_limit(request: Request) -> None:
    """Max 10 tentatives par minute par IP sur les endpoints d'authentification."""
    await check_rate_limit(request, max_requests=10, window_seconds=60, endpoint_name="auth")


async def strict_rate_limit(request: Request) -> None:
    """Max 30 requêtes par minute (endpoints API généraux)."""
    await check_rate_limit(request, max_requests=30, window_seconds=60, endpoint_name="api")


async def backtest_rate_limit(request: Request) -> None:
    """Max 5 backtests par heure par IP."""
    await check_rate_limit(request, max_requests=5, window_seconds=3600, endpoint_name="backtest")


# ── Security headers ─────────────────────────────────────────────────────────
SECURITY_HEADERS = {
    "X-Content-Type-Options":    "nosniff",
    "X-Frame-Options":           "DENY",
    "X-XSS-Protection":          "1; mode=block",
    "Referrer-Policy":           "strict-origin-when-cross-origin",
    "Permissions-Policy":        "camera=(), microphone=(), geolocation=()",
    "Content-Security-Policy": (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data:; "
        "connect-src 'self' ws: wss: https:"
    ),
}
