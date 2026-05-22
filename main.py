import asyncio
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn

from config import settings
from database import connect_db, disconnect_db
from routers import auth, bot, trades, portfolio, signals, websocket, market, backtest
from utils.logger import get_logger
from utils.security import SECURITY_HEADERS, _rate_limiter

logger = get_logger(__name__)

app = FastAPI(
    title="ARIA Trading Bot API",
    version="2.0.0",
    docs_url="/docs" if settings.ENVIRONMENT == "development" else None,
    redoc_url=None, openapi_url="/openapi.json" if settings.ENVIRONMENT == "development" else None,
)

cors_origins = settings.cors_origins_list or ["http://localhost:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins, allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "X-Requested-With"],
    expose_headers=["Retry-After"], max_age=600,
)

if settings.ENVIRONMENT == "production":
    allowed_hosts = [h.replace("https://", "").replace("http://", "").split("/")[0] for h in cors_origins]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        for header, value in SECURITY_HEADERS.items():
            response.headers[header] = value
        if settings.ENVIRONMENT == "production":
            response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
        for h in ("server", "x-powered-by"):
            if h in response.headers:
                del response.headers[h]
        return response

app.add_middleware(SecurityHeadersMiddleware)


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > 1 * 1024 * 1024:
            return JSONResponse(status_code=413, content={"detail": "Requête trop grande"})
        return await call_next(request)

app.add_middleware(RequestSizeLimitMiddleware)


class FallbackCORSMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "")
        try:
            response = await call_next(request)
        except Exception as exc:
            logger.error(f"Middleware exception: {exc}")
            response = JSONResponse({"detail": "Erreur interne."}, status_code=500)
        if origin and "access-control-allow-origin" not in response.headers:
            if origin in cors_origins or settings.ENVIRONMENT == "development":
                response.headers["Access-Control-Allow-Origin"]      = origin
                response.headers["Access-Control-Allow-Credentials"] = "true"
        return response

app.add_middleware(FallbackCORSMiddleware)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    logger.info(f"Starting ARIA Trading Bot API v2.0 [{settings.ENVIRONMENT}]")
    await connect_db()

    from services.bot_engine import bot_engine
    bot_engine.initialize()

    # ── AUTO-RESTART les bots qui tournaient avant le redémarrage ─────────────
    await _auto_restart_bots(bot_engine)

    # Cache cleaner
    from utils.cache import cache
    async def _cache_cleaner():
        while True:
            await asyncio.sleep(300)
            cache.clear_expired()
            await _rate_limiter.cleanup()
    asyncio.create_task(_cache_cleaner())

    logger.info("API v2.0 ready — bots auto-restarted")


async def _auto_restart_bots(bot_engine) -> None:
    """
    Au démarrage du serveur, relance automatiquement tous les bots
    qui étaient actifs avant l'arrêt. L'état est persisté dans MongoDB.
    """
    try:
        from database import get_database
        db = get_database()
        active_users = await db.users.find(
            {"bot_config.is_running": True}
        ).to_list(length=100)

        if not active_users:
            logger.info("Aucun bot à redémarrer automatiquement")
            return

        for user in active_users:
            user_id    = str(user["_id"])
            bot_config = user.get("bot_config", {})
            asyncio.create_task(bot_engine.start(user_id, dict(bot_config)))
            logger.info(f"Bot auto-redémarré: user={user_id} symbol={bot_config.get('symbol')}")

    except Exception as e:
        logger.error(f"Auto-restart failed: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down…")
    from services.bot_engine import bot_engine
    # Ne pas marquer is_running=False dans la DB → permet l'auto-restart au prochain démarrage
    logger.info("Shutdown complete (bot state preserved in DB for auto-restart)")
    await disconnect_db()


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled {request.method} {request.url.path}: {type(exc).__name__}: {exc}")
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        content={"detail": "Une erreur interne s'est produite."})


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    return JSONResponse(status_code=404, content={"detail": "Ressource introuvable"})


PREFIX = "/api/v1"
app.include_router(auth.router,      prefix=PREFIX)
app.include_router(bot.router,       prefix=PREFIX)
app.include_router(trades.router,    prefix=PREFIX)
app.include_router(portfolio.router, prefix=PREFIX)
app.include_router(signals.router,   prefix=PREFIX)
app.include_router(market.router,    prefix=PREFIX)
app.include_router(backtest.router,  prefix=PREFIX)
app.include_router(websocket.router, prefix=PREFIX)


@app.get("/health", tags=["Health"])
async def health_check():
    from utils.cache import cache
    from services.bot_engine import bot_engine
    return {
        "status": "healthy", "version": "2.0.0",
        "active_bots": len(bot_engine._running_bots),
        "cache": cache.stats(),
    }


@app.get("/health/ip", tags=["Health"])
async def get_server_ip():
    """Retourne l'IP publique du serveur — à whitelister sur Binance."""
    import httpx
    services = [
        "https://api.ipify.org?format=json",
        "https://api64.ipify.org?format=json",
        "https://ifconfig.me/all.json",
    ]
    for url in services:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(url)
                data = r.json()
                ip = data.get("ip") or data.get("ip_addr")
                if ip:
                    logger.info(f"Server public IP: {ip}")
                    return {"server_ip": ip, "action": "Ajoutez cette IP dans Binance > Gestion des clés API > Restrictions IP"}
        except Exception:
            continue
    return {"server_ip": "inconnu", "error": "Impossible de déterminer l'IP publique"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000,
                reload=False, log_level="info")
