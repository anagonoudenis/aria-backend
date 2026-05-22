import asyncio
import json
from typing import Dict, List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from utils.jwt import verify_token
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["WebSocket"])

# ── Connexions actives ─────────────────────────────────────────────────────────
# Dict protégé par un asyncio.Lock pour éviter les race conditions
_connections: Dict[str, List[WebSocket]] = {}
_lock = asyncio.Lock()

PING_INTERVAL = 30   # secondes


# ── Endpoint sécurisé ──────────────────────────────────────────────────────────
@router.websocket("/ws/{user_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    user_id: str,
    token: Optional[str] = Query(default=None, description="JWT access token"),
):
    """
    WebSocket temps réel.
    Le client DOIT passer son JWT comme query param : /ws/{user_id}?token=<jwt>
    """
    # ── 1. Authentification ──────────────────────────────────────────────────
    if not token:
        await websocket.close(code=4001, reason="Authentication required")
        logger.warning(f"WS rejected — no token — user_id={user_id}")
        return

    verified_id = verify_token(token)
    if not verified_id:
        await websocket.close(code=4001, reason="Invalid or expired token")
        logger.warning(f"WS rejected — invalid token — user_id={user_id}")
        return

    # L'utilisateur ne peut accéder qu'à son propre flux
    if verified_id != user_id:
        await websocket.close(code=4003, reason="Forbidden")
        logger.warning(f"WS rejected — token user_id={verified_id} ≠ path user_id={user_id}")
        return

    # ── 2. Accepter la connexion ──────────────────────────────────────────────
    await websocket.accept()
    logger.info(f"WS connected: user_id={user_id}")

    async with _lock:
        if user_id not in _connections:
            _connections[user_id] = []
        _connections[user_id].append(websocket)

    ping_task:  Optional[asyncio.Task] = None
    price_task: Optional[asyncio.Task] = None

    try:
        # Message de bienvenue
        await websocket.send_json({
            "type": "connected",
            "data": {
                "message": "Connexion WebSocket établie",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        })

        ping_task  = asyncio.create_task(_ping_loop(websocket, user_id))
        price_task = asyncio.create_task(_price_stream_loop(websocket, user_id))

        # Boucle de lecture
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=PING_INTERVAL + 10)
            except asyncio.TimeoutError:
                # Pas de message depuis trop longtemps → fermer
                break

            if not raw or len(raw) > 4096:
                # Ignorer les messages trop longs (DoS)
                continue

            try:
                msg = json.loads(raw)
                msg_type = msg.get("type", "")
                if msg_type == "pong":
                    continue  # keepalive OK
                if msg_type == "ping":
                    await websocket.send_json({"type": "pong", "data": {}})
            except (json.JSONDecodeError, Exception):
                # Ignorer les messages malformés sans les laisser provoquer une exception
                logger.debug(f"WS malformed message from user_id={user_id}")

    except WebSocketDisconnect:
        logger.info(f"WS disconnected: user_id={user_id}")
    except Exception as e:
        logger.error(f"WS error for user_id={user_id}: {type(e).__name__}: {e}")
    finally:
        for task in (ping_task, price_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await _remove_connection(user_id, websocket)


async def _ping_loop(websocket: WebSocket, user_id: str) -> None:
    """Envoie un ping toutes les 30 secondes pour maintenir la connexion."""
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await websocket.send_json({
                    "type": "ping",
                    "data": {"timestamp": datetime.now(timezone.utc).isoformat()},
                })
            except Exception:
                break
    except asyncio.CancelledError:
        pass


async def _price_stream_loop(websocket: WebSocket, user_id: str) -> None:
    """
    Streame les prix en temps réel toutes les 3 secondes.
    Récupère le symbole configuré pour l'utilisateur depuis la DB.
    """
    from services.binance_service import binance_service
    from database import get_database
    from bson import ObjectId

    STREAM_INTERVAL = 3  # secondes

    # Symboles par défaut à streamer
    DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

    try:
        while True:
            await asyncio.sleep(STREAM_INTERVAL)

            # Récupère le symbole configuré par l'user
            symbols = set(DEFAULT_SYMBOLS)
            try:
                from bson import ObjectId, errors as bson_errors
                db = get_database()
                user_doc = await db.users.find_one({"_id": ObjectId(user_id)}, {"bot_config": 1})
                if user_doc and user_doc.get("bot_config", {}).get("symbol"):
                    symbols.add(user_doc["bot_config"]["symbol"])
            except Exception as e:
                logger.debug(f"Price stream user fetch error: {type(e).__name__}")

            # Fetch + broadcast chaque prix
            now = datetime.now(timezone.utc).isoformat()
            for sym in symbols:
                try:
                    price = await asyncio.get_event_loop().run_in_executor(
                        None, lambda s=sym: binance_service.get_current_price(s)
                    )
                    await websocket.send_json({
                        "type": "price_update",
                        "data": {"symbol": sym, "price": price},
                        "timestamp": now,
                    })
                except Exception as e:
                    logger.debug(f"Price stream error for {sym}: {type(e).__name__}")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.debug(f"Price stream loop ended for {user_id}: {e}")


async def _remove_connection(user_id: str, websocket: WebSocket) -> None:
    """Supprime une connexion de la liste de manière thread-safe."""
    async with _lock:
        if user_id in _connections:
            try:
                _connections[user_id].remove(websocket)
            except ValueError:
                pass
            if not _connections[user_id]:
                del _connections[user_id]


# ── Broadcast (utilisé par bot_engine et notification_service) ────────────────
async def send_update(user_id: str, message_type: str, data: dict) -> None:
    """
    Envoie un message à toutes les connexions actives d'un utilisateur.
    Thread-safe grâce au lock asyncio.
    """
    async with _lock:
        conns = list(_connections.get(user_id, []))

    if not conns:
        return

    message = {
        "type":      message_type,
        "data":      data,
        "timestamp": datetime.utcnow().isoformat(),
    }

    dead: List[WebSocket] = []
    for ws in conns:
        try:
            await ws.send_json(message)
        except Exception as e:
            logger.debug(f"WS send failed for user_id={user_id}: {e}")
            dead.append(ws)

    # Purger les connexions mortes
    for ws in dead:
        await _remove_connection(user_id, ws)


async def broadcast_price(user_id: str, symbol: str, price: float) -> None:
    await send_update(user_id, "price_update", {
        "symbol":    symbol,
        "price":     price,
        "timestamp": datetime.utcnow().isoformat(),
    })
