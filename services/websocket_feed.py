"""Real-time Binance miniTicker WebSocket feed (Phase 1).

Maintains a live price cache for all tracked pairs.
Falls back to REST if WS is disconnected.
"""
import asyncio
import json
from typing import Dict, List, Optional, Callable
from utils.logger import get_logger
from utils.cache import cache, PRICE_TTL

logger = get_logger(__name__)

_price_cache: Dict[str, float] = {}
_move_callbacks: List[Callable] = []
_ws_task: Optional[asyncio.Task] = None
_running: bool = False
_symbols: List[str] = []


def get_cached_price(symbol: str) -> Optional[float]:
    """Returns latest WS price for symbol, or None if not yet received."""
    return _price_cache.get(symbol)


def register_move_callback(fn: Callable) -> None:
    """Register async callback(symbol, price, prev_price) triggered on moves >= 0.1%."""
    _move_callbacks.append(fn)


async def start(symbols: List[str]) -> None:
    """Start the WS feed for the given symbols. Safe to call even if already running."""
    global _ws_task, _running, _symbols
    if _running:
        return
    _symbols  = [s.lower() for s in symbols]
    _running  = True
    _ws_task  = asyncio.create_task(_feed_loop())
    logger.info(f"WS feed starting: {len(symbols)} symbols")


async def stop() -> None:
    """Stop the WS feed gracefully."""
    global _running, _ws_task
    _running = False
    if _ws_task and not _ws_task.done():
        _ws_task.cancel()
        try:
            await _ws_task
        except asyncio.CancelledError:
            pass
    logger.info("WS feed stopped")


async def _feed_loop() -> None:
    """Main WS loop with exponential back-off reconnect."""
    try:
        import websockets
    except ImportError:
        logger.error("websockets package not installed — WS feed disabled")
        return

    streams = "/".join(f"{s}@miniTicker" for s in _symbols)
    url     = f"wss://stream.binance.com:9443/stream?streams={streams}"
    delay   = 5

    while _running:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                logger.info("WS connected to Binance miniTicker stream")
                delay = 5  # reset on successful connect
                async for raw in ws:
                    if not _running:
                        break
                    _handle(raw)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            if _running:
                logger.warning(f"WS error ({exc}) — reconnect in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)


def _handle(raw: str) -> None:
    """Parse a single miniTicker message and update caches."""
    try:
        msg  = json.loads(raw)
        data = msg.get("data", msg)
        if data.get("e") != "24hrMiniTicker":
            return
        sym   = data["s"]
        price = float(data["c"])
        if price <= 0:
            return

        prev = _price_cache.get(sym, price)
        _price_cache[sym] = price
        # Also update the shared cache so _fetch_prices reads WS price
        cache.set(f"price:{sym}", price, ttl_seconds=PRICE_TTL)

        # Fire move callbacks when price moves >= 0.1%
        move_pct = abs(price - prev) / prev * 100 if prev > 0 else 0
        if move_pct >= 0.1 and _move_callbacks:
            loop = asyncio.get_event_loop()
            for fn in _move_callbacks:
                try:
                    loop.create_task(fn(sym, price, prev))
                except Exception:
                    pass
    except Exception:
        pass
