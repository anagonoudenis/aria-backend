"""
Public market data endpoint — used by the frontend chart.
No auth required (public OHLCV data).
Cache agressif pour minimiser la latence et les appels Binance.
"""
import asyncio
import time
from fastapi import APIRouter, Query, HTTPException, status

from services.binance_service import binance_service
from utils.cache import cache, KLINE_TTL, PRICE_TTL
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/market", tags=["Market Data"])

VALID_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "1d"}

# Cache pairs — rechargé en background toutes les heures
_pairs_cache:     list  = []
_pairs_cache_ts:  float = 0
_pairs_loading:   bool  = False
_PAIRS_TTL = 3600


async def _refresh_pairs_cache() -> None:
    global _pairs_cache, _pairs_cache_ts, _pairs_loading
    _pairs_loading = True
    try:
        pairs = await asyncio.get_event_loop().run_in_executor(
            None, lambda: binance_service.get_top_pairs(min_volume_usdt=5_000_000)
        )
        _pairs_cache   = pairs
        _pairs_cache_ts = time.monotonic()
        logger.info(f"Pairs cache refreshed — {len(pairs)} pairs")
    except Exception as e:
        logger.error(f"Pairs cache refresh error: {e}")
    finally:
        _pairs_loading = False


async def _ensure_pairs() -> None:
    global _pairs_loading
    age = time.monotonic() - _pairs_cache_ts
    if age < _PAIRS_TTL and _pairs_cache:
        return
    if not _pairs_loading:
        # Refresh en background si déjà un cache valide (stale-while-revalidate)
        if _pairs_cache:
            asyncio.create_task(_refresh_pairs_cache())
            return
        # Première fois : bloquer jusqu'à ce que le cache soit prêt
        await _refresh_pairs_cache()


@router.get("/pairs")
async def get_pairs(min_volume: float = Query(default=5_000_000, ge=0)):
    await _ensure_pairs()
    pairs = [p for p in _pairs_cache if p["volume_usdt"] >= min_volume]
    return {"count": len(pairs), "pairs": pairs}


@router.get("/klines")
async def get_klines(
    symbol:   str = Query(default="BTCUSDT"),
    interval: str = Query(default="15m"),
    limit:    int = Query(default=200, ge=10, le=500),
):
    symbol = symbol.upper()

    if interval not in VALID_INTERVALS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Interval invalide. Valides: {sorted(VALID_INTERVALS)}",
        )

    # Validation symbole légère (regex)
    import re
    if not re.match(r'^[A-Z]{2,12}USDT$', symbol):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Symbole invalide")

    # ── Cache hit ──────────────────────────────────────────────────────────
    cache_key = f"klines:{symbol}:{interval}:{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return {"symbol": symbol, "interval": interval, "data": cached, "cached": True}

    # ── Cache miss → Binance ───────────────────────────────────────────────
    try:
        data = await asyncio.get_event_loop().run_in_executor(
            None, lambda: binance_service.get_klines_raw(symbol, interval, limit)
        )
        ttl = KLINE_TTL.get(interval, 900)
        cache.set(cache_key, data, ttl_seconds=ttl)
        return {"symbol": symbol, "interval": interval, "data": data, "cached": False}
    except Exception as e:
        logger.error(f"Klines error {symbol}/{interval}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Impossible de récupérer les données de marché",
        )


@router.get("/price/{symbol}")
async def get_price(symbol: str):
    symbol = symbol.upper()

    import re
    if not re.match(r'^[A-Z]{2,12}USDT$', symbol):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Symbole invalide")

    # ── Cache hit (TTL 5s) ────────────────────────────────────────────────
    cache_key = f"price:{symbol}"
    cached = cache.get(cache_key)
    if cached is not None:
        return {"symbol": symbol, "price": cached, "cached": True}

    # ── Cache miss → Binance ───────────────────────────────────────────────
    try:
        price = await asyncio.get_event_loop().run_in_executor(
            None, lambda: binance_service.get_current_price(symbol)
        )
        cache.set(cache_key, price, ttl_seconds=PRICE_TTL)
        return {"symbol": symbol, "price": price, "cached": False}
    except Exception as e:
        logger.error(f"Price error {symbol}: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Impossible de récupérer le prix",
        )
