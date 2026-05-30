"""Order Book Imbalance — bid/ask pressure analysis (Phase 2)."""
from typing import Dict, Any
from services.binance_service import binance_service
from utils.logger import get_logger

logger = get_logger(__name__)


def get_order_book_imbalance(symbol: str, depth: int = 20) -> Dict[str, Any]:
    """
    Fetches the order book and measures bid vs ask pressure.
    Returns bid_ask_ratio, imbalance_signal (BUY/SELL/NEUTRAL), imbalance_score (0-1).
    ratio > 1.5 = strong buying pressure, < 0.67 = strong selling pressure.
    """
    try:
        book = binance_service._client.get_order_book(symbol=symbol, limit=depth)
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            return _neutral()

        bid_vol = sum(float(b[1]) for b in bids)
        ask_vol = sum(float(a[1]) for a in asks)
        if ask_vol == 0:
            return _neutral()

        ratio = bid_vol / ask_vol

        # Identify the single largest level on each side (wall)
        bid_wall = max(bids, key=lambda x: float(x[1]))
        ask_wall = max(asks, key=lambda x: float(x[1]))

        if ratio >= 1.5:
            signal = "BUY"
            score  = min((ratio - 1.0) / 2.0, 1.0)
        elif ratio <= 0.67:
            signal = "SELL"
            score  = min((1.0 / ratio - 1.0) / 1.5, 1.0)
        else:
            signal = "NEUTRAL"
            score  = 0.0

        return {
            "bid_volume":       round(bid_vol, 2),
            "ask_volume":       round(ask_vol, 2),
            "bid_ask_ratio":    round(ratio, 3),
            "imbalance_signal": signal,
            "imbalance_score":  round(score, 3),
            "bid_wall_price":   float(bid_wall[0]),
            "bid_wall_size":    float(bid_wall[1]),
            "ask_wall_price":   float(ask_wall[0]),
            "ask_wall_size":    float(ask_wall[1]),
        }
    except Exception as e:
        logger.debug(f"Order book {symbol} error: {e}")
        return _neutral()


def _neutral() -> Dict[str, Any]:
    return {
        "bid_volume": 0.0, "ask_volume": 0.0,
        "bid_ask_ratio": 1.0,
        "imbalance_signal": "NEUTRAL",
        "imbalance_score": 0.0,
        "bid_wall_price": 0.0, "bid_wall_size": 0.0,
        "ask_wall_price": 0.0, "ask_wall_size": 0.0,
    }
