"""
Service Binance Futures USD-M
Gère les ordres avec levier en mode ISOLATED margin.
Utilisé uniquement en Aggressive Bull Mode (conf >= 0.85 + triple confluence).
"""
import asyncio
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional
from binance.client import Client
from binance.exceptions import BinanceAPIException
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_LEVERAGE = 10          # 10x levier — multiplicateur puissant mais contrôlé
MAX_LEVERAGE     = 15          # plafond de sécurité absolu
MARGIN_TYPE      = "ISOLATED"  # chaque position protège le reste du capital


class FuturesService:
    def __init__(self):
        if settings.BINANCE_TESTNET:
            self._client = Client(
                api_key=settings.BINANCE_API_KEY,
                api_secret=settings.BINANCE_SECRET_KEY,
                testnet=True,
            )
            logger.info("FuturesService — TESTNET mode")
        else:
            self._client = Client(
                api_key=settings.BINANCE_API_KEY,
                api_secret=settings.BINANCE_SECRET_KEY,
                testnet=False,
            )
            logger.info("FuturesService — LIVE mode")

        self._leverage_set: Dict[str, bool] = {}  # cache levier configuré par symbole
        self._symbol_cache: Dict[str, Dict] = {}  # cache précision par symbole

    # ══════════════════════════════════════════════════════════════════════════
    # BALANCE ET POSITIONS
    # ══════════════════════════════════════════════════════════════════════════

    def get_futures_balance(self) -> float:
        """Retourne le solde USDT disponible dans le wallet Futures."""
        try:
            balances = self._client.futures_account_balance()
            for b in balances:
                if b.get("asset") == "USDT":
                    return float(b.get("availableBalance", 0))
        except BinanceAPIException as e:
            logger.error(f"FuturesService balance error: {e}")
        return 0.0

    def get_futures_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Retourne la position ouverte sur un symbole.
        - Dict avec données → position ouverte
        - {} vide         → API OK mais aucune position (fermée par SL/TP)
        - None            → erreur API (état inconnu — ne pas agir)
        """
        try:
            positions = self._client.futures_position_information(symbol=symbol)
            for p in positions:
                amt = float(p.get("positionAmt", 0))
                if abs(amt) > 0:
                    return {
                        "symbol":         p.get("symbol"),
                        "qty":            amt,
                        "entry_price":    float(p.get("entryPrice", 0)),
                        "unrealized_pnl": float(p.get("unrealizedProfit", 0)),
                        "leverage":       int(p.get("leverage", DEFAULT_LEVERAGE)),
                        "margin_type":    p.get("marginType", "isolated"),
                        "notional":       abs(amt) * float(p.get("markPrice", 0)),
                    }
            return {}  # API OK — aucune position ouverte sur ce symbole
        except BinanceAPIException as e:
            logger.error(f"FuturesService position {symbol}: {e}")
            return None  # état inconnu — erreur API

    def get_mark_price(self, symbol: str) -> float:
        """Prix mark Futures (plus stable que le last pour les décisions SL/TP)."""
        try:
            data = self._client.futures_mark_price(symbol=symbol)
            return float(data.get("markPrice", 0))
        except Exception as e:
            logger.warning(f"FuturesService mark price {symbol}: {e}")
            return 0.0

    # ══════════════════════════════════════════════════════════════════════════
    # CONFIGURATION LEVIER + MARGE
    # ══════════════════════════════════════════════════════════════════════════

    def _ensure_leverage(self, symbol: str, leverage: int) -> bool:
        """Configure le levier et le mode ISOLATED si pas encore fait."""
        leverage = min(leverage, MAX_LEVERAGE)
        cache_key = f"{symbol}:{leverage}"
        if cache_key in self._leverage_set:
            return True
        try:
            # Mode ISOLATED — chaque position a sa propre marge
            try:
                self._client.futures_change_margin_type(symbol=symbol, marginType=MARGIN_TYPE)
                logger.info(f"Futures {symbol}: margin type → {MARGIN_TYPE}")
            except BinanceAPIException as e:
                if "No need to change margin type" not in str(e):
                    logger.warning(f"Futures margin type {symbol}: {e}")

            # Levier
            self._client.futures_change_leverage(symbol=symbol, leverage=leverage)
            logger.info(f"Futures {symbol}: levier → {leverage}x")
            self._leverage_set[cache_key] = True
            return True
        except BinanceAPIException as e:
            logger.error(f"FuturesService set leverage {symbol}: {e}")
            return False

    # ══════════════════════════════════════════════════════════════════════════
    # PRÉCISION SYMBOLE
    # ══════════════════════════════════════════════════════════════════════════

    def _get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """Retourne step_size et min_qty depuis les exchange info Futures."""
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]
        try:
            info = self._client.futures_exchange_info()
            for s in info.get("symbols", []):
                if s["symbol"] == symbol:
                    result = {"step_size": "0.1", "min_qty": "0.1", "min_notional": "5"}
                    for f in s.get("filters", []):
                        if f["filterType"] == "LOT_SIZE":
                            result["step_size"] = f.get("stepSize", "0.1")
                            result["min_qty"]   = f.get("minQty", "0.1")
                        elif f["filterType"] == "MIN_NOTIONAL":
                            result["min_notional"] = f.get("notional", "5")
                    self._symbol_cache[symbol] = result
                    return result
        except Exception as e:
            logger.warning(f"FuturesService symbol info {symbol}: {e}")
        return {"step_size": "0.1", "min_qty": "0.1", "min_notional": "5"}

    def _calc_qty(self, symbol: str, notional_usdt: float, price: float) -> float:
        """Calcule la quantité à acheter selon le notional voulu."""
        if price <= 0:
            return 0.0
        info = self._get_symbol_info(symbol)
        step = Decimal(info["step_size"])
        raw_qty = notional_usdt / price
        qty = float((Decimal(str(raw_qty)) // step) * step)
        return qty

    # ══════════════════════════════════════════════════════════════════════════
    # ORDRES
    # ══════════════════════════════════════════════════════════════════════════

    def open_long(
        self, symbol: str, notional_usdt: float, price: float, leverage: int = DEFAULT_LEVERAGE
    ) -> Optional[Dict[str, Any]]:
        """Ouvre une position LONG (BUY) sur Futures avec levier."""
        if not self._ensure_leverage(symbol, leverage):
            return None

        qty = self._calc_qty(symbol, notional_usdt, price)
        if qty <= 0:
            logger.warning(f"Futures {symbol}: quantité nulle — achat annulé")
            return None

        try:
            order = self._client.futures_create_order(
                symbol=symbol,
                side="BUY",
                type="MARKET",
                quantity=qty,
            )
            fills    = order.get("fills", [])
            # Futures market orders retournent avgPrice, pas fills[0]["price"]
            ex_price = (float(fills[0]["price"]) if fills
                        else float(order.get("avgPrice") or price))
            ex_qty   = float(order.get("executedQty", qty))
            logger.info(
                f"Futures LONG {symbol}: {ex_qty} @ {ex_price:.4f} "
                f"levier={leverage}x notional={ex_qty * ex_price:.2f} USDT"
            )
            return {
                "order_id":  str(order.get("orderId", "")),
                "symbol":    symbol,
                "qty":       ex_qty,
                "price":     ex_price,
                "notional":  ex_qty * ex_price,
                "leverage":  leverage,
                "side":      "BUY",
            }
        except BinanceAPIException as e:
            logger.error(f"Futures open_long {symbol}: {e}")
            return None

    def close_long(self, symbol: str, qty: float) -> Optional[Dict[str, Any]]:
        """Ferme une position LONG (SELL de clôture)."""
        info = self._get_symbol_info(symbol)
        step = Decimal(info["step_size"])
        sell_qty = float((Decimal(str(qty)) // step) * step)
        if sell_qty <= 0:
            return None
        try:
            order = self._client.futures_create_order(
                symbol=symbol,
                side="SELL",
                type="MARKET",
                quantity=sell_qty,
                reduceOnly=True,  # ne ferme pas plus que la position
            )
            fills    = order.get("fills", [])
            # Futures market orders retournent avgPrice, pas fills[0]["price"]
            ex_price = (float(fills[0]["price"]) if fills
                        else float(order.get("avgPrice") or 0))
            ex_qty   = float(order.get("executedQty", sell_qty))
            logger.info(f"Futures CLOSE {symbol}: {ex_qty} @ {ex_price:.4f}")
            return {
                "order_id": str(order.get("orderId", "")),
                "symbol":   symbol,
                "qty":      ex_qty,
                "price":    ex_price,
            }
        except BinanceAPIException as e:
            logger.error(f"Futures close_long {symbol}: {e}")
            return None

    def close_all_positions(self) -> int:
        """Ferme toutes les positions Futures ouvertes. Retourne le nombre fermé."""
        closed = 0
        try:
            positions = self._client.futures_position_information()
            for p in positions:
                amt = float(p.get("positionAmt", 0))
                if abs(amt) > 0:
                    sym = p.get("symbol", "")
                    result = self.close_long(sym, abs(amt))
                    if result:
                        closed += 1
        except Exception as e:
            logger.error(f"FuturesService close_all: {e}")
        return closed

    def set_stop_loss_tp(self, symbol: str, qty: float, sl_price: float, tp_price: float) -> bool:
        """Place des ordres SL et TP sur la position Futures."""
        try:
            info = self._get_symbol_info(symbol)
            step = Decimal(info["step_size"])
            order_qty = float((Decimal(str(qty)) // step) * step)

            # Stop Loss (STOP_MARKET)
            self._client.futures_create_order(
                symbol=symbol,
                side="SELL",
                type="STOP_MARKET",
                quantity=order_qty,
                stopPrice=round(sl_price, 4),
                reduceOnly=True,
                timeInForce="GTC",
                workingType="MARK_PRICE",
            )
            logger.info(f"Futures SL {symbol}: {sl_price:.4f}")

            # Take Profit (TAKE_PROFIT_MARKET)
            self._client.futures_create_order(
                symbol=symbol,
                side="SELL",
                type="TAKE_PROFIT_MARKET",
                quantity=order_qty,
                stopPrice=round(tp_price, 4),
                reduceOnly=True,
                timeInForce="GTC",
                workingType="MARK_PRICE",
            )
            logger.info(f"Futures TP {symbol}: {tp_price:.4f}")
            return True

        except BinanceAPIException as e:
            logger.error(f"Futures SL/TP {symbol}: {e}")
            return False


futures_service = FuturesService()
