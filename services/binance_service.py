import pandas as pd
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Callable
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class BinanceService:
    def __init__(self):
        if settings.BINANCE_TESTNET:
            self._client = Client(
                api_key=settings.BINANCE_API_KEY,
                api_secret=settings.BINANCE_SECRET_KEY,
                testnet=True,
            )
            logger.info("Binance client initialised — TESTNET mode")
        else:
            self._client = Client(
                api_key=settings.BINANCE_API_KEY,
                api_secret=settings.BINANCE_SECRET_KEY,
                testnet=False,
            )
            logger.info("Binance client initialised — LIVE mode")

        # Symbol info cache to reduce redundant API calls
        self._symbol_cache: Dict[str, Dict[str, Any]] = {}

    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        interval_map = {
            "1m": Client.KLINE_INTERVAL_1MINUTE,
            "5m": Client.KLINE_INTERVAL_5MINUTE,
            "15m": Client.KLINE_INTERVAL_15MINUTE,
            "1h": Client.KLINE_INTERVAL_1HOUR,
            "4h": Client.KLINE_INTERVAL_4HOUR,
            "1d": Client.KLINE_INTERVAL_1DAY,
        }
        binance_interval = interval_map.get(interval, Client.KLINE_INTERVAL_15MINUTE)

        try:
            klines = self._client.get_klines(
                symbol=symbol, interval=binance_interval, limit=limit
            )
            df = pd.DataFrame(klines, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base", "taker_buy_quote", "ignore",
            ])
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(float)
            return df
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting klines for {symbol}: code={e.code} msg={e.message}")
            raise

    def get_current_price(self, symbol: str) -> float:
        try:
            ticker = self._client.get_symbol_ticker(symbol=symbol)
            return float(ticker["price"])
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting price for {symbol}: {e.message}")
            raise

    def get_account_balance(self) -> Dict[str, Any]:
        try:
            account = self._client.get_account()
            balances: Dict[str, Any] = {}
            for asset_data in account["balances"]:
                free = float(asset_data["free"])
                locked = float(asset_data["locked"])
                if free > 0 or locked > 0:
                    balances[asset_data["asset"]] = {
                        "free": free,
                        "locked": locked,
                        "total": free + locked,
                    }
            return balances
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting balance: {e.message}")
            raise

    def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        try:
            info = self._client.get_symbol_info(symbol)
            if not info:
                raise ValueError(f"Symbol {symbol} not found on Binance")

            result: Dict[str, Any] = {
                "symbol": symbol,
                "base_asset": info["baseAsset"],
                "quote_asset": info["quoteAsset"],
                "step_size": "0.00100000",
                "tick_size": "0.01000000",
                "min_qty": "0.00100000",
                "min_notional": "10",
            }
            for f in info.get("filters", []):
                ft = f.get("filterType", "")
                if ft == "LOT_SIZE":
                    result["step_size"] = f["stepSize"]
                    result["min_qty"] = f["minQty"]
                elif ft == "PRICE_FILTER":
                    result["tick_size"] = f["tickSize"]
                elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                    result["min_notional"] = f.get("minNotional", "10")

            self._symbol_cache[symbol] = result
            return result
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting symbol info for {symbol}: {e.message}")
            raise

    def calculate_quantity(self, symbol: str, usdt_amount: float, price: float) -> float:
        if price <= 0:
            raise ValueError("Price must be positive")
        if usdt_amount <= 0:
            raise ValueError("USDT amount must be positive")

        try:
            info = self.get_symbol_info(symbol)
            step_size = Decimal(info["step_size"])
            raw_qty = Decimal(str(usdt_amount)) / Decimal(str(price))
            qty = (raw_qty // step_size) * step_size
            qty = qty.quantize(step_size, rounding=ROUND_DOWN)

            min_qty = Decimal(info["min_qty"])
            if qty < min_qty:
                raise ValueError(
                    f"Calculated quantity {qty} is below minimum {min_qty} for {symbol}"
                )

            # Vérification min notional (montant minimum par ordre)
            min_notional = float(info.get("min_notional", "10"))
            notional = float(qty) * price
            if notional < min_notional:
                raise ValueError(
                    f"Order notional {notional:.2f} USDT < minimum {min_notional:.0f} USDT for {symbol}"
                )

            return float(qty)
        except Exception as e:
            logger.error(f"Error calculating quantity for {symbol}: {e}")
            raise

    def place_market_order(self, symbol: str, side: str, quantity: float) -> Dict[str, Any]:
        if side not in ("BUY", "SELL"):
            raise ValueError(f"Invalid side: {side}")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        # Formater la quantité sans notation scientifique (ex: 4e-05 → "0.00004")
        qty_decimal = Decimal(str(quantity))
        qty_str = f"{qty_decimal:.8f}".rstrip("0").rstrip(".")

        try:
            order = self._client.order_market(
                symbol=symbol,
                side=side,
                quantity=qty_str,
            )
            logger.info(
                f"Market order placed: {side} {quantity} {symbol} "
                f"order_id={order.get('orderId')}"
            )
            return order
        except BinanceAPIException as e:
            logger.error(f"Binance API error placing market order {side} {symbol}: code={e.code} {e.message}")
            raise
        except BinanceOrderException as e:
            logger.error(f"Binance order error {side} {symbol}: {e}")
            raise

    def place_oco_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_price: float,
        stop_limit_price: float,
    ) -> Dict[str, Any]:
        """Place an OCO order. Works for both SELL and BUY sides."""
        try:
            params = {
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "price": str(price),
                "stopPrice": str(stop_price),
                "stopLimitPrice": str(stop_limit_price),
                "stopLimitTimeInForce": "GTC",
            }
            order = self._client.create_oco_order(**params)
            logger.info(f"OCO order placed for {symbol} side={side}")
            return order
        except BinanceAPIException as e:
            logger.error(f"Binance API error placing OCO order {side} {symbol}: {e.message}")
            raise

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            self._client.cancel_order(symbol=symbol, orderId=order_id)
            logger.info(f"Order {order_id} cancelled for {symbol}")
            return True
        except BinanceAPIException as e:
            logger.error(f"Binance API error cancelling order {order_id} for {symbol}: {e.message}")
            return False

    def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        try:
            return self._client.get_order(symbol=symbol, orderId=order_id)
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting order {order_id} for {symbol}: {e.message}")
            raise

    def get_top_pairs(self, min_volume_usdt: float = 5_000_000) -> list:
        """Retourne tous les marchés USDT triés par volume 24h, filtrés par volume minimum."""
        try:
            tickers = self._client.get_ticker()
            pairs = []
            for t in tickers:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                base = sym[:-4]
                # Exclure les leverage tokens et stablecoins
                if any(x in base for x in ("UP", "DOWN", "BEAR", "BULL", "3L", "3S")):
                    continue
                if base in ("USDC", "BUSD", "TUSD", "DAI", "USDP", "FDUSD", "USDT"):
                    continue
                try:
                    vol = float(t.get("quoteVolume", 0))
                    price = float(t.get("lastPrice", 0))
                    change = float(t.get("priceChangePercent", 0))
                    high = float(t.get("highPrice", 0))
                    low = float(t.get("lowPrice", 0))
                except (ValueError, TypeError):
                    continue
                if vol < min_volume_usdt or price <= 0:
                    continue
                pairs.append({
                    "symbol": sym,
                    "base": base,
                    "price": price,
                    "change_pct": round(change, 2),
                    "volume_usdt": round(vol),
                    "high_24h": high,
                    "low_24h": low,
                })
            pairs.sort(key=lambda x: x["volume_usdt"], reverse=True)
            return pairs
        except BinanceAPIException as e:
            logger.error(f"Binance API error getting top pairs: {e.message}")
            raise

    def get_klines_raw(self, symbol: str, interval: str, limit: int = 200) -> list:
        """Return raw OHLCV list for the frontend chart endpoint."""
        df = self.get_klines(symbol, interval, limit)
        result = []
        for _, row in df.iterrows():
            result.append({
                "time": int(row["open_time"].timestamp() * 1000),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            })
        return result


binance_service = BinanceService()
