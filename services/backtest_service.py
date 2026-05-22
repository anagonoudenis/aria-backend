# NOUVEAU FICHIER — à créer
"""
Moteur de backtesting vectorisé sur données historiques Binance.
Utilise une stratégie rule-based (RSI + MACD + SMA) pour simuler les décisions
sans appeler Claude (trop lent pour des milliers de bougies).
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import numpy as np
import pandas as pd

from services.binance_service import binance_service
from services import analysis_service
from utils.logger import get_logger

logger = get_logger(__name__)

BINANCE_FEE = 0.001   # 0.1% par côté


class BacktestEngine:

    async def run(
        self,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
        initial_capital: float,
        bot_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Lance un backtest complet sur la période demandée.
        """
        cfg = bot_config or {}
        stop_loss_pct    = cfg.get("stop_loss_pct",    2.0)
        take_profit_pct  = cfg.get("take_profit_pct",  4.0)
        risk_per_trade   = cfg.get("risk_per_trade_pct", 2.0) / 100.0

        logger.info(f"Backtest: {symbol} {interval} {start_date}→{end_date} capital={initial_capital}")

        # ── 1. Télécharger les données historiques ───────────────────────────
        df = await self._fetch_historical(symbol, interval, start_date, end_date)
        if df is None or len(df) < 50:
            raise ValueError(f"Données insuffisantes pour {symbol} ({len(df) if df is not None else 0} bougies)")

        # ── 2. Calculer les indicateurs sur toute la série ───────────────────
        df = self._add_indicators(df)

        # ── 3. Simuler les trades ────────────────────────────────────────────
        capital     = initial_capital
        position    = None   # {"entry_price", "quantity", "entry_idx", "sl", "tp"}
        trades      = []
        equity_curve= []

        for i in range(50, len(df)):
            row  = df.iloc[i]
            date = row.get("open_time", df.index[i])
            price= float(row["close"])

            equity_curve.append({
                "date":  date.strftime("%Y-%m-%d %H:%M") if hasattr(date, "strftime") else str(date),
                "value": round(capital, 2),
            })

            # Gestion position ouverte
            if position:
                high  = float(df.iloc[i]["high"])
                low   = float(df.iloc[i]["low"])

                # Take-profit
                if high >= position["tp"]:
                    pnl = (position["tp"] - position["entry_price"]) * position["quantity"]
                    pnl -= position["entry_price"] * position["quantity"] * BINANCE_FEE
                    pnl -= position["tp"] * position["quantity"] * BINANCE_FEE
                    capital += pnl
                    trades.append(self._make_trade(position, position["tp"], date, pnl, "take_profit"))
                    position = None
                    continue

                # Stop-loss
                if low <= position["sl"]:
                    pnl = (position["sl"] - position["entry_price"]) * position["quantity"]
                    pnl -= position["entry_price"] * position["quantity"] * BINANCE_FEE
                    pnl -= position["sl"] * position["quantity"] * BINANCE_FEE
                    capital += pnl
                    trades.append(self._make_trade(position, position["sl"], date, pnl, "stop_loss"))
                    position = None
                    continue

            # Signal d'entrée (uniquement si pas en position)
            if not position and capital > 10:
                signal = self._get_signal(df, i)
                if signal == "BUY":
                    risk_amount = capital * risk_per_trade
                    qty         = risk_amount / (price * stop_loss_pct / 100)
                    cost        = qty * price * (1 + BINANCE_FEE)
                    if cost <= capital:
                        position = {
                            "entry_price": price,
                            "quantity":    qty,
                            "entry_idx":   i,
                            "entry_date":  date,
                            "sl": price * (1 - stop_loss_pct / 100),
                            "tp": price * (1 + take_profit_pct / 100),
                        }
                        capital -= cost

        # Fermer position ouverte en fin de période
        if position:
            last_price = float(df.iloc[-1]["close"])
            pnl = (last_price - position["entry_price"]) * position["quantity"]
            pnl -= position["entry_price"] * position["quantity"] * BINANCE_FEE
            pnl -= last_price * position["quantity"] * BINANCE_FEE
            capital += pnl
            trades.append(self._make_trade(position, last_price, df.iloc[-1].get("open_time", ""), pnl, "end_of_period"))

        # ── 4. Calculer les métriques ────────────────────────────────────────
        summary = self._compute_metrics(trades, initial_capital, capital, equity_curve)
        monthly = self._compute_monthly_returns(equity_curve)
        drawdowns = self._compute_drawdown_periods(equity_curve)

        return {
            "summary":        summary,
            "equity_curve":   equity_curve[-500:],  # limiter la taille
            "trades":         trades[-200:],
            "monthly_returns": monthly,
            "drawdown_periods": drawdowns,
        }

    def _get_signal(self, df: pd.DataFrame, i: int) -> str:
        """
        Stratégie rule-based : RSI + MACD + SMA trend-following.
        BUY si : RSI < 40 remonte + MACD histogram > 0 + prix > SMA20
        """
        try:
            row      = df.iloc[i]
            rsi      = float(row.get("rsi", 50))
            macd_h   = float(row.get("macd_h", 0))
            close    = float(row["close"])
            sma20    = float(row.get("sma20", close))
            prev_rsi = float(df.iloc[i - 1].get("rsi", 50))

            buy_condition = (
                rsi < 40 and
                prev_rsi < rsi and      # RSI en remontée
                macd_h > 0 and          # MACD positif
                close > sma20           # au-dessus de la SMA20
            )
            return "BUY" if buy_condition else "HOLD"
        except Exception:
            return "HOLD"

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        import ta
        df = df.copy()
        df["rsi"]    = ta.momentum.RSIIndicator(df["close"], window=14).rsi()
        df["sma20"]  = ta.trend.sma_indicator(df["close"], window=20)
        macd         = ta.trend.MACD(df["close"])
        df["macd_h"] = macd.macd_diff()
        df["atr"]    = ta.volatility.AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()
        return df.fillna(0)

    async def _fetch_historical(
        self, symbol: str, interval: str, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        Télécharge les klines Binance en plusieurs batches si nécessaire.
        """
        try:
            from binance.client import Client
            interval_map = {
                "1m": Client.KLINE_INTERVAL_1MINUTE,  "5m": Client.KLINE_INTERVAL_5MINUTE,
                "15m": Client.KLINE_INTERVAL_15MINUTE, "1h": Client.KLINE_INTERVAL_1HOUR,
                "4h": Client.KLINE_INTERVAL_4HOUR,    "1d": Client.KLINE_INTERVAL_1DAY,
            }
            b_interval = interval_map.get(interval, Client.KLINE_INTERVAL_1HOUR)

            start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
            end_ms   = int(datetime.strptime(end_date,   "%Y-%m-%d").timestamp() * 1000)

            all_klines = []
            current_ms = start_ms

            while current_ms < end_ms:
                klines = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda s=current_ms: binance_service._client.get_klines(
                        symbol=symbol, interval=b_interval,
                        startTime=s, endTime=end_ms, limit=1000
                    )
                )
                if not klines:
                    break
                all_klines.extend(klines)
                current_ms = int(klines[-1][0]) + 1
                if len(klines) < 1000:
                    break

            if not all_klines:
                return None

            df = pd.DataFrame(all_klines, columns=[
                "open_time","open","high","low","close","volume",
                "close_time","quote_vol","trades","taker_base","taker_quote","ignore"
            ])
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            for col in ["open","high","low","close","volume"]:
                df[col] = df[col].astype(float)
            return df

        except Exception as e:
            logger.error(f"Backtest fetch failed: {e}")
            return None

    def _make_trade(self, position: dict, close_price: float, close_date, pnl: float, reason: str) -> dict:
        entry = position["entry_price"]
        pct   = (pnl / (entry * position["quantity"])) * 100 if entry > 0 else 0
        return {
            "entry_price":  round(entry, 4),
            "exit_price":   round(close_price, 4),
            "quantity":     round(position["quantity"], 6),
            "pnl":          round(pnl, 4),
            "pnl_pct":      round(pct, 2),
            "entry_date":   str(position.get("entry_date", "")),
            "exit_date":    str(close_date),
            "close_reason": reason,
        }

    def _compute_metrics(
        self, trades: List[dict], initial: float, final: float,
        equity_curve: List[dict]
    ) -> dict:
        if not trades:
            return {
                "initial_capital": initial, "final_capital": round(final, 2),
                "total_return_pct": 0.0, "total_trades": 0, "winning_trades": 0,
                "losing_trades": 0, "win_rate": 0.0, "profit_factor": 0.0,
                "sharpe_ratio": 0.0, "sortino_ratio": 0.0,
                "max_drawdown_pct": 0.0, "max_drawdown_duration_days": 0,
                "avg_trade_duration_hours": 0.0, "best_trade_pct": 0.0,
                "worst_trade_pct": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
                "calmar_ratio": 0.0, "recovery_factor": 0.0, "expectancy_usdt": 0.0,
            }

        pnls    = [t["pnl"] for t in trades]
        pnl_pct = [t["pnl_pct"] for t in trades]
        wins    = [p for p in pnls if p > 0]
        losses  = [p for p in pnls if p < 0]
        win_pcts = [p for p in pnl_pct if p > 0]
        loss_pcts= [p for p in pnl_pct if p < 0]

        total_return = ((final - initial) / initial) * 100 if initial > 0 else 0
        win_rate     = len(wins) / len(trades) * 100 if trades else 0
        profit_factor= sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else 0

        # Equity returns for Sharpe
        equity_values = [e["value"] for e in equity_curve]
        returns = np.diff(equity_values) / np.array(equity_values[:-1]) if len(equity_values) > 1 else np.array([0])
        sharpe  = (np.mean(returns) / np.std(returns) * np.sqrt(252)) if np.std(returns) > 0 else 0

        # Sortino (penalise only downside)
        neg_returns = returns[returns < 0]
        down_std    = np.std(neg_returns) if len(neg_returns) > 0 else 1e-8
        sortino     = (np.mean(returns) / down_std * np.sqrt(252)) if down_std > 0 else 0

        # Max drawdown
        peak     = initial
        max_dd   = 0.0
        for e in equity_curve:
            v = e["value"]
            if v > peak:
                peak = v
            dd = (peak - v) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        calmar   = (total_return / max_dd) if max_dd > 0 else 0
        recovery = ((final - initial) / (max_dd / 100 * initial)) if max_dd > 0 else 0
        expectancy = sum(pnls) / len(pnls) if pnls else 0

        return {
            "initial_capital":          round(initial, 2),
            "final_capital":            round(final, 2),
            "total_return_pct":         round(total_return, 2),
            "total_trades":             len(trades),
            "winning_trades":           len(wins),
            "losing_trades":            len(losses),
            "win_rate":                 round(win_rate, 2),
            "profit_factor":            round(profit_factor, 3),
            "sharpe_ratio":             round(float(sharpe), 3),
            "sortino_ratio":            round(float(sortino), 3),
            "max_drawdown_pct":         round(max_dd, 2),
            "max_drawdown_duration_days": 0,
            "avg_trade_duration_hours": 0.0,
            "best_trade_pct":           round(max(pnl_pct, default=0), 2),
            "worst_trade_pct":          round(min(pnl_pct, default=0), 2),
            "avg_win_pct":              round(float(np.mean(win_pcts)), 2) if win_pcts else 0.0,
            "avg_loss_pct":             round(float(np.mean(loss_pcts)), 2) if loss_pcts else 0.0,
            "calmar_ratio":             round(float(calmar), 3),
            "recovery_factor":          round(float(recovery), 3),
            "expectancy_usdt":          round(float(expectancy), 4),
        }

    def _compute_monthly_returns(self, equity_curve: List[dict]) -> Dict[str, float]:
        if not equity_curve:
            return {}
        monthly: Dict[str, List[float]] = {}
        for point in equity_curve:
            try:
                date_str = point["date"][:7]  # "YYYY-MM"
                monthly.setdefault(date_str, []).append(point["value"])
            except Exception:
                continue
        result = {}
        prev_val = None
        for month in sorted(monthly):
            vals = monthly[month]
            if prev_val is not None and prev_val > 0:
                ret = (vals[-1] - prev_val) / prev_val * 100
                result[month] = round(ret, 2)
            prev_val = vals[-1]
        return result

    def _compute_drawdown_periods(self, equity_curve: List[dict]) -> List[dict]:
        if not equity_curve:
            return []
        periods = []
        in_dd = False
        dd_start = None
        peak = equity_curve[0]["value"]
        max_dd = 0.0

        for point in equity_curve:
            v = point["value"]
            if v > peak:
                if in_dd:
                    periods.append({"start": dd_start, "end": point["date"], "pct": round(max_dd, 2)})
                    in_dd = False; max_dd = 0.0
                peak = v
            else:
                dd = (peak - v) / peak * 100 if peak > 0 else 0
                if dd > 1.0 and not in_dd:
                    in_dd = True; dd_start = point["date"]
                if dd > max_dd:
                    max_dd = dd

        return periods[:20]


backtest_engine = BacktestEngine()
