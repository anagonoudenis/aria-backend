from typing import Dict, Any, List, Tuple, Optional
import numpy as np
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_DRAWDOWN_PCT       = 10.0   # 10% max drawdown (conservateur micro-compte)
MAX_DAILY_LOSS_PCT     = 3.0    # 3% perte max par jour
MIN_CONFIDENCE         = 0.65   # 65% min — signaux haute qualité uniquement
MIN_USDT               = 5.50   # min notional Binance + marge
MIN_ACCOUNT_USDT       = 6.0    # arrêt complet si capital < 6 USDT
CIRCUIT_BREAKER_LOSSES = 3      # pause après 3 pertes consécutives
MAX_ATR_RATIO          = 3.0    # volatilité max réduite
KELLY_FRACTION         = 0.20   # Kelly conservateur

CORRELATED_PAIRS = [
    {"BTCUSDT", "ETHUSDT"},
    {"BNBUSDT", "SOLUSDT"},
]


class RiskManager:

    def validate_trade(
        self,
        signal: Dict[str, Any],
        portfolio: Dict[str, Any],
        bot_config: Dict[str, Any],
        open_trades_count: int = 0,
        open_trade_symbols: Optional[List[str]] = None,
        consecutive_losses: int = 0,
        indicators: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:

        action = signal.get("action", "HOLD")
        if action == "HOLD":
            return False, "Signal HOLD"

        # Garde-fou capital minimum absolu
        total_usdt = portfolio.get("total_usdt", portfolio.get("available_usdt", 0))
        if total_usdt < MIN_ACCOUNT_USDT:
            return False, f"Capital insuffisant pour trader: {total_usdt:.2f} < {MIN_ACCOUNT_USDT} USDT"

        # Veto TradingAgents : bloque un BUY si la recherche dit SELL
        from services.research_reader import get_bias_sync
        bias = get_bias_sync(signal.get("symbol", ""))
        if bias and bias.get("rating") == "SELL" and action == "BUY":
            return False, f"TradingAgents SELL — BUY bloqué sur {signal.get('symbol', '')}"

        confidence = signal.get("confidence", 0.0)
        if confidence < MIN_CONFIDENCE:
            return False, f"Confiance insuffisante: {confidence:.2f} < {MIN_CONFIDENCE}"

        available = portfolio.get("available_usdt", 0.0)
        if available < MIN_USDT:
            return False, f"Capital insuffisant: {available:.2f} USDT < {MIN_USDT}"

        max_open = bot_config.get("max_open_trades", 1)
        if open_trades_count >= max_open:
            return False, f"Position deja ouverte ({open_trades_count}/{max_open})"

        if consecutive_losses >= CIRCUIT_BREAKER_LOSSES:
            return False, f"Circuit breaker: {consecutive_losses} pertes consecutives"

        if indicators:
            atr_ratio = indicators.get("volatility", {}).get("atr_ratio", 1.0)
            if atr_ratio > MAX_ATR_RATIO:
                return False, f"Volatilite excessive: ATR ratio={atr_ratio:.2f}"

        total_pnl_pct = portfolio.get("total_pnl_pct", 0.0)
        if total_pnl_pct <= -MAX_DRAWDOWN_PCT:
            return False, f"Drawdown max atteint: {total_pnl_pct:.2f}%"

        return True, "Trade valide"

    def calculate_position_size(
        self,
        usdt_available: float,
        risk_pct: float,
        entry_price: float,
        stop_loss_pct: float,
        max_positions: int = 1,
        symbol: str = "",
    ) -> float:
        """
        Taille adaptative : divise le capital par le nombre de positions max.
        Applique un multiplicateur TradingAgents selon la conviction multi-agents.
        """
        size = (usdt_available / max(max_positions, 1)) * 0.90

        # Multiplicateur TradingAgents Intelligence
        from services.research_reader import get_bias_sync
        bias = get_bias_sync(symbol)
        if bias:
            rating = bias.get("rating", "HOLD")
            if rating == "SELL":
                return 0.0
            multiplier = bias.get("size_multiplier", 1.0)
            size = size * multiplier
            logger.info(f"TradingAgents ×{multiplier:.1f} ({rating}) sur {symbol}")

        size = max(size, 5.50)
        size = min(size, usdt_available * 0.95)
        logger.info(f"Position size: {size:.2f} USDT (capital={usdt_available:.2f})")
        return size

    def kelly_position_size(
        self,
        win_rate: float, avg_win: float, avg_loss: float,
        capital: float, trade_count: int = 0,
    ) -> float:
        if trade_count < 20 or avg_loss <= 0:
            return capital * 0.95

        b = avg_win / avg_loss
        p, q = win_rate, 1.0 - win_rate
        kelly = max(0.0, min((b * p - q) / b, 0.25))
        size = capital * kelly * KELLY_FRACTION
        return max(size, MIN_USDT)

    def count_consecutive_losses(self, recent_trades: List[Dict[str, Any]]) -> int:
        count = 0
        for trade in reversed(recent_trades):
            pnl = trade.get("pnl")
            if pnl is None: continue
            if float(pnl) < 0: count += 1
            else: break
        return count

    def check_drawdown(self, portfolio: Dict[str, Any]) -> bool:
        pct = portfolio.get("total_pnl_pct", 0.0)
        if pct <= -MAX_DRAWDOWN_PCT:
            logger.warning(f"Max drawdown: {pct:.2f}%")
            return False
        return True

    def check_daily_loss_limit(self, trades_today: List[Dict[str, Any]]) -> bool:
        if not trades_today: return True
        daily_pnl = sum(t.get("pnl", 0.0) or 0.0 for t in trades_today)
        invested  = sum(t.get("total_usdt", 0.0) for t in trades_today if t.get("total_usdt"))
        if invested <= 0: return True
        if (daily_pnl / invested * 100) <= -MAX_DAILY_LOSS_PCT:
            logger.warning(f"Daily loss limit: {daily_pnl/invested*100:.2f}%")
            return False
        return True

    def compute_trailing_stop(
        self, entry_price: float, highest_price_seen: float,
        trailing_pct: float, side: str = "BUY",
    ) -> float:
        if side == "BUY":
            return highest_price_seen * (1.0 - trailing_pct / 100.0)
        return highest_price_seen * (1.0 + trailing_pct / 100.0)

    def should_trigger_trailing_stop(
        self, current_price: float, trailing_stop_price: float, side: str = "BUY",
    ) -> bool:
        return current_price <= trailing_stop_price if side == "BUY" else current_price >= trailing_stop_price

    def get_risk_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        closed = [t for t in trades if t.get("status") == "CLOSED" and t.get("pnl") is not None]
        if not closed:
            return {"sharpe_ratio": None, "max_drawdown": 0.0, "avg_win": 0.0,
                    "avg_loss": 0.0, "risk_reward_ratio": 0.0, "profit_factor": 0.0,
                    "consecutive_losses": 0, "total_closed": 0}

        pnl_values = [float(t["pnl"]) for t in closed]
        wins   = [p for p in pnl_values if p > 0]
        losses = [p for p in pnl_values if p < 0]

        avg_win   = float(np.mean(wins))   if wins   else 0.0
        avg_loss  = abs(float(np.mean(losses))) if losses else 0.0
        rr        = avg_win / avg_loss if avg_loss > 0 else 0.0
        pf        = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else 0.0

        cumulative  = np.cumsum(pnl_values)
        running_max = np.maximum.accumulate(cumulative)
        max_dd      = float(abs(np.min(cumulative - running_max))) if len(pnl_values) > 0 else 0.0

        sharpe = None
        if len(pnl_values) > 1:
            mean, std = np.mean(pnl_values), np.std(pnl_values)
            if std > 0: sharpe = float((mean / std) * np.sqrt(252))

        return {
            "sharpe_ratio": sharpe, "max_drawdown": max_dd,
            "avg_win": avg_win, "avg_loss": avg_loss,
            "risk_reward_ratio": rr, "profit_factor": pf,
            "consecutive_losses": self.count_consecutive_losses(closed),
            "total_closed": len(closed),
        }


risk_manager = RiskManager()
