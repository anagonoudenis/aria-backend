import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from database import get_database
from services import analysis_service, claude_service
from services.binance_service import binance_service
from services.risk_manager import risk_manager
from services.notification_service import notification_service
from models.signal import SignalInDB
from models.trade import TradeInDB, TradeSide, TradeStatus
from models.portfolio import PortfolioSnapshot
from utils.cache import cache, KLINE_TTL, INDICATOR_TTL, PRICE_TTL
from utils.logger import get_logger
from services import websocket_feed
from services.market_data_service import get_order_book_imbalance

logger = get_logger(__name__)

INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
BINANCE_FEE      = 0.001

# 12 paires liquides — min notional $5 sur Binance Spot
SCAN_PAIRS = [
    "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT",
    "DOTUSDT", "LTCUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT",
    "MATICUSDT", "ETHUSDT",
]
# Paires nécessitant min notional $10 — exclues si capital < $15
HIGH_NOTIONAL_PAIRS = {"BTCUSDT", "ETHUSDT"}

# TP/SL calibrés sur données réelles — ratio 2.5:1
TRAIL_TRIGGER_PCT  = 1.2
TRAIL_STEP_PCT     = 0.6
BREAKEVEN_PCT      = 0.8
STOP_LOSS_PCT      = 0.8
TAKE_PROFIT_PCT    = 2.0

# Scalping haute confiance
SCALP_CONFIDENCE   = 0.85
SCALP_SL_PCT       = 0.4
SCALP_TP_PCT       = 1.0

# Filtres qualité signal — évite les mauvais trades
MIN_CONFIDENCE       = 0.65
MIN_COMPOSITE_SCORE  = 20.0
MAX_CONSECUTIVE_LOSSES = 5
SL_COOLDOWN_SECONDS  = 45 * 60  # 45 min cooldown par paire après SL

_active_cycles: set = set()


class BotEngine:
    def __init__(self):
        self._scheduler = AsyncIOScheduler()
        self._running_bots: Dict[str, Dict[str, Any]] = {}
        logger.info("BotEngine created")

    def initialize(self) -> None:
        if not self._scheduler.running:
            self._scheduler.start()
            logger.info("BotEngine scheduler started")

    async def start(self, user_id: str, bot_config: Dict[str, Any]) -> None:
        if user_id in self._running_bots:
            return
        interval = bot_config.get("interval", "5m")
        interval_seconds = INTERVAL_SECONDS.get(interval, 300)

        self._running_bots[user_id] = {
            "config": bot_config,
            "started_at": datetime.now(timezone.utc),
            "cycles_count": 0, "last_cycle_at": None,
            "consecutive_losses": 0,
            "circuit_breaker_active": False,
            "circuit_breaker_reason": None,
            "scan_results": {},
            "sl_cooldown": {},       # {symbol: datetime} — paires en cooldown après SL
            "pending_entry": None,   # signal en attente d'un pullback vers EMA9
        }

        db = get_database()
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"bot_config.is_running": True}})
        logger.info(f"Bot started: user={user_id} interval={interval} scanning={SCAN_PAIRS}")
        asyncio.create_task(self._bot_loop(user_id, interval_seconds))
        if len(self._running_bots) == 1:
            try:
                await websocket_feed.start(list(set(SCAN_PAIRS + ["BTCUSDT"])))
            except Exception as ws_err:
                logger.warning(f"WS feed start failed (non-bloquant): {ws_err}")

    async def _bot_loop(self, user_id: str, interval_seconds: int) -> None:
        logger.info(f"[{user_id}] Loop started ({interval_seconds}s)")
        await self._run_cycle_safe(user_id)
        while self.is_running(user_id):
            await asyncio.sleep(interval_seconds)
            if self.is_running(user_id):
                await self._run_cycle_safe(user_id)
        logger.info(f"[{user_id}] Loop stopped")

    async def stop(self, user_id: str, reason: str = "Manuel") -> None:
        if user_id not in self._running_bots:
            return
        del self._running_bots[user_id]
        db = get_database()
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"bot_config.is_running": False}})
        logger.info(f"Bot stopped: {user_id} ({reason})")
        if not self._running_bots:
            await websocket_feed.stop()

    def is_running(self, user_id: str) -> bool:
        return user_id in self._running_bots

    def get_status(self, user_id: str) -> Optional[Dict[str, Any]]:
        return self._running_bots.get(user_id)

    async def _run_cycle_safe(self, user_id: str) -> None:
        if user_id in _active_cycles:
            return
        _active_cycles.add(user_id)
        try:
            await self.run_cycle(user_id)
        except Exception as e:
            logger.error(f"Cycle error {user_id}: {e}")
        finally:
            _active_cycles.discard(user_id)

    # ══════════════════════════════════════════════════════════════════════════
    # CYCLE PRINCIPAL — MULTI-PAIRES
    # ══════════════════════════════════════════════════════════════════════════

    async def run_cycle(self, user_id: str) -> None:
        if user_id not in self._running_bots:
            return

        bot_info = self._running_bots[user_id]
        config   = bot_info["config"]

        if bot_info.get("circuit_breaker_active"):
            logger.warning(f"[{user_id}] Circuit breaker actif — cycle ignore")
            return

        # ── 0. Filtre horaire — pas de trade entre 1h et 7h UTC ─────────────
        utc_hour = datetime.now(timezone.utc).hour
        if 1 <= utc_hour < 7:
            logger.info(f"[{user_id}] Heure creuse ({utc_hour}h UTC) — cycle skip")
            return

        # ── 1. Gérer les positions ouvertes AVANT d'ouvrir de nouvelles ─────
        all_scan = list(set(SCAN_PAIRS) | {"BTCUSDT"})
        await self._manage_all_positions(user_id, all_scan)

        # ── 2. Portefeuille — source de vérité ───────────────────────────────
        db = get_database()
        portfolio_data = await self._get_portfolio(db, user_id)
        available_usdt = portfolio_data["available_usdt"]

        logger.info(f"[{user_id}] Cycle start — capital={available_usdt:.2f} USDT")

        if available_usdt < 5.5:
            logger.info(f"[{user_id}] Capital insuffisant ({available_usdt:.2f}) — pas de nouveau trade")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 3. Prix courants en parallèle ────────────────────────────────────
        prices = await self._fetch_prices(all_scan)

        # ── 3b. Vérifier entrée en attente (pullback) ─────────────────────────
        executed_pending = await self._check_pending_entries(user_id, prices, db, portfolio_data)
        if executed_pending:
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 4. Circuit breaker — arrêt après MAX pertes consécutives ─────────
        recent_trades = await db.trades.find(
            {"user_id": user_id, "status": "CLOSED"}
        ).sort("created_at", -1).limit(50).to_list(50)

        consecutive = risk_manager.count_consecutive_losses(recent_trades)
        bot_info["consecutive_losses"] = consecutive

        if consecutive >= MAX_CONSECUTIVE_LOSSES:
            reason = f"Circuit breaker: {consecutive} pertes consécutives"
            bot_info["circuit_breaker_active"] = True
            bot_info["circuit_breaker_reason"]  = reason
            await notification_service.send(user_id, "circuit_breaker", {"reason": reason})
            logger.warning(f"[{user_id}] {reason}")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 4b. Filtre BTC macro — pas de BUY si BTC en tendance baissière ──────
        btc_ok, btc_reason = await self._check_btc_macro()
        bot_info["btc_context"] = btc_reason  # pass to Sonnet final validation
        if not btc_ok:
            logger.info(f"[{user_id}] Filtre BTC: {btc_reason}")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return
        logger.info(f"[{user_id}] BTC macro OK — {btc_reason}")

        # ── 5. Positions ouvertes + limite adaptative ─────────────────────────
        open_trades = await db.trades.find(
            {"user_id": user_id, "status": "OPEN"}
        ).to_list(20)
        open_symbols  = {t.get("symbol") for t in open_trades}
        max_positions = self._get_max_positions(available_usdt)

        if len(open_trades) >= max_positions:
            logger.info(f"[{user_id}] Max positions ({len(open_trades)}/{max_positions}) — attente")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 6. SCAN MULTI-PAIRES — meilleure opportunité ──────────────────────
        best = await self._scan_best_opportunity(
            user_id, SCAN_PAIRS, open_symbols, prices, config, portfolio_data, btc_reason
        )

        if best is None:
            logger.info(f"[{user_id}] Aucune opportunité ce cycle")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        symbol        = best["symbol"]
        signal_data   = best["signal"]
        indicators    = best["indicators"]
        current_price = prices.get(symbol, 0)

        logger.info(
            f"[{user_id}] Signal: {symbol} "
            f"action={signal_data['action']} conf={signal_data['confidence']:.0%} "
            f"score={best['composite_score']:.1f}"
        )

        # ── 7. Sauvegarder le signal ──────────────────────────────────────────
        await self._save_signal(db, user_id, symbol, signal_data, indicators, current_price)
        await self._broadcast(user_id, "signal", {
            **signal_data, "symbol": symbol, "price": current_price
        })

        # ── 8. Filtres qualité — seulement les bons signaux ───────────────────
        if signal_data["action"] != "BUY":
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        if signal_data["confidence"] < MIN_CONFIDENCE:
            logger.info(
                f"[{user_id}] Confiance {signal_data['confidence']:.0%} "
                f"< {MIN_CONFIDENCE:.0%} — signal ignore"
            )
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        if best["composite_score"] < MIN_COMPOSITE_SCORE:
            logger.info(
                f"[{user_id}] Score {best['composite_score']:.1f} "
                f"< {MIN_COMPOSITE_SCORE} — signal ignore"
            )
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 8b. Filtre régime de marché — refuse les mauvaises conditions ────
        market_regime   = signal_data.get("market_regime", "RANGING")
        tf_alignment    = signal_data.get("timeframe_alignment", "MODERATE")

        # Jamais acheter en tendance baissière confirmée
        if market_regime == "TRENDING_DOWN":
            logger.info(f"[{user_id}] Regime TRENDING_DOWN — trade refuse")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # Jamais acheter avec signal faible
        if tf_alignment == "WEAK":
            logger.info(f"[{user_id}] Alignement WEAK — trade refuse")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # En marché volatile ou en range : exiger un score plus élevé
        if market_regime in ("VOLATILE", "RANGING"):
            required_score = MIN_COMPOSITE_SCORE * 1.25  # +25% de seuil
            if best["composite_score"] < required_score:
                logger.info(
                    f"[{user_id}] Regime {market_regime}: score {best['composite_score']:.1f} "
                    f"< {required_score:.1f} — trade refuse"
                )
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return

        # ── 9. Validation risk manager ────────────────────────────────────────
        is_valid, reason = risk_manager.validate_trade(
            signal_data, portfolio_data, config,
            len(open_trades), list(open_symbols), consecutive, indicators,
        )

        if not is_valid:
            logger.info(f"[{user_id}] Trade refusé: {reason}")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 10. SL/TP dynamiques basés sur ATR — ratio 2:1 garanti ──────────
        conf = signal_data["confidence"]
        if conf >= SCALP_CONFIDENCE:
            sl_pct = SCALP_SL_PCT
            tp_pct = SCALP_TP_PCT
            logger.info(f"[{user_id}] Mode SCALPING ({conf:.0%}): SL={sl_pct}% TP={tp_pct}%")
        else:
            atr = indicators.get("volatility", {}).get("atr", 0)
            if atr > 0 and current_price > 0:
                raw_sl = (1.2 * atr / current_price) * 100
                raw_tp = (2.5 * atr / current_price) * 100
                # Bornes de securite : SL [0.5% — 1.5%], TP [1.5% — 4.0%]
                sl_pct = round(max(min(raw_sl, 1.5), 0.5), 3)
                tp_pct = round(max(min(raw_tp, 4.0), 1.5), 3)
                logger.info(
                    f"[{user_id}] ATR={atr:.4f} SL={sl_pct}% TP={tp_pct}% "
                    f"ratio={tp_pct/sl_pct:.1f}:1"
                )
            else:
                sl_pct = STOP_LOSS_PCT
                tp_pct = TAKE_PROFIT_PCT
                logger.info(f"[{user_id}] ATR indisponible — SL={sl_pct}% TP={tp_pct}% (defaut)")

        # ── 11. Kelly position sizing ─────────────────────────────────────────
        base_usdt    = (available_usdt / max_positions) * 0.90
        win_rate_pct = portfolio_data.get("win_rate", 0)
        kelly_mult   = 1.0
        if win_rate_pct > 0:
            win_r    = win_rate_pct / 100
            rr_ratio = tp_pct / sl_pct if sl_pct > 0 else 2.5
            kelly    = win_r - (1 - win_r) / rr_ratio
            # Half-Kelly, bounded: [0.7x, 1.5x] base sizing
            kelly_mult   = 1.0 + max(min(kelly * 0.5, 0.5), -0.3)
            position_usdt = base_usdt * kelly_mult
        else:
            position_usdt = base_usdt
        position_usdt = max(position_usdt, 5.50)
        position_usdt = min(position_usdt, available_usdt * 0.95)
        logger.info(
            f"[{user_id}] Position: {position_usdt:.2f} USDT "
            f"(capital={available_usdt:.2f} WR={win_rate_pct:.0f}% kelly_mult={kelly_mult:.2f})"
        )

        # ── 12. Pullback entry — attendre EMA9 si prix trop haut ─────────────
        ema9 = indicators.get("trend", {}).get("ema_9", 0)
        if ema9 > 0 and current_price > ema9 * 1.003:
            # Prix > EMA9 + 0.3% → attendre un pullback vers EMA9
            bot_info["pending_entry"] = {
                "symbol":        symbol,
                "target_entry":  round(ema9, 8),
                "signal_data":   signal_data,
                "indicators":    indicators,
                "sl_pct":        sl_pct,
                "tp_pct":        tp_pct,
                "config":        config,
                "created_at":    datetime.now(timezone.utc),
                "expires_at":    datetime.now(timezone.utc) + timedelta(minutes=5),
            }
            logger.info(
                f"[{user_id}] Pullback pending {symbol}: "
                f"prix={current_price:.4f} > EMA9={ema9:.4f} — attente retour"
            )
        else:
            # Prix deja proche ou sous EMA9 → entree immediate
            await self._execute_buy(
                user_id, db, symbol, position_usdt, current_price,
                sl_pct, tp_pct, signal_data, portfolio_data, config,
                indicators=indicators,
            )

        bot_info["cycles_count"] += 1
        bot_info["last_cycle_at"] = datetime.now(timezone.utc)

    # ══════════════════════════════════════════════════════════════════════════
    # FILTRE BTC MACRO
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_btc_macro(self) -> Tuple[bool, str]:
        """
        Vérifie la direction macro de BTC sur 15m.
        Conditions pour trader :
          - BTC close > EMA21 (tendance court terme haussière)
          - EMA21 > SMA50  (structure de tendance confirmée)
        Retourne (can_trade: bool, reason: str).
        """
        try:
            key_df = "klines:BTCUSDT:15m"
            df = cache.get(key_df)
            if df is None:
                df = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines("BTCUSDT", "15m", limit=100)
                )
                cache.set(key_df, df, ttl_seconds=KLINE_TTL.get("15m", 900))

            key_ind = "indicators:BTCUSDT:15m"
            ind = cache.get(key_ind)
            if ind is None:
                ind = analysis_service.compute_indicators(df)
                cache.set(key_ind, ind, ttl_seconds=INDICATOR_TTL.get("15m", 900))

            trend   = ind.get("trend", {})
            candles = ind.get("candles_summary", [])
            btc_close = candles[-1]["close"] if candles else 0
            ema21     = trend.get("ema_21", 0)
            sma50     = trend.get("sma_50", 0)

            if btc_close <= 0 or ema21 <= 0:
                return True, "donnees BTC indisponibles — trade autorise par defaut"

            if btc_close < ema21:
                return False, f"BTC {btc_close:.0f} < EMA21 {ema21:.0f} — marche baissier"

            if sma50 > 0 and ema21 < sma50:
                return False, f"BTC EMA21 {ema21:.0f} < SMA50 {sma50:.0f} — structure baissiere"

            return True, f"BTC {btc_close:.0f} > EMA21 {ema21:.0f} — marche haussier"

        except Exception as e:
            logger.warning(f"BTC macro check failed: {e} — trade autorise par defaut")
            return True, "erreur BTC check — autorise par defaut"

    # ══════════════════════════════════════════════════════════════════════════
    # PULLBACK ENTRY — ENTRÉE SUR RETOUR EMA9
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_pending_entries(
        self,
        user_id: str,
        prices: Dict[str, float],
        db,
        portfolio_data: Dict[str, Any],
    ) -> bool:
        """
        Vérifie si une entrée en attente (pullback) peut être exécutée.
        Retourne True si un trade a été exécuté, False sinon.
        """
        bot_info = self._running_bots.get(user_id, {})
        pending  = bot_info.get("pending_entry")
        if not pending:
            return False

        symbol     = pending["symbol"]
        target     = pending["target_entry"]
        expires_at = pending["expires_at"]
        now        = datetime.now(timezone.utc)

        # Expiré — annuler
        if now > expires_at:
            logger.info(f"[{user_id}] Pending entry {symbol} expire — annule")
            bot_info["pending_entry"] = None
            return False

        # Paire désormais en position ouverte — annuler
        open_syms = {
            t.get("symbol") for t in
            await db.trades.find({"user_id": user_id, "status": "OPEN"}).to_list(20)
        }
        if symbol in open_syms:
            logger.info(f"[{user_id}] Pending entry {symbol} annule — position deja ouverte")
            bot_info["pending_entry"] = None
            return False

        current_price = prices.get(symbol, 0)
        if current_price <= 0:
            return False

        # Prix revenu à la cible EMA9 (tolérance 0.2%)
        if current_price <= target * 1.002:
            available_usdt = portfolio_data.get("available_usdt", 0.0)
            if available_usdt < 5.5:
                logger.info(f"[{user_id}] Pending entry {symbol} — capital insuffisant ({available_usdt:.2f})")
                bot_info["pending_entry"] = None
                return False

            max_positions = self._get_max_positions(available_usdt)
            position_usdt = (available_usdt / max_positions) * 0.90
            position_usdt = max(position_usdt, 5.50)
            position_usdt = min(position_usdt, available_usdt * 0.95)

            logger.info(
                f"[{user_id}] Pullback atteint {symbol}: "
                f"prix={current_price:.4f} <= cible={target:.4f} — execution"
            )

            config = pending["config"]
            await self._execute_buy(
                user_id, db, symbol, position_usdt, current_price,
                pending["sl_pct"], pending["tp_pct"],
                pending["signal_data"], portfolio_data, config,
                indicators=pending.get("indicators"),
            )
            bot_info["pending_entry"] = None
            return True

        remaining = int((expires_at - now).total_seconds())
        logger.info(
            f"[{user_id}] Pending {symbol}: prix={current_price:.4f} "
            f"cible={target:.4f} — attente ({remaining}s restants)"
        )
        return False

    # ══════════════════════════════════════════════════════════════════════════
    # SCAN MULTI-PAIRES
    # ══════════════════════════════════════════════════════════════════════════

    async def _scan_best_opportunity(
        self, user_id: str,
        symbols: List[str],
        open_symbols: set,
        prices: Dict[str, float],
        config: Dict[str, Any],
        portfolio_data: Optional[Dict[str, Any]] = None,
        btc_context: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Triple timeframe scan (5m+15m+1h) + Order Book + Sonnet final validation.
        Filters: HIGH_NOTIONAL cap, SL cooldown, EMA trend, MTF confluence, OB pressure.
        """
        available = (portfolio_data or {}).get("available_usdt", 0.0)

        now = datetime.now(timezone.utc)
        sl_cooldown = self._running_bots.get(user_id, {}).get("sl_cooldown", {})
        candidates = [
            s for s in symbols
            if s not in open_symbols
            and (s not in HIGH_NOTIONAL_PAIRS or available >= 15.0)
            and (
                s not in sl_cooldown
                or (now - sl_cooldown[s]).total_seconds() > SL_COOLDOWN_SECONDS
            )
        ]
        if not candidates:
            return None

        # Phase 1 : triple timeframe analysis en parallèle
        tasks   = [self._analyze_pair_fast(s) for s in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scored = []
        for sym, result in zip(candidates, results):
            if isinstance(result, Exception) or result is None:
                continue

            ind_5m   = result["ind_5m"]
            rule_5m  = result["rule_5m"]
            rule_15m = result["rule_15m"]
            ind_1h   = result.get("ind_1h", {})
            rule_1h  = result.get("rule_1h", {})

            action_5m  = rule_5m.get("action", "HOLD")
            action_15m = rule_15m.get("action", "HOLD")
            action_1h  = rule_1h.get("action", "HOLD")
            score_5m   = rule_5m.get("score", 0)

            # EMA trend filter for BUY
            if action_5m == "BUY":
                trend   = ind_5m.get("trend", {})
                candles = ind_5m.get("candles_summary", [])
                close   = candles[-1]["close"] if candles else 0
                ema9    = trend.get("ema_9", 0)
                ema21   = trend.get("ema_21", 0)
                if not (ema9 > ema21 and close > ema21):
                    logger.debug(f"{sym}: EMA filter failed ema9={ema9:.4f} ema21={ema21:.4f}")
                    continue

            # 5m vs 15m contradiction
            if action_5m == "BUY" and action_15m == "SELL":
                logger.debug(f"{sym}: 5m/15m contradiction BUY/SELL")
                continue
            if action_5m == "SELL" and action_15m == "BUY":
                logger.debug(f"{sym}: 5m/15m contradiction SELL/BUY")
                continue

            # 1h contradiction — 5m BUY but 1h SELL → skip
            if action_5m == "BUY" and action_1h == "SELL":
                logger.debug(f"{sym}: 1h SELL contradicts 5m BUY — skip")
                continue

            # Confluence multiplier: triple > double > single
            triple_bull = (action_5m == "BUY" and action_15m == "BUY" and action_1h == "BUY")
            double_bull = (action_5m == "BUY" and action_15m == "BUY")

            if triple_bull:
                confluence_mult = 1.5
            elif double_bull:
                confluence_mult = 1.3
            else:
                confluence_mult = 1.0

            score = round(score_5m * confluence_mult, 1)

            scored.append({
                "symbol":        sym,
                "score":         score,
                "action":        action_5m,
                "indicators":    ind_5m,
                "rule_sig":      rule_5m,
                "confluence_15m": action_15m,
                "confluence_1h": action_1h,
                "ind_1h":        ind_1h,
                "triple_bull":   triple_bull,
            })

        if not scored:
            return None

        # Phase 2 : best candidate
        scored.sort(key=lambda x: x["score"], reverse=True)
        best   = scored[0]
        sym    = best["symbol"]
        indicators = best["indicators"]
        rule_sig   = best["rule_sig"]
        ind_1h     = best.get("ind_1h") or {}
        price      = prices.get(sym, 0)
        pf         = portfolio_data or {}

        logger.info(
            f"[{user_id}] Best: {sym} score={best['score']:.1f} "
            f"5m={best['action']} 15m={best['confluence_15m']} 1h={best['confluence_1h']}"
            + (" [TRIPLE BULL]" if best.get("triple_bull") else "")
        )

        # Phase 2b : Order Book Imbalance check
        order_book = None
        try:
            order_book = await asyncio.get_event_loop().run_in_executor(
                None, lambda: get_order_book_imbalance(sym)
            )
            ob_signal = order_book.get("imbalance_signal", "NEUTRAL")
            ob_score  = order_book.get("imbalance_score", 0.0)
            ob_ratio  = order_book.get("bid_ask_ratio", 1.0)
            logger.info(f"[{user_id}] OB {sym}: ratio={ob_ratio:.2f} {ob_signal} ({ob_score:.0%})")
            if ob_signal == "SELL" and ob_score > 0.5:
                logger.info(f"[{user_id}] OB strong SELL pressure on {sym} — skip")
                return None
        except Exception as obe:
            logger.debug(f"OB check {sym} failed: {obe}")

        # Phase 3 : Claude Sonnet final validation
        try:
            signal_data = await claude_service.analyze_market_final(
                sym, indicators, price, pf,
                order_book=order_book,
                ind_1h=ind_1h if ind_1h else None,
                btc_context=btc_context,
            )
        except Exception as e:
            logger.warning(f"Sonnet failed for {sym}: {e} — using rule-based")
            signal_data = claude_service._from_rule(rule_sig, indicators)

        composite = signal_data["confidence"] * 10 + best["score"]

        return {
            "symbol":          sym,
            "signal":          signal_data,
            "indicators":      indicators,
            "composite_score": composite,
        }

    async def _analyze_pair_fast(self, symbol: str) -> Optional[Dict]:
        """Triple timeframe analysis: 5m + 15m + 1h. Returns dict with all data."""
        try:
            # ── 5m ───────────────────────────────────────────────────────────
            key_5m = f"klines:{symbol}:5m"
            df_5m  = cache.get(key_5m)
            if df_5m is None:
                df_5m = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines(symbol, "5m", limit=100)
                )
                cache.set(key_5m, df_5m, ttl_seconds=KLINE_TTL.get("5m", 300))

            ind_key_5m = f"indicators:{symbol}:5m"
            ind_5m = cache.get(ind_key_5m)
            if ind_5m is None:
                ind_5m = analysis_service.compute_indicators(df_5m)
                cache.set(ind_key_5m, ind_5m, ttl_seconds=INDICATOR_TTL.get("5m", 300))

            rule_5m = ind_5m.get("rule_signal", {})

            # ── 15m ──────────────────────────────────────────────────────────
            rule_15m: Dict = {}
            try:
                key_15m = f"klines:{symbol}:15m"
                df_15m  = cache.get(key_15m)
                if df_15m is None:
                    df_15m = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: binance_service.get_klines(symbol, "15m", limit=100)
                    )
                    cache.set(key_15m, df_15m, ttl_seconds=KLINE_TTL.get("15m", 900))

                ind_key_15m = f"indicators:{symbol}:15m"
                ind_15m = cache.get(ind_key_15m)
                if ind_15m is None:
                    ind_15m = analysis_service.compute_indicators(df_15m)
                    cache.set(ind_key_15m, ind_15m, ttl_seconds=INDICATOR_TTL.get("15m", 900))

                rule_15m = ind_15m.get("rule_signal", {})
            except Exception as e15:
                logger.debug(f"15m {symbol} failed (non bloquant): {e15}")

            # ── 1h ───────────────────────────────────────────────────────────
            ind_1h: Dict = {}
            rule_1h: Dict = {}
            try:
                key_1h = f"klines:{symbol}:1h"
                df_1h  = cache.get(key_1h)
                if df_1h is None:
                    df_1h = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: binance_service.get_klines(symbol, "1h", limit=100)
                    )
                    cache.set(key_1h, df_1h, ttl_seconds=KLINE_TTL.get("1h", 3600))

                ind_key_1h = f"indicators:{symbol}:1h"
                ind_1h = cache.get(ind_key_1h)
                if ind_1h is None:
                    ind_1h = analysis_service.compute_indicators(df_1h)
                    cache.set(ind_key_1h, ind_1h, ttl_seconds=INDICATOR_TTL.get("1h", 3600))

                rule_1h = ind_1h.get("rule_signal", {})
            except Exception as e1h:
                logger.debug(f"1h {symbol} failed (non bloquant): {e1h}")

            return {
                "ind_5m":  ind_5m,  "rule_5m":  rule_5m,
                "rule_15m": rule_15m,
                "ind_1h":  ind_1h,  "rule_1h":  rule_1h,
            }

        except Exception as e:
            logger.debug(f"Fast analysis {symbol} failed: {e}")
            return None

    # ══════════════════════════════════════════════════════════════════════════
    # GESTION ACTIVE DES POSITIONS — TRAILING TP
    # ══════════════════════════════════════════════════════════════════════════

    async def _manage_all_positions(self, user_id: str, symbols: List[str]) -> None:
        """Vérifie toutes les positions ouvertes — trailing TP, SL, breakeven."""
        db = get_database()

        open_trades = await db.trades.find(
            {"user_id": user_id, "status": "OPEN"}
        ).to_list(20)

        if not open_trades:
            return

        # Fetch tous les prix en parallèle
        syms_needed = list({t.get("symbol") for t in open_trades})
        prices = await self._fetch_prices(syms_needed)

        for trade in open_trades:
            sym   = trade.get("symbol", "BNBUSDT")
            price = prices.get(sym)
            if not price:
                continue
            await self._manage_position(user_id, trade, price, db)

    async def _manage_position(
        self, user_id: str, trade: dict, price: float, db
    ) -> None:
        entry     = float(trade.get("price", 0))
        qty       = float(trade.get("quantity", 0))
        tp_price  = float(trade.get("take_profit_price",  entry * (1 + TAKE_PROFIT_PCT / 100)))
        sl_price  = float(trade.get("stop_loss_price",    entry * (1 - STOP_LOSS_PCT  / 100)))
        highest   = float(trade.get("highest_price_seen", entry))
        trailing_tp = float(trade.get("trailing_tp_price", tp_price))

        if not entry or not qty:
            return

        pnl_pct = (price - entry) / entry * 100

        # Dynamic trail step: tighter when ADX > 30 (strong trend — lock profits faster)
        adx_at_entry = float(trade.get("signal_adx", 0) or 0)
        trail_step   = 0.3 if adx_at_entry > 30 else TRAIL_STEP_PCT

        # Mettre à jour le plus haut
        updates = {}
        if price > highest:
            updates["highest_price_seen"] = price
            highest = price

        # ── BREAKEVEN : quand +2%, stop passe à l'entrée ─────────────────────
        if pnl_pct >= BREAKEVEN_PCT and sl_price < entry:
            new_sl = entry * 1.001
            updates["stop_loss_price"] = new_sl
            sl_price = new_sl
            logger.info(f"[{user_id}] Breakeven {trade['symbol']}: SL -> ${new_sl:.4f}")

        # ── TRAILING TAKE-PROFIT : le TP suit le prix après TRAIL_TRIGGER_PCT ─
        if pnl_pct >= TRAIL_TRIGGER_PCT:
            new_trailing_tp = price * (1 + trail_step / 100)
            if new_trailing_tp > trailing_tp:
                updates["trailing_tp_price"] = new_trailing_tp
                trailing_tp = new_trailing_tp
                logger.info(
                    f"[{user_id}] Trailing TP {trade['symbol']}: "
                    f"+{pnl_pct:.1f}% -> TP ${new_trailing_tp:.4f} (step={trail_step}%)"
                )

        if updates:
            await db.trades.update_one({"_id": trade["_id"]}, {"$set": updates})

        # ── FERMETURE ─────────────────────────────────────────────────────────

        # Take-profit original atteint → activer le trailing
        if price >= tp_price and pnl_pct < TRAIL_TRIGGER_PCT + 1:
            updates["trailing_tp_active"] = True
            await db.trades.update_one({"_id": trade["_id"]}, {"$set": updates})

        # Trailing TP déclenché (prix redescend sous le trailing TP)
        trailing_active = trade.get("trailing_tp_active", False)
        if trailing_active and price <= trailing_tp * 0.998:  # 0.2% de marge
            await self._close_position(user_id, trade, price, "trailing_tp")
            return

        # Take-profit simple (non trailing)
        if not trailing_active and price >= tp_price:
            await self._close_position(user_id, trade, price, "take_profit")
            return

        # Stop-loss
        if price <= sl_price:
            await self._close_position(user_id, trade, price, "stop_loss")
            return

    # ══════════════════════════════════════════════════════════════════════════
    # EXÉCUTION DES ORDRES
    # ══════════════════════════════════════════════════════════════════════════

    async def _execute_buy(
        self, user_id: str, db,
        symbol: str, position_usdt: float, current_price: float,
        sl_pct: float, tp_pct: float,
        signal_data: Dict, portfolio_data: Dict, config: Dict,
        indicators: Optional[Dict] = None,
    ) -> None:
        """Place un ordre BUY et sauvegarde le trade."""
        try:
            qty = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.calculate_quantity(symbol, position_usdt, current_price)
            )
            order = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.place_market_order(symbol, "BUY", qty)
            )
            fills = order.get("fills", [])
            ex_price = float(fills[0]["price"]) if fills else current_price
            ex_qty   = float(order.get("executedQty", qty))
            gross    = ex_qty * ex_price
            fee      = gross * BINANCE_FEE
            total    = gross + fee
            order_id = str(order.get("orderId", ""))
        except Exception as e:
            logger.error(f"[{user_id}] BUY {symbol} failed: {e}")
            return

        tp_price = ex_price * (1 + tp_pct / 100)
        sl_price = ex_price * (1 - sl_pct / 100)

        trade = TradeInDB(
            user_id=user_id, symbol=symbol, side=TradeSide("BUY"),
            quantity=ex_qty, price=ex_price,
            total_usdt=total, binance_order_id=order_id,
            status=TradeStatus.OPEN,
        )
        doc = trade.to_mongo()
        doc.update({
            "highest_price_seen":  ex_price,
            "take_profit_price":   tp_price,
            "stop_loss_price":     sl_price,
            "trailing_tp_price":   tp_price,
            "trailing_tp_active":  False,
            "signal_confidence":   signal_data["confidence"],
            "signal_source":       signal_data.get("source", "claude"),
            "signal_adx":          (indicators or {}).get("trend", {}).get("adx", 0),
        })

        trade_id = None
        try:
            res = await db.trades.insert_one(doc)
            trade_id = str(res.inserted_id)
        except Exception as e:
            logger.error(f"[{user_id}] Trade save failed: {e}")
            return

        logger.info(
            f"[{user_id}] ✅ BUY {ex_qty} {symbol} @ {ex_price:.4f} "
            f"SL={sl_price:.4f} TP={tp_price:.4f} ({signal_data['confidence']:.0%})"
        )

        await notification_service.send(user_id, "trade_executed", {
            "symbol": symbol, "side": "BUY",
            "quantity": ex_qty, "price": ex_price,
            "tp": tp_price, "sl": sl_price,
        })

        try:
            await self._update_portfolio(db, user_id)
        except Exception:
            pass

        updated = await self._get_portfolio(db, user_id)
        await self._broadcast(user_id, "trade_executed", {
            "id": trade_id, "symbol": symbol, "side": "BUY",
            "quantity": ex_qty, "price": ex_price,
            "total_usdt": total, "take_profit": tp_price, "stop_loss": sl_price,
        })
        await self._broadcast(user_id, "portfolio_update", updated)

    async def _close_position(
        self, user_id: str, trade: dict, close_price: float, reason: str
    ) -> None:
        """Ferme une position avec le vrai solde Binance."""
        db     = get_database()
        symbol = trade.get("symbol", "BNBUSDT")
        entry  = float(trade.get("price", 0))
        cost   = float(trade.get("total_usdt", 0))

        # Solde réel disponible
        try:
            base  = symbol.replace("USDT", "").replace("BUSD", "")
            bals  = await asyncio.get_event_loop().run_in_executor(
                None, binance_service.get_account_balance
            )
            avail = float(bals.get(base, {}).get("free", 0.0))
            if avail <= 0:
                logger.warning(f"[{user_id}] Pas de {base} disponible pour SELL")
                return

            # Précision Binance
            from decimal import Decimal, ROUND_DOWN
            info    = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.get_symbol_info(symbol)
            )
            step    = Decimal(info["step_size"])
            sell_qty= float((Decimal(str(avail)) // step) * step)
            if sell_qty <= 0:
                return
        except Exception as e:
            logger.error(f"[{user_id}] Balance check failed: {e}")
            sell_qty = float(trade.get("quantity", 0))

        try:
            order      = await asyncio.get_event_loop().run_in_executor(
                None, lambda q=sell_qty: binance_service.place_market_order(symbol, "SELL", q)
            )
            fills      = order.get("fills", [])
            sell_price = float(fills[0]["price"]) if fills else close_price
            gross      = sell_qty * sell_price
            fee        = gross * BINANCE_FEE
            net        = gross - fee
            pnl        = net - cost
            pnl_pct    = (pnl / cost * 100) if cost > 0 else 0

            await db.trades.update_one(
                {"_id": trade["_id"]},
                {"$set": {
                    "status": "CLOSED", "pnl": round(pnl, 6),
                    "pnl_pct": round(pnl_pct, 4),
                    "closed_at": datetime.utcnow(),
                    "close_price": sell_price, "close_reason": reason,
                }},
            )

            emoji = "✅" if pnl > 0 else "❌"
            logger.info(
                f"[{user_id}] {emoji} CLOSE {symbol} @ {sell_price:.4f} "
                f"PnL={pnl:+.4f} USDT ({pnl_pct:+.2f}%) [{reason}]"
            )

            await notification_service.send(user_id, "trade_executed", {
                "symbol": symbol, "side": "SELL",
                "price": sell_price, "pnl": pnl, "pnl_pct": pnl_pct, "reason": reason,
            })
            await self._broadcast(user_id, "trade_executed", {
                "symbol": symbol, "side": "SELL",
                "price": sell_price, "pnl": pnl, "pnl_pct": pnl_pct, "reason": reason,
            })

            bot_info = self._running_bots.get(user_id, {})
            bot_info["consecutive_losses"] = (
                bot_info.get("consecutive_losses", 0) + 1 if pnl < 0 else 0
            )

            # Cooldown 45 min sur la paire après un SL
            if reason == "stop_loss":
                if "sl_cooldown" not in bot_info:
                    bot_info["sl_cooldown"] = {}
                bot_info["sl_cooldown"][symbol] = datetime.now(timezone.utc)
                logger.info(f"[{user_id}] Cooldown 45min sur {symbol} apres stop_loss")

            await self._update_portfolio(db, user_id)

        except Exception as e:
            logger.error(f"[{user_id}] Close {symbol} failed: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _fetch_prices(self, symbols: List[str]) -> Dict[str, float]:
        """Fetch prices — WebSocket cache first, REST fallback."""
        prices: Dict[str, float] = {}
        to_fetch: List[str] = []

        for sym in symbols:
            ws_price = websocket_feed.get_cached_price(sym)
            if ws_price and ws_price > 0:
                prices[sym] = ws_price
            else:
                to_fetch.append(sym)

        if to_fetch:
            async def _get(sym):
                key = f"price:{sym}"
                p   = cache.get(key)
                if p:
                    return sym, p
                try:
                    p = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: binance_service.get_current_price(sym)
                    )
                    cache.set(key, p, ttl_seconds=PRICE_TTL)
                    return sym, p
                except Exception:
                    return sym, 0.0

            rest = await asyncio.gather(*[_get(s) for s in to_fetch])
            for sym, p in rest:
                if p > 0:
                    prices[sym] = p

        return prices

    async def _save_signal(
        self, db, user_id: str, symbol: str,
        signal_data: Dict, indicators: Dict, price: float,
    ) -> None:
        sig = SignalInDB(
            symbol=symbol, action=signal_data["action"],
            confidence=signal_data["confidence"],
            reasoning=signal_data.get("reasoning", ""),
            key_factors=signal_data.get("key_factors", []),
            risk_level=signal_data.get("risk_level", "MEDIUM"),
            suggested_stop_loss_pct=signal_data.get("suggested_stop_loss_pct", 2.0),
            suggested_take_profit_pct=signal_data.get("suggested_take_profit_pct", 6.0),
            market_regime=signal_data.get("market_regime", "RANGING"),
            timeframe_alignment=signal_data.get("timeframe_alignment", "MODERATE"),
            entry_quality=signal_data.get("entry_quality", 0.5),
            indicators={
                "rsi":       indicators.get("momentum", {}).get("rsi"),
                "macd_h":    indicators.get("trend", {}).get("macd_histogram"),
                "adx":       indicators.get("trend", {}).get("adx"),
                "bb_pct":    indicators.get("volatility", {}).get("bb_pct"),
                "vol_ratio": indicators.get("volume", {}).get("volume_ratio"),
                "patterns":  indicators.get("patterns", {}),
                "source":    signal_data.get("source", "claude"),
                "timeframe_confluence": signal_data.get("timeframe_confluence", {}),
            },
            price_at_signal=price,
        )
        try:
            await db.signals.insert_one(sig.to_mongo())
        except Exception as e:
            logger.debug(f"Signal save error: {e}")

    async def _get_portfolio(self, db, user_id: str) -> Dict[str, Any]:
        """
        Lit TOUJOURS le solde réel depuis Binance pour éviter les snapshots périmés.
        Les métriques agrégées (PnL total, win rate) viennent de MongoDB.
        """
        # Solde réel Binance — source de vérité pour le capital disponible
        usdt_free = 0.0
        try:
            bals      = await asyncio.get_event_loop().run_in_executor(
                None, binance_service.get_account_balance
            )
            usdt_free = float(bals.get("USDT", {}).get("free", 0.0))
        except Exception as e:
            logger.warning(f"[{user_id}] Binance balance fetch failed: {e}")

        # Valeur des positions ouvertes
        open_trades  = await db.trades.find(
            {"user_id": user_id, "status": "OPEN"}
        ).to_list(20)
        invested     = sum(t.get("total_usdt", 0.0) for t in open_trades)
        total_usdt   = usdt_free + invested

        # Métriques agrégées depuis MongoDB (win rate, total PnL)
        latest = await db.portfolio_history.find_one(
            {"user_id": user_id}, sort=[("recorded_at", -1)]
        )
        total_pnl     = latest.get("total_pnl",     0.0) if latest else 0.0
        total_pnl_pct = latest.get("total_pnl_pct", 0.0) if latest else 0.0
        win_rate      = latest.get("win_rate",       0.0) if latest else 0.0

        logger.info(
            f"[{user_id}] Portfolio: dispo={usdt_free:.2f} investi={invested:.2f} total={total_usdt:.2f} USDT"
        )

        return {
            "available_usdt": usdt_free,
            "total_usdt":     total_usdt,
            "invested_usdt":  invested,
            "total_pnl":      total_pnl,
            "total_pnl_pct":  total_pnl_pct,
            "win_rate":       win_rate,
        }

    async def _update_portfolio(self, db, user_id: str) -> None:
        try:
            all_trades  = await db.trades.find({"user_id": user_id}).to_list(5000)
            closed      = [t for t in all_trades if t.get("status") == "CLOSED"]
            open_trades = [t for t in all_trades if t.get("status") == "OPEN"]

            total_pnl = sum(t.get("pnl", 0.0) or 0.0 for t in closed)
            winning   = sum(1 for t in closed if (t.get("pnl") or 0.0) > 0)
            win_rate  = (winning / len(closed) * 100) if closed else 0.0
            invested  = sum(t.get("total_usdt", 0.0) for t in open_trades)

            try:
                bals      = await asyncio.get_event_loop().run_in_executor(
                    None, binance_service.get_account_balance
                )
                usdt_free = float(bals.get("USDT", {}).get("free", 0.0))
            except Exception:
                usdt_free = 0.0

            total_usdt    = usdt_free + invested
            first_snap    = await db.portfolio_history.find_one(
                {"user_id": user_id}, sort=[("recorded_at", 1)]
            )
            initial       = first_snap.get("total_usdt", total_usdt) if first_snap else total_usdt
            if initial <= 0: initial = total_usdt
            total_pnl_pct = ((total_usdt - initial) / initial * 100) if initial > 0 else 0.0

            snap = PortfolioSnapshot(
                user_id=user_id, total_usdt=total_usdt, available_usdt=usdt_free,
                invested_usdt=invested, total_pnl=total_pnl, total_pnl_pct=total_pnl_pct,
                win_rate=win_rate, total_trades=len(all_trades), winning_trades=winning,
            )
            await db.portfolio_history.insert_one(snap.to_mongo())
        except Exception as e:
            logger.error(f"Portfolio update error {user_id}: {e}")

    @staticmethod
    def _get_max_positions(available_usdt: float) -> int:
        """Nombre max de positions simultanées selon le capital disponible."""
        if available_usdt < 15.0:  return 1
        if available_usdt < 25.0:  return 2
        if available_usdt < 50.0:  return 3
        return 4

    async def _broadcast(self, user_id: str, message_type: str, data: Dict[str, Any]) -> None:
        from routers.websocket import send_update
        try:
            await send_update(user_id, message_type, data)
        except Exception:
            pass


bot_engine = BotEngine()
