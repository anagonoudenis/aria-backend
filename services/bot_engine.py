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

logger = get_logger(__name__)

INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
BINANCE_FEE      = 0.001

# 20 paires liquides — couvre bull ET bear market
SCAN_PAIRS = [
    "BNBUSDT",  "SOLUSDT",  "XRPUSDT",  "DOGEUSDT", "ADAUSDT",
    "DOTUSDT",  "LTCUSDT",  "TRXUSDT",  "LINKUSDT", "AVAXUSDT",
    "ETHUSDT",  "SHIBUSDT", "UNIUSDT",  "ATOMUSDT", "NEARUSDT",
    "APTUSDT",  "ARBUSDT",  "OPUSDT",   "INJUSDT",  "SUIUSDT",
]
# Paires nécessitant min notional $10 — exclues si capital < $15
HIGH_NOTIONAL_PAIRS = {"BTCUSDT", "ETHUSDT"}

# TP/SL — ratio 3:1
TRAIL_TRIGGER_PCT  = 0.8   # trailing SL activé dès +0.8% profit
TRAIL_STEP_PCT     = 0.35  # SL trail = highest × (1 - 0.35%)
BREAKEVEN_PCT      = 0.5
STOP_LOSS_PCT      = 0.9   # SL défaut légèrement plus large (évite faux triggers)
TAKE_PROFIT_PCT    = 2.5   # TP défaut — ratio 2.8:1


# ── Filtres BULL market (BTC > EMA21) ────────────────────────────────────────
MIN_CONFIDENCE_BULL      = 0.65   # 65% — aligné sur risk_manager
MIN_COMPOSITE_SCORE_BULL = 3.5    # score×conf ≥ 3.5 (était 2.5)
MIN_SCORE_RAW_BULL       = 5      # 5/15 minimum (était 4)
MIN_ADX_BULL             = 20     # ADX ≥ 20 = tendance réelle (était 8)
MIN_VOLUME_RATIO_BULL    = 1.1    # volume 1.1x la moyenne

# ── Filtres BEAR market (BTC < EMA21) — rebonds oversold uniquement ──────────
MIN_CONFIDENCE_BEAR      = 0.65   # 65% en bear aussi
MIN_COMPOSITE_SCORE_BEAR = 4.0    # seuil plus strict en bear (était 3.5)
MIN_SCORE_RAW_BEAR       = 5      # 5/15 minimum
MIN_ADX_BEAR             = 15     # ADX ≥ 15 en bear
MIN_VOLUME_RATIO_BEAR    = 1.1    # volume 1.1x
RSI_OVERSOLD_BEAR        = 45     # RSI < 45 — vrai oversold (était 55)

# Aliases (compatibilité)
MIN_CONFIDENCE      = MIN_CONFIDENCE_BULL
MIN_COMPOSITE_SCORE = MIN_COMPOSITE_SCORE_BULL
MIN_SCORE_RAW       = MIN_SCORE_RAW_BULL
MIN_ADX             = MIN_ADX_BULL
MIN_VOLUME_RATIO    = MIN_VOLUME_RATIO_BULL
MAX_CONSECUTIVE_LOSSES   = 5        # pause après 5 pertes (était 6)
SL_COOLDOWN_SECONDS      = 30 * 60  # 30 min cooldown par paire après SL
MIN_TRADE_INTERVAL_SECS  = 25 * 60  # 25 min minimum entre deux trades
DAILY_MAX_LOSS_PCT        = 3.0     # stoppe si perte journalière > 3% (était 4%)

_active_cycles: set = set()

# Cache paires à fort momentum (rafraîchi toutes les 15 min)
_hot_pairs_cache: Dict[str, Any] = {"pairs": [], "last_update": None}


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
            "sl_cooldown": {},          # {symbol: datetime} — paires en cooldown après SL
            "pending_entry": None,     # signal en attente d'un pullback vers EMA9
            "daily_start_capital": 0.0,  # capital en début de journée UTC
            "last_day_reset": None,    # date du dernier reset journalier
        }

        db = get_database()
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"bot_config.is_running": True}})
        logger.info(f"Bot started: user={user_id} interval={interval} scanning={SCAN_PAIRS}")
        asyncio.create_task(self._bot_loop(user_id, interval_seconds))

    async def _bot_loop(self, user_id: str, interval_seconds: int) -> None:
        logger.info(f"[{user_id}] Loop started ({interval_seconds}s)")
        # Monitoring positions indépendant — vérifie SL/TP toutes les 30s
        asyncio.create_task(self._position_monitor_loop(user_id))
        await self._run_cycle_safe(user_id)
        while self.is_running(user_id):
            await asyncio.sleep(interval_seconds)
            if self.is_running(user_id):
                await self._run_cycle_safe(user_id)
        logger.info(f"[{user_id}] Loop stopped")

    async def _position_monitor_loop(self, user_id: str) -> None:
        """Surveille les positions ouvertes toutes les 30s — SL/TP en temps quasi-réel."""
        logger.info(f"[{user_id}] Position monitor started (30s)")
        while self.is_running(user_id):
            await asyncio.sleep(30)
            if not self.is_running(user_id):
                break
            try:
                db = get_database()
                open_trades = await db.trades.find(
                    {"user_id": user_id, "status": "OPEN"}
                ).to_list(10)
                if not open_trades:
                    continue
                syms   = list({t.get("symbol") for t in open_trades})
                prices = await self._fetch_prices(syms)
                for trade in open_trades:
                    sym   = trade.get("symbol", "")
                    price = prices.get(sym)
                    if price:
                        await self._manage_position(user_id, trade, price, db)
            except Exception as e:
                logger.debug(f"[{user_id}] Position monitor error: {e}")
        logger.info(f"[{user_id}] Position monitor stopped")

    async def stop(self, user_id: str, reason: str = "Manuel") -> None:
        if user_id not in self._running_bots:
            return
        del self._running_bots[user_id]
        db = get_database()
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"bot_config.is_running": False}})
        logger.info(f"Bot stopped: {user_id} ({reason})")

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
            since = bot_info.get("circuit_breaker_since")
            if not since or (datetime.now(timezone.utc) - since).total_seconds() >= 4 * 3600:
                bot_info["circuit_breaker_active"] = False
                bot_info["circuit_breaker_reason"]  = None
                bot_info["consecutive_losses"]      = 0
                logger.info(f"[{user_id}] Circuit breaker reset automatique (4h ecoulees)")
            else:
                logger.warning(f"[{user_id}] Circuit breaker actif — cycle ignore")
                return

        # ── 0. Filtre horaire — zone morte crypto (3h-6h UTC, liquidité très faible) ──
        utc_hour = datetime.now(timezone.utc).hour
        if 3 <= utc_hour < 6:
            logger.info(f"[{user_id}] Zone morte ({utc_hour}h UTC) — cycle skip")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 1. Gérer les positions ouvertes AVANT d'ouvrir de nouvelles ─────
        all_scan = list(set(SCAN_PAIRS) | {"BTCUSDT"})
        await self._manage_all_positions(user_id, all_scan)

        # ── 2. Portefeuille — source de vérité ───────────────────────────────
        db = get_database()
        portfolio_data = await self._get_portfolio(db, user_id)
        available_usdt = portfolio_data["available_usdt"]

        logger.info(f"[{user_id}] Cycle start — capital={available_usdt:.2f} USDT")

        # ── 2b. Reset journalier + circuit breaker perte journalière ─────────
        today = datetime.now(timezone.utc).date()
        if bot_info.get("last_day_reset") != today:
            bot_info["daily_start_capital"] = available_usdt
            bot_info["last_day_reset"]       = today
            logger.info(f"[{user_id}] Nouveau jour — capital de reference: {available_usdt:.2f} USDT")

        daily_start = bot_info.get("daily_start_capital", available_usdt)
        if daily_start > 0:
            daily_loss_pct = (daily_start - available_usdt) / daily_start * 100
            if daily_loss_pct >= DAILY_MAX_LOSS_PCT:
                logger.warning(
                    f"[{user_id}] Perte journaliere {daily_loss_pct:.1f}% "
                    f">= {DAILY_MAX_LOSS_PCT}% — pause trading aujourd'hui"
                )
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return

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

        # ── 4b. Cooldown minimum entre deux trades ────────────────────────────
        last_trade_at = bot_info.get("last_trade_at")
        if last_trade_at:
            elapsed = (datetime.now(timezone.utc) - last_trade_at).total_seconds()
            if elapsed < MIN_TRADE_INTERVAL_SECS:
                remaining = int(MIN_TRADE_INTERVAL_SECS - elapsed)
                logger.info(f"[{user_id}] Cooldown inter-trades: {remaining}s restants — attente")
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return

        if consecutive >= MAX_CONSECUTIVE_LOSSES:
            reason = f"Circuit breaker: {consecutive} pertes consécutives"
            bot_info["circuit_breaker_active"] = True
            bot_info["circuit_breaker_reason"]  = reason
            bot_info["circuit_breaker_since"]   = datetime.now(timezone.utc)
            await notification_service.send(user_id, "circuit_breaker", {"reason": reason})
            logger.warning(f"[{user_id}] {reason}")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 4b. Mode marché BTC — adapte la stratégie (ne bloque plus tout) ───
        market_mode, btc_reason = await self._get_market_mode()
        bot_info["market_mode"] = market_mode
        logger.info(f"[{user_id}] Mode marche: {market_mode} — {btc_reason}")

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
            user_id, SCAN_PAIRS, open_symbols, prices, config, portfolio_data,
            market_mode=market_mode,
        )

        if best is None:
            logger.info(f"[{user_id}] Aucune opportunite ce cycle (mode={market_mode})")
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
            f"score={best['composite_score']:.1f} mode={market_mode}"
        )

        # ── 7. Sauvegarder le signal ──────────────────────────────────────────
        await self._save_signal(db, user_id, symbol, signal_data, indicators, current_price)
        await self._broadcast(user_id, "signal", {
            **signal_data, "symbol": symbol, "price": current_price
        })

        # ── 8. Filtres qualité adaptés au mode marché ─────────────────────────
        if signal_data["action"] != "BUY":
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # Seuils selon mode marché
        is_btd = best.get("buy_the_dip", False)   # signal buy-the-dip ?

        if market_mode == "BULL":
            min_conf  = MIN_CONFIDENCE_BULL
            min_score = MIN_COMPOSITE_SCORE_BULL
        elif market_mode == "BEAR":
            min_conf  = MIN_CONFIDENCE_BEAR
            min_score = MIN_COMPOSITE_SCORE_BEAR
            # RSI check seulement pour les signaux BUY classiques (pas buy_the_dip)
            if not is_btd:
                rsi_chk = indicators.get("momentum", {}).get("rsi", 50)
                if rsi_chk > RSI_OVERSOLD_BEAR:
                    logger.info(f"[{user_id}] BEAR RSI {rsi_chk:.1f} > {RSI_OVERSOLD_BEAR} — refuse (oversold requis)")
                    bot_info["cycles_count"] += 1
                    bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                    return
        else:  # NEUTRAL
            min_conf  = MIN_CONFIDENCE_BEAR   # utiliser seuils BEAR (conservative)
            min_score = MIN_COMPOSITE_SCORE_BEAR

        if signal_data["confidence"] < min_conf:
            logger.info(
                f"[{user_id}] Confiance {signal_data['confidence']:.0%} "
                f"< {min_conf:.0%} ({market_mode}) — signal ignore"
            )
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        if best["composite_score"] < min_score:
            logger.info(
                f"[{user_id}] Score {best['composite_score']:.1f} "
                f"< {min_score} ({market_mode}) — signal ignore"
            )
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 8b. Filtres régime — assouplis pour buy_the_dip ──────────────────
        market_regime = signal_data.get("market_regime", "RANGING")
        tf_alignment  = signal_data.get("timeframe_alignment", "MODERATE")

        # TRENDING_DOWN bloqué seulement si pas un buy_the_dip
        # (buy_the_dip entre précisément dans une tendance baissière pour un rebond)
        if market_regime == "TRENDING_DOWN" and not is_btd:
            logger.info(f"[{user_id}] TRENDING_DOWN sans BTD — trade refuse")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # WEAK alignment accepté pour buy_the_dip (par définition le 5m est baissier)
        if tf_alignment == "WEAK" and not is_btd and market_mode == "BEAR":
            logger.info(f"[{user_id}] WEAK alignment en BEAR sans BTD — trade refuse")
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

        # ── 10. SL/TP — ratio 3:1 garanti ────────────────────────────────────
        conf = signal_data["confidence"]
        atr = indicators.get("volatility", {}).get("atr", 0)
        if atr > 0 and current_price > 0:
            raw_sl = (1.0 * atr / current_price) * 100
            raw_tp = (3.0 * atr / current_price) * 100
            # SL [0.7%-1.5%] — plus large pour absorber le bruit 5m, TP [2.0%-5.0%]
            sl_pct = round(max(min(raw_sl, 1.5), 0.7), 3)
            tp_pct = round(max(min(raw_tp, 5.0), max(2.0, sl_pct * 2.8)), 3)
            logger.info(
                f"[{user_id}] ATR SL={sl_pct}% TP={tp_pct}% ratio={tp_pct/sl_pct:.1f}:1 conf={conf:.0%}"
            )
        else:
            sl_pct = STOP_LOSS_PCT    # 0.9%
            tp_pct = TAKE_PROFIT_PCT  # 2.5%
            logger.info(f"[{user_id}] Default SL={sl_pct}% TP={tp_pct}%")

        # Vérification R:R minimal (sécurité absolue)
        if tp_pct / sl_pct < 2.2:
            tp_pct = round(sl_pct * 2.5, 3)
            logger.info(f"[{user_id}] R:R ajusté → SL={sl_pct}% TP={tp_pct}% (2.5:1 garanti)")

        # TP étendu pour setups de très haute qualité (BB Squeeze + ADX fort + triple bull)
        _bb_sq = indicators.get("volatility", {}).get("bb_squeeze", False)
        _bb_ex = indicators.get("volatility", {}).get("bb_expanding", False)
        _adx_v = indicators.get("trend", {}).get("adx", 0)
        _adx_r = indicators.get("trend", {}).get("adx_rising", False)
        _triple = best.get("triple_bull", False) if best else False
        if _bb_sq and _bb_ex and _adx_v > 25 and _triple:
            extended_tp = round(sl_pct * 3.5, 3)
            if extended_tp > tp_pct:
                logger.info(
                    f"[{user_id}] TP étendu setup premium "
                    f"BB-Squeeze+ADX{_adx_v:.0f}+TripleBull: {tp_pct}% → {extended_tp}%"
                )
                tp_pct = extended_tp

        # ── 11. Kelly position sizing avec réduction après pertes ────────────
        base_usdt    = (available_usdt / max_positions) * 0.90
        win_rate_pct = portfolio_data.get("win_rate", 0)
        kelly_mult   = 1.0
        if win_rate_pct > 0:
            win_r    = win_rate_pct / 100
            rr_ratio = tp_pct / sl_pct if sl_pct > 0 else 2.8
            kelly    = win_r - (1 - win_r) / rr_ratio
            kelly_mult = 1.0 + max(min(kelly * 0.5, 0.4), -0.4)
            position_usdt = base_usdt * kelly_mult
        else:
            position_usdt = base_usdt
        # Réduire la taille après des pertes consécutives (protection capital)
        if consecutive >= 3:
            loss_factor = 0.5  # 50% de la taille normale après 3+ pertes
            position_usdt = position_usdt * loss_factor
            logger.info(f"[{user_id}] Taille réduite ×0.5 ({consecutive} pertes consécutives)")
        elif consecutive >= 2:
            position_usdt = position_usdt * 0.7  # 70% après 2 pertes
        position_usdt = max(position_usdt, 5.50)
        position_usdt = min(position_usdt, available_usdt * 0.90)

        # Multiplicateur qualité setup : BB Squeeze ou ADX fort → position plus grande
        bb_squeeze_now  = indicators.get("volatility", {}).get("bb_squeeze", False)
        bb_expanding_now = indicators.get("volatility", {}).get("bb_expanding", False)
        adx_now_val     = indicators.get("trend", {}).get("adx", 0)
        adx_rising_now  = indicators.get("trend", {}).get("adx_rising", False)
        conf_now        = signal_data.get("confidence", 0)

        quality_label = ""
        if bb_squeeze_now and bb_expanding_now:
            position_usdt = min(position_usdt * 1.25, available_usdt * 0.92)
            quality_label = " [BB-SQUEEZE ×1.25]"
        elif adx_now_val > 30 and adx_rising_now:
            position_usdt = min(position_usdt * 1.15, available_usdt * 0.92)
            quality_label = " [ADX-FORT ×1.15]"
        elif conf_now >= 0.82:
            position_usdt = min(position_usdt * 1.10, available_usdt * 0.92)
            quality_label = " [CONF-HAUTE ×1.10]"

        logger.info(
            f"[{user_id}] Position: {position_usdt:.2f} USDT "
            f"(WR={win_rate_pct:.0f}% kelly_mult={kelly_mult:.2f} consec_losses={consecutive}){quality_label}"
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
            bot_info["last_trade_at"] = datetime.now(timezone.utc)

        bot_info["cycles_count"] += 1
        bot_info["last_cycle_at"] = datetime.now(timezone.utc)

    # ══════════════════════════════════════════════════════════════════════════
    # FILTRE BTC MACRO
    # ══════════════════════════════════════════════════════════════════════════

    async def _get_market_mode(self) -> Tuple[str, str]:
        """
        Détermine le mode de marché actuel : BULL / BEAR / NEUTRAL.
        Ne bloque JAMAIS les trades — adapte seulement les filtres.
        BULL   → seuils assouplis, toutes paires
        NEUTRAL→ seuils standard
        BEAR   → seuils plus stricts, cherche uniquement les oversold extrêmes
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
                ind = analysis_service.compute_indicators(df, symbol="BTCUSDT")
                cache.set(key_ind, ind, ttl_seconds=INDICATOR_TTL.get("15m", 900))

            trend     = ind.get("trend", {})
            candles   = ind.get("candles_summary", [])
            btc_close = candles[-1]["close"] if candles else 0
            ema21     = trend.get("ema_21", 0)
            sma50     = trend.get("sma_50", 0)

            if btc_close <= 0 or ema21 <= 0:
                return "NEUTRAL", "donnees BTC indisponibles"

            if btc_close > ema21 and (sma50 == 0 or ema21 > sma50):
                return "BULL", f"BTC {btc_close:.0f} > EMA21 {ema21:.0f}"
            elif btc_close > ema21:
                return "NEUTRAL", f"BTC haussier CT mais structure mixte"
            else:
                return "BEAR", f"BTC {btc_close:.0f} < EMA21 {ema21:.0f} — rebonds oversold uniquement"

        except Exception as e:
            logger.warning(f"Market mode check failed: {e}")
            return "NEUTRAL", "erreur — mode NEUTRAL par defaut"

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

        # Vérifier si max positions déjà atteint
        open_trades_now = await db.trades.find(
            {"user_id": user_id, "status": "OPEN"}
        ).to_list(20)
        max_pos = self._get_max_positions(portfolio_data.get("available_usdt", 0))
        if len(open_trades_now) >= max_pos:
            logger.info(f"[{user_id}] Pending entry {symbol} annulé — max positions atteint ({len(open_trades_now)}/{max_pos})")
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
            bot_info["last_trade_at"] = datetime.now(timezone.utc)
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

    async def _get_hot_pairs(self) -> List[str]:
        """Retourne les paires avec fort momentum du moment (rafraîchi toutes les 15 min)."""
        now = datetime.now(timezone.utc)
        cache = _hot_pairs_cache
        last = cache.get("last_update")
        if last is None or (now - last).total_seconds() > 900:
            try:
                all_pairs = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_top_pairs(min_volume_usdt=10_000_000)
                )
                # Paires à forte variation + volume élevé + pas déjà dans SCAN_PAIRS
                scan_set = set(SCAN_PAIRS)
                hot = [
                    p["symbol"] for p in all_pairs[:100]
                    if p["symbol"] not in scan_set
                    and abs(p.get("change_pct", 0)) > 2.5
                    and p.get("volume_usdt", 0) > 20_000_000
                    and p["symbol"].endswith("USDT")
                ][:6]
                cache["pairs"] = hot
                cache["last_update"] = now
                if hot:
                    logger.info(f"Hot pairs 15m: {hot}")
            except Exception as e:
                logger.debug(f"Hot pairs fetch error: {e}")
        return cache.get("pairs", [])

    async def _scan_best_opportunity(
        self, user_id: str,
        symbols: List[str],
        open_symbols: set,
        prices: Dict[str, float],
        config: Dict[str, Any],
        portfolio_data: Optional[Dict[str, Any]] = None,
        market_mode: str = "NEUTRAL",
    ) -> Optional[Dict[str, Any]]:
        """
        Analyse toutes les paires en parallèle et retourne la meilleure opportunité.
        Filtre : HIGH_NOTIONAL si capital < $15, cooldown SL, tendance EMA, confluence MTF.
        """
        available = (portfolio_data or {}).get("available_usdt", 0.0)

        # Ajouter les paires à fort momentum du moment
        hot_pairs = await self._get_hot_pairs()
        effective_symbols = list(dict.fromkeys(list(symbols) + hot_pairs))  # déduplique, ordre stable

        # Cooldown : paires ayant récemment déclenché un SL
        now = datetime.now(timezone.utc)
        sl_cooldown = self._running_bots.get(user_id, {}).get("sl_cooldown", {})
        candidates = [
            s for s in effective_symbols
            if s not in open_symbols
            and (s not in HIGH_NOTIONAL_PAIRS or available >= 15.0)
            and (
                s not in sl_cooldown
                or (now - sl_cooldown[s]).total_seconds() > SL_COOLDOWN_SECONDS
            )
        ]
        if not candidates:
            return None

        # Phase 1 : analyse triple timeframe 5m + 15m + 1h en parallèle
        tasks = [self._analyze_pair_fast(s) for s in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scored = []
        for sym, result in zip(candidates, results):
            if isinstance(result, Exception) or result is None:
                continue

            # Unpack 5-tuple (rétrocompatible si 4h/1h manquent)
            if len(result) == 5:
                indicators_5m, rule_sig_5m, rule_sig_15m, rule_sig_1h, rule_sig_4h = result
            elif len(result) == 4:
                indicators_5m, rule_sig_5m, rule_sig_15m, rule_sig_1h = result
                rule_sig_4h = {}
            else:
                indicators_5m, rule_sig_5m, rule_sig_15m = result
                rule_sig_1h = {}
                rule_sig_4h = {}

            action_5m  = rule_sig_5m.get("action", "HOLD")
            action_15m = rule_sig_15m.get("action", "HOLD")
            action_1h  = rule_sig_1h.get("action", "HOLD")
            action_4h  = rule_sig_4h.get("action", "HOLD")
            score_5m   = rule_sig_5m.get("score", 0)
            score_15m  = rule_sig_15m.get("score", 0)

            trend_5m   = indicators_5m.get("trend", {})
            momentum   = indicators_5m.get("momentum", {})
            volume_ind = indicators_5m.get("volume", {})
            candles    = indicators_5m.get("candles_summary", [])
            rsi        = momentum.get("rsi", 50)
            adx        = trend_5m.get("adx", 0)
            vol_ratio  = volume_ind.get("vol_ratio", 1.0)
            macd_h     = trend_5m.get("macd_histogram", 0)

            # ── Déterminer le signal effectif ────────────────────────────
            effective_action = action_5m
            effective_score  = score_5m

            # RSI oversold extrême → rebond forcé (tous modes)
            if rsi < 28:
                effective_action = "BUY"
                effective_score  = max(score_5m, 6)
                logger.info(f"[{user_id}] {sym}: OVERSOLD EXTREME RSI={rsi:.0f} → BUY force")

            elif action_5m in ("SELL", "HOLD"):
                # Buy-the-dip : 5m baissier mais TFs supérieurs haussiers
                # BULL : exige DEUX TFs haussiers (15m ET 1h) + RSI < 50
                # BEAR/NEUTRAL : un seul TF suffit + RSI très oversold
                if market_mode == "BULL":
                    btd = (action_15m == "BUY" and action_1h == "BUY" and rsi < 50)
                elif market_mode == "BEAR":
                    btd = ((action_15m == "BUY" or action_1h == "BUY") and rsi < 40)
                else:  # NEUTRAL
                    btd = ((action_15m == "BUY" or action_1h == "BUY") and rsi < 50)
                if btd:
                    effective_action = "BUY"
                    effective_score  = max(score_5m, score_15m)
                    logger.info(f"[{user_id}] {sym}: BUY-THE-DIP({market_mode}) 5m={action_5m} 15m={action_15m} 1h={action_1h} RSI={rsi:.0f}")

            # ── Ignorer si pas BUY et pas de position à fermer ───────────
            if effective_action != "BUY" and sym not in open_symbols:
                logger.debug(f"{sym}: {action_5m} sans position — ignore")
                continue

            # ── Score minimum ─────────────────────────────────────────────
            min_raw = MIN_SCORE_RAW_BEAR if market_mode == "BEAR" else MIN_SCORE_RAW_BULL
            if effective_score < min_raw:
                logger.info(f"[{user_id}] {sym}: score {effective_score} < {min_raw} ({market_mode}) — skip")
                continue

            # ── Filtres qualité pour BUY ──────────────────────────────────
            if effective_action == "BUY":
                # ADX minimum
                adx_min = MIN_ADX_BEAR if market_mode == "BEAR" else MIN_ADX_BULL
                if adx < adx_min:
                    logger.info(f"[{user_id}] {sym}: ADX={adx:.1f} < {adx_min} ({market_mode}) — skip")
                    continue

                # Volume minimum — spike réel requis pour confirmer le mouvement
                vol_min = 1.5 if market_mode == "BULL" else 1.2
                if vol_ratio < vol_min:
                    logger.info(f"[{user_id}] {sym}: vol={vol_ratio:.1f}x < {vol_min}x ({market_mode}) — skip")
                    continue

                # RSI — zone selon mode
                if market_mode == "BEAR":
                    if rsi > 58:
                        logger.debug(f"{sym}: BEAR RSI={rsi:.1f} > 58 — skip")
                        continue
                elif market_mode == "BULL":
                    if not (28 <= rsi <= 72):
                        logger.debug(f"{sym}: BULL RSI={rsi:.1f} hors 28-72 — skip")
                        continue
                else:
                    if not (25 <= rsi <= 75):
                        logger.info(f"[{user_id}] {sym}: NEUTRAL RSI={rsi:.1f} hors 25-75 — skip")
                        continue

                # MACD requis seulement en BULL
                if market_mode == "BULL" and macd_h <= 0:
                    logger.debug(f"{sym}: BULL MACD_h={macd_h:.6f} <= 0 — skip")
                    continue

                # Anti-chasing — désactivé pour buy_the_dip (on veut les rebonds)
                # Actif uniquement pour les signaux BUY classiques (5m=BUY)
                if effective_action == "BUY" and action_5m == "BUY":
                    if len(candles) >= 2:
                        last_c = candles[-1]
                        prev_c = candles[-2]
                        if prev_c.get("close", 0) > 0:
                            last_move = (last_c.get("close",0) - prev_c.get("close",0)) / prev_c.get("close",0) * 100
                            if last_move > 2.0:
                                logger.info(f"[{user_id}] {sym}: chasing +{last_move:.2f}% — skip")
                                continue

                # EMA requis seulement en BULL
                if market_mode == "BULL":
                    ema9  = trend_5m.get("ema_9", 0)
                    ema21 = trend_5m.get("ema_21", 0)
                    if ema9 > 0 and ema21 > 0 and ema9 < ema21:
                        logger.debug(f"{sym}: BULL EMA9<EMA21 — skip")
                        continue

                # Bloquer si TOUS les TF sont baissiers (triple SELL)
                if action_5m == "SELL" and action_15m == "SELL" and action_1h == "SELL":
                    logger.debug(f"{sym}: triple SELL — skip")
                    continue

                # 15m confirmation bloquante — si 15m dit SELL, on n'achète pas sur 5m
                # La 15m est 3x plus fiable que la 5m pour la direction
                if action_5m == "BUY" and action_15m == "SELL":
                    logger.info(f"[{user_id}] {sym}: 5m BUY mais 15m SELL — contre-tendance, skip")
                    continue

                # Anti-pump : éviter les entrées tardives (prix +7% sur 1h)
                if len(candles) >= 12:
                    price_1h_ago = candles[-12].get("close", 0)
                    if price_1h_ago > 0:
                        move_1h = (candles[-1]["close"] - price_1h_ago) / price_1h_ago * 100
                        if move_1h > 7.0:
                            logger.info(f"[{user_id}] {sym}: pump +{move_1h:.1f}% (1h) — entrée tardive, skip")
                            continue
                        if move_1h < -8.0:
                            logger.info(f"[{user_id}] {sym}: dump {move_1h:.1f}% (1h) — skip BUY")
                            continue

                # Confirmation bougie fermée au-dessus EMA9 — BUY classiques uniquement
                # (Ne s'applique PAS aux buy_the_dip : par définition le prix est sous EMA9)
                if action_5m == "BUY":
                    ema9_chk  = trend_5m.get("ema_9", 0)
                    close_chk = candles[-1]["close"] if candles else 0
                    if ema9_chk > 0 and close_chk > 0 and close_chk < ema9_chk * 0.998:
                        logger.info(f"[{user_id}] {sym}: close {close_chk:.4f} < EMA9 {ema9_chk:.4f} — confirmation échouée, skip")
                        continue

            # ── Confluence multiplier ─────────────────────────────────────
            triple = (action_15m == "BUY" and action_1h == "BUY")
            double = (action_15m == "BUY" or action_1h  == "BUY")
            if triple and action_5m == "BUY":
                confluence_mult = 1.5
            elif triple:
                confluence_mult = 1.3   # buy-the-dip avec 15m+1h BUY
            elif double:
                confluence_mult = 1.1
            else:
                confluence_mult = 1.0

            # 4h macro trend boost / malus
            if action_4h == "BUY":
                confluence_mult = round(confluence_mult * 1.15, 3)
                logger.debug(f"{sym}: 4h BUY — confluence ×1.15")
            elif action_4h == "SELL" and rsi > 38:
                confluence_mult = round(confluence_mult * 0.85, 3)
                logger.debug(f"{sym}: 4h SELL — confluence ×0.85 (RSI={rsi:.0f})")

            score = round(effective_score * confluence_mult, 1)

            scored.append({
                "symbol":         sym,
                "score":          score,
                "action":         effective_action,
                "indicators":     indicators_5m,
                "rule_sig":       rule_sig_5m,
                "confluence_15m": action_15m,
                "confluence_1h":  action_1h,
                "confluence_4h":  action_4h,
                "triple_bull":    (action_5m == "BUY" and triple),
                "buy_the_dip":    (effective_action == "BUY" and action_5m != "BUY"),
            })

        if not scored:
            return None

        # Phase 2 : meilleur candidat → validation Claude
        scored.sort(key=lambda x: x["score"], reverse=True)
        best_candidate = scored[0]
        sym        = best_candidate["symbol"]
        indicators = best_candidate["indicators"]
        rule_sig   = best_candidate["rule_sig"]
        price      = prices.get(sym, 0)
        pf         = portfolio_data or {}

        tag = "[BUY-THE-DIP]" if best_candidate.get("buy_the_dip") else ("[TRIPLE BULL]" if best_candidate.get("triple_bull") else "")
        logger.info(
            f"[{user_id}] Best: {sym} score={best_candidate['score']:.1f} "
            f"5m={best_candidate['action']} 15m={best_candidate['confluence_15m']} "
            f"1h={best_candidate.get('confluence_1h','?')} 4h={best_candidate.get('confluence_4h','?')} {tag}"
        )

        try:
            signal_data = await claude_service.analyze_market(sym, indicators, price, pf)
        except Exception as e:
            logger.warning(f"Claude failed for {sym}: {e} — using rule-based")
            signal_data = claude_service._from_rule(rule_sig, indicators)

        # Override Claude uniquement si buy_the_dip ET 1h confirme BUY (signal fort)
        if best_candidate.get("buy_the_dip") and signal_data.get("action") not in ("BUY",):
            rule_score    = best_candidate["rule_sig"].get("score", 0)
            confirm_1h    = best_candidate.get("confluence_1h") == "BUY"
            # Exiger que la 1h soit BUY ET score suffisant
            if confirm_1h and rule_score >= MIN_SCORE_RAW_BEAR:
                signal_data["action"]     = "BUY"
                signal_data["confidence"] = max(signal_data.get("confidence", 0.65), 0.66)
                logger.info(f"[{user_id}] Buy-the-dip override (1h BUY): {sym} → BUY {signal_data['confidence']:.0%}")
            else:
                logger.info(f"[{user_id}] Buy-the-dip override annulé: 1h={best_candidate.get('confluence_1h')} score={rule_score}")

        bonus = 1.5 if best_candidate.get("triple_bull") else (1.2 if best_candidate.get("buy_the_dip") else 1.0)
        composite = round(best_candidate["score"] * signal_data["confidence"] * bonus, 2)

        logger.info(
            f"[{user_id}] Composite: {sym} "
            f"score={best_candidate['score']} × conf={signal_data['confidence']:.2f} "
            f"× bonus={bonus} = {composite:.2f} (seuil={MIN_COMPOSITE_SCORE_BULL if market_mode!='BEAR' else MIN_COMPOSITE_SCORE_BEAR})"
        )

        return {
            "symbol":          sym,
            "signal":          signal_data,
            "indicators":      indicators,
            "composite_score": composite,
            "buy_the_dip":     best_candidate.get("buy_the_dip", False),
        }

    async def _analyze_pair_fast(
        self, symbol: str
    ) -> Optional[Tuple[Dict, Dict, Dict, Dict, Dict]]:
        """Quad timeframe : 5m + 15m + 1h + 4h. Retourne (ind_5m, sig_5m, sig_15m, sig_1h, sig_4h)."""
        try:
            # ── 5m ───────────────────────────────────────────────────────────
            key_5m = f"klines:{symbol}:5m"
            df_5m = cache.get(key_5m)
            if df_5m is None:
                df_5m = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines(symbol, "5m", limit=100)
                )
                cache.set(key_5m, df_5m, ttl_seconds=KLINE_TTL.get("5m", 300))

            ind_key_5m = f"indicators:{symbol}:5m"
            ind_5m = cache.get(ind_key_5m)
            if ind_5m is None:
                ind_5m = analysis_service.compute_indicators(df_5m, symbol=symbol)
                cache.set(ind_key_5m, ind_5m, ttl_seconds=INDICATOR_TTL.get("5m", 300))

            rule_5m = ind_5m.get("rule_signal", {})

            # ── 15m ──────────────────────────────────────────────────────────
            rule_15m: Dict = {}
            try:
                key_15m = f"klines:{symbol}:15m"
                df_15m = cache.get(key_15m)
                if df_15m is None:
                    df_15m = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: binance_service.get_klines(symbol, "15m", limit=100)
                    )
                    cache.set(key_15m, df_15m, ttl_seconds=KLINE_TTL.get("15m", 900))

                ind_key_15m = f"indicators:{symbol}:15m"
                ind_15m = cache.get(ind_key_15m)
                if ind_15m is None:
                    ind_15m = analysis_service.compute_indicators(df_15m, symbol=symbol)
                    cache.set(ind_key_15m, ind_15m, ttl_seconds=INDICATOR_TTL.get("15m", 900))

                rule_15m = ind_15m.get("rule_signal", {})
            except Exception as e15:
                logger.debug(f"15m {symbol} failed (non bloquant): {e15}")

            # ── 1h ───────────────────────────────────────────────────────────
            rule_1h: Dict = {}
            try:
                key_1h = f"klines:{symbol}:1h"
                df_1h = cache.get(key_1h)
                if df_1h is None:
                    df_1h = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: binance_service.get_klines(symbol, "1h", limit=100)
                    )
                    cache.set(key_1h, df_1h, ttl_seconds=KLINE_TTL.get("1h", 3600))

                ind_key_1h = f"indicators:{symbol}:1h"
                ind_1h = cache.get(ind_key_1h)
                if ind_1h is None:
                    ind_1h = analysis_service.compute_indicators(df_1h, symbol=symbol)
                    cache.set(ind_key_1h, ind_1h, ttl_seconds=INDICATOR_TTL.get("1h", 3600))

                rule_1h = ind_1h.get("rule_signal", {})
            except Exception as e1h:
                logger.debug(f"1h {symbol} failed (non bloquant): {e1h}")

            # ── 4h (macro trend) ──────────────────────────────────────────
            rule_4h: Dict = {}
            try:
                key_4h = f"klines:{symbol}:4h"
                df_4h = cache.get(key_4h)
                if df_4h is None:
                    df_4h = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: binance_service.get_klines(symbol, "4h", limit=50)
                    )
                    cache.set(key_4h, df_4h, ttl_seconds=KLINE_TTL.get("4h", 14400))

                ind_key_4h = f"indicators:{symbol}:4h"
                ind_4h = cache.get(ind_key_4h)
                if ind_4h is None:
                    ind_4h = analysis_service.compute_indicators(df_4h, symbol=symbol)
                    cache.set(ind_key_4h, ind_4h, ttl_seconds=INDICATOR_TTL.get("4h", 14400))

                rule_4h = ind_4h.get("rule_signal", {})
            except Exception as e4h:
                logger.debug(f"4h {symbol} failed (non bloquant): {e4h}")

            return ind_5m, rule_5m, rule_15m, rule_1h, rule_4h

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
        entry    = float(trade.get("price", 0))
        qty      = float(trade.get("quantity", 0))
        tp_price = float(trade.get("take_profit_price", entry * (1 + TAKE_PROFIT_PCT / 100)))
        sl_price = float(trade.get("stop_loss_price",   entry * (1 - STOP_LOSS_PCT  / 100)))
        highest  = float(trade.get("highest_price_seen", entry))

        if not entry or not qty:
            return

        pnl_pct = (price - entry) / entry * 100

        # Trailing step adaptatif : plus serré quand on est bien en profit (lock gains)
        adx_entry  = float(trade.get("signal_adx", 0) or 0)
        if pnl_pct >= 2.0:
            trail_step = 0.20   # > +2% : trail très serré — protège presque tout
        elif pnl_pct >= 1.5:
            trail_step = 0.25   # > +1.5% : trail serré
        elif adx_entry > 35:
            trail_step = 0.30   # tendance forte à l'entrée
        else:
            trail_step = TRAIL_STEP_PCT  # 0.35% par défaut

        # Mettre à jour le plus haut
        updates = {}
        if price > highest:
            updates["highest_price_seen"] = price
            highest = price

        # ── BREAKEVEN : dès +0.5%, SL monte à l'entrée + 0.1% ───────────────
        if pnl_pct >= BREAKEVEN_PCT and sl_price < entry:
            new_sl = round(entry * 1.001, 8)
            updates["stop_loss_price"] = new_sl
            sl_price = new_sl
            logger.info(f"[{user_id}] Breakeven {trade['symbol']}: SL → {new_sl:.4f} (+0.1%)")

        # ── TRAILING SL : dès +0.8%, SL suit le plus haut à -trail_step% ────
        # SL monte uniquement → lock les gains progressivement
        if pnl_pct >= TRAIL_TRIGGER_PCT:
            trail_floor = round(highest * (1 - trail_step / 100), 8)
            if trail_floor > sl_price:
                updates["stop_loss_price"] = trail_floor
                sl_price = trail_floor
                logger.info(
                    f"[{user_id}] Trailing SL {trade['symbol']}: "
                    f"+{pnl_pct:.1f}% highest={highest:.4f} → SL={trail_floor:.4f}"
                )

        if updates:
            await db.trades.update_one({"_id": trade["_id"]}, {"$set": updates})

        # ── FERMETURE ─────────────────────────────────────────────────────────

        # Take-profit atteint
        if price >= tp_price:
            await self._close_position(user_id, trade, price, "take_profit")
            return

        # Stop-loss (inclut le trailing SL mis à jour ci-dessus)
        if price <= sl_price:
            reason = "trailing_stop" if pnl_pct > 0 else "stop_loss"
            await self._close_position(user_id, trade, price, reason)
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
        # Garde-fou spread — évite les achats en marché illiquide (mauvais prix d'exécution)
        try:
            spread_pct = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.get_spread_pct(symbol)
            )
            if spread_pct > 0.25:
                logger.warning(f"[{user_id}] {symbol}: spread {spread_pct:.3f}% trop large — achat annulé")
                return
        except Exception as e_spread:
            logger.debug(f"Spread check {symbol} non bloquant: {e_spread}")

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

        # Ajustement TP selon résistance proche — éviter de se faire rejeter
        if indicators:
            resistance = indicators.get("levels", {}).get("resistance", 0)
            if resistance > ex_price * 1.005:
                dist_res_pct = (resistance - ex_price) / ex_price * 100
                if dist_res_pct < tp_pct:
                    # Résistance avant le TP → viser juste en dessous
                    adj_tp_pct = max(dist_res_pct * 0.90, sl_pct * 2.2)
                    if adj_tp_pct < tp_pct:
                        tp_price = ex_price * (1 + adj_tp_pct / 100)
                        logger.info(
                            f"[{user_id}] TP ajusté sous résistance {resistance:.4f}: "
                            f"{tp_pct:.2f}% → {adj_tp_pct:.2f}%"
                        )

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
        """Fetch tous les prix en parallèle."""
        async def _get(sym):
            key = f"price:{sym}"
            p   = cache.get(key)
            if p: return sym, p
            try:
                p = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_current_price(sym)
                )
                cache.set(key, p, ttl_seconds=PRICE_TTL)
                return sym, p
            except Exception:
                return sym, 0.0

        results = await asyncio.gather(*[_get(s) for s in symbols])
        return {sym: price for sym, price in results if price > 0}

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
