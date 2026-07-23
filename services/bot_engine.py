import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from database import get_database
from services import analysis_service, claude_service
from services.binance_service import binance_service
from services.futures_service import futures_service
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

# 25 paires liquides — couvre bull, bear ET range market
SCAN_PAIRS = [
    "SOLUSDT",  "XRPUSDT",  "ADAUSDT",
    "LTCUSDT",  "TRXUSDT",  "LINKUSDT",
    "ETHUSDT",  "UNIUSDT",  "ATOMUSDT", "NEARUSDT",
    "APTUSDT",  "ARBUSDT",  "OPUSDT",   "INJUSDT",  "SUIUSDT",
    "TIAUSDT",  "JUPUSDT",  "FETUSDT",
]
# Paires nécessitant min notional $10 — exclues si capital < $15
HIGH_NOTIONAL_PAIRS = {"BTCUSDT", "ETHUSDT"}
# Paires bannies définitivement (WR < 20% ou pertes structurelles)
BANNED_PAIRS = {
    "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "BNBUSDT",   # anciens losers
    "SPCXBUSDT", "DEXEUSDT", "WLDUSDT", "HMSTRUSDT", # nouveaux losers jul-13
    "MUBUSDT", "GRAMUSDT", "OPGUSDT",                # grosses pertes isolées
    "SHIBUSDT", "MATICUSDT", "LDOUSDT", "FTMUSDT",   # tokens faible qualité
    "RNDRUSDT", "WIFUSDT",                           # mèmes/hype, WR < 25%
    "KITEUSDT",                                      # paire volatile ajoutée par hot_pairs — banni
}
PAIR_EXTRA_FILTERS: Dict[str, Dict] = {}  # réservé pour futures paires à seuils renforcés

# TP/SL — défauts BULL (ATR override dans run_cycle)
TRAIL_TRIGGER_PCT  = 2.0   # trailing SL activé à +2.0% — laisse les gagnants courir
TRAIL_STEP_PCT     = 0.25  # SL trail = highest × (1 - 0.25%)
BREAKEVEN_PCT      = 1.5   # SL → entrée dès +1.5% — protège sans couper trop tôt
STOP_LOSS_PCT      = 0.9   # SL défaut BULL
TAKE_PROFIT_PCT    = 5.0   # TP défaut BULL — laisser les winners courir (était 3.0)

# ── SL/TP spécifiques par mode — override ATR ────────────────────────────────
BEAR_SL_PCT  = 0.5   # BEAR : coupe vite si rebond échoue
BEAR_TP_PCT  = 1.0   # BEAR : 1% réaliste même en downtrend (WR cible 55-65%)
RANGE_SL_PCT = 0.4   # RANGE : volatilité faible — SL très serré
RANGE_TP_PCT = 1.2   # RANGE : milieu de bande BB

# ── Filtres BULL market — seuils élevés, qualité avant quantité ──────────────
MIN_CONFIDENCE_BULL      = 0.82   # relevé 0.75→0.82 : seuls les signaux forts passent
MIN_COMPOSITE_SCORE_BULL = 6.0   # abaissé 6.5→6.0 : laisser passer plus de setups valides
MIN_SCORE_RAW_BULL       = 8     # ajusté 9→8 : trop restrictif
MIN_ADX_BULL             = 25    # relevé 22→25 : tendance plus établie requise
MIN_VOLUME_RATIO_BULL    = 1.8   # relevé 1.5→1.8 : volume fort confirme le move

# ── Filtres BEAR market — TRADING DÉSACTIVÉ ──────────────────────────────────
# WR 27% en BEAR = pire que pile ou face. Ne jamais trader contre le trend.
# Le capital est préservé. On attend le retour en BULL ou RANGE.
MIN_CONFIDENCE_BEAR      = 0.70
MIN_COMPOSITE_SCORE_BEAR = 6.0
MIN_SCORE_RAW_BEAR       = 7
MIN_ADX_BEAR             = 15
MIN_VOLUME_RATIO_BEAR    = 1.2
RSI_OVERSOLD_BEAR        = 28    # capitulation extrême uniquement
BB_PCT_MAX_BEAR          = 0.15  # tiers très bas BB
BEAR_MAX_DAILY_TRADES    = 0     # BEAR = 0 trades — capital préservé, attente BULL

# ── Filtres RANGE market — entrée uniquement en zone très basse ───────────────
MIN_CONFIDENCE_RANGE      = 0.72  # relevé 0.65→0.72
MIN_COMPOSITE_SCORE_RANGE = 6.0   # relevé 5.5→6.0
MIN_SCORE_RAW_RANGE       = 6     # relevé 5→6
MIN_ADX_RANGE             = 8
MIN_VOLUME_RATIO_RANGE    = 0.8   # relevé 0.6→0.8
RSI_MAX_RANGE             = 38    # relevé 48→38 : zone oversold stricte
BB_PCT_MAX_RANGE          = 0.18  # relevé 0.30→0.18 : fond de range uniquement

# Aliases (compatibilité)
MIN_CONFIDENCE      = MIN_CONFIDENCE_BULL
MIN_COMPOSITE_SCORE = MIN_COMPOSITE_SCORE_BULL
MIN_SCORE_RAW       = MIN_SCORE_RAW_BULL
MIN_ADX             = MIN_ADX_BULL
MIN_VOLUME_RATIO    = MIN_VOLUME_RATIO_BULL
MAX_CONSECUTIVE_LOSSES   = 2        # circuit breaker après 2 pertes (était 3)
SL_COOLDOWN_SECONDS      = 90 * 60  # 90 min cooldown après SL (était 45 min)
MIN_TRADE_INTERVAL_SECS  = 20 * 60  # 20 min entre trades — qualité > quantité
DAILY_MAX_LOSS_PCT        = 1.0     # stoppe à -1% jour (était 1.2%)
MAX_DAILY_TRADES          = 6       # max 6 trades/jour — moins c'est plus (était 18)
DAILY_PROFIT_LOCK_PCT     = 5.0     # verrouille les gains si +5% du capital en une journée (était 2%)

# ── FUTURES / LEVIER ────────────────────────────────────────────────────────
FUTURES_ENABLED          = True     # active le trading avec levier en mode Aggressive Bull
FUTURES_LEVERAGE         = 10       # 10x levier — 20 USDT → 200 USDT pouvoir d'achat
FUTURES_BALANCE_RESERVE  = 2.0     # garder 2 USDT non engagés comme marge de sécurité
MAX_DAILY_LOSS_FUTURES   = 8.0     # arrêt total Futures si perte > 8 USDT/jour

# ── AGGRESSIVE BULL MODE ────────────────────────────────────────────────────
AGGRESSIVE_BULL_MIN_CONF    = 0.80  # abaissé 0.85→0.80 pour plus de trades Futures
AGGRESSIVE_BULL_MIN_ADX     = 25    # abaissé 30→25 pour capter plus de setups
AGGRESSIVE_BULL_POSITION_PCT = 0.65 # 65% du capital Futures engagé
AGGRESSIVE_BULL_TP_PCT       = 5.0  # TP 5% — ride le trend
AGGRESSIVE_BULL_SL_PCT       = 1.0  # SL 1% — coupe vite si ça retourne

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
            "cb_last_reset_at": datetime.now(timezone.utc),
            "scan_results": {},
            "sl_cooldown": {},          # {symbol: expiry_datetime} — paires en cooldown après SL
            "pair_losses": {},         # {symbol: count} — SL consécutifs par paire
            "pending_entry": None,     # signal en attente d'un pullback vers EMA9
            "daily_start_capital": 0.0,  # capital en début de journée UTC (legacy)
            "last_day_reset": None,    # date du dernier reset journalier
            "daily_trade_count": 0,    # trades ouverts aujourd'hui (reset à minuit UTC)
            "daily_trade_date": None,  # date du compteur
        }

        db = get_database()
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"bot_config.is_running": True}})

        # Reconstruire sl_cooldown depuis MongoDB — survit aux redéploiements
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=8)
            recent_sl = await db.trades.find({
                "user_id": user_id, "status": "CLOSED",
                "close_reason": "stop_loss", "closed_at": {"$gt": cutoff}
            }).sort("closed_at", 1).to_list(100)
            pair_counts: Dict[str, int] = {}
            for t in recent_sl:
                sym = t.get("symbol", "")
                closed_at = t.get("closed_at")
                if not sym or not closed_at:
                    continue
                if closed_at.tzinfo is None:
                    closed_at = closed_at.replace(tzinfo=timezone.utc)
                pair_counts[sym] = pair_counts.get(sym, 0) + 1
                cnt = pair_counts[sym]
                secs = 8 * 3600 if cnt >= 3 else (3 * 3600 if cnt >= 2 else 45 * 60)
                expiry = closed_at + timedelta(seconds=secs)
                if expiry > datetime.now(timezone.utc):
                    self._running_bots[user_id]["sl_cooldown"][sym] = expiry
                    self._running_bots[user_id]["pair_losses"][sym] = cnt
            if self._running_bots[user_id]["sl_cooldown"]:
                logger.info(f"[{user_id}] SL cooldown restauré: {list(self._running_bots[user_id]['sl_cooldown'].keys())}")
        except Exception as e:
            logger.warning(f"[{user_id}] SL cooldown restore failed: {e}")

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
                    sym = trade.get("symbol", "")
                    if trade.get("futures"):
                        # Futures : détection fermeture SL/TP serveur Binance
                        await self._manage_futures_position(user_id, trade, db)
                        continue
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
            if not since or (datetime.now(timezone.utc) - since).total_seconds() >= 2 * 3600:
                bot_info["circuit_breaker_active"] = False
                bot_info["circuit_breaker_reason"]  = None
                bot_info["consecutive_losses"]      = 0
                bot_info["cb_last_reset_at"]        = datetime.now(timezone.utc)
                logger.info(f"[{user_id}] Circuit breaker reset automatique (2h ecoulees)")
            else:
                logger.warning(f"[{user_id}] Circuit breaker actif — cycle ignore")
                return

        # ── 0. Filtre horaire — zone morte 03h-05h UTC (réduit de 4h à 2h)
        # 02h-03h conservé car oversold BEAR fréquents en début session asiatique
        utc_hour = datetime.now(timezone.utc).hour
        if 3 <= utc_hour <= 5:
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

        # ── 2b. Perte journalière + max trades — calcul MongoDB (résiste aux restarts) ──
        today = datetime.now(timezone.utc).date()
        total_usdt = portfolio_data.get("total_usdt", available_usdt)

        # Reset compteur trades si nouveau jour
        if bot_info.get("daily_trade_date") != today:
            bot_info["daily_trade_count"] = 0
            bot_info["daily_trade_date"]  = today

        # Perte journalière depuis MongoDB — survit aux redéploiements Railway
        today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_closed = await db.trades.find(
            {"user_id": user_id, "status": "CLOSED", "closed_at": {"$gte": today_utc}},
            {"pnl": 1}
        ).to_list(200)
        daily_realized_pnl = sum(float(t.get("pnl") or 0) for t in today_closed)

        # ── Protection perte journalière ─────────────────────────────────────
        if daily_realized_pnl < 0:
            daily_start_est = total_usdt - daily_realized_pnl
            daily_loss_pct  = abs(daily_realized_pnl) / daily_start_est * 100
            if daily_loss_pct >= DAILY_MAX_LOSS_PCT:
                logger.warning(
                    f"[{user_id}] Perte journaliere {daily_loss_pct:.1f}% "
                    f">= {DAILY_MAX_LOSS_PCT}% (PnL={daily_realized_pnl:+.4f}$) — pause trading aujourd'hui"
                )
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return

        # ── Verrouillage gains journaliers — protège les profits acquis ──────
        # Si le bot a gagné >= DAILY_PROFIT_LOCK_PCT% aujourd'hui, on s'arrête.
        # Un bon jour reste un bon jour — ne pas le retransformer en mauvais jour.
        if daily_realized_pnl > 0 and total_usdt > 0:
            profit_lock_threshold = total_usdt * DAILY_PROFIT_LOCK_PCT / 100
            if daily_realized_pnl >= profit_lock_threshold:
                logger.info(
                    f"[{user_id}] Gains journaliers verrouilles: "
                    f"+{daily_realized_pnl:.4f} USDT (+{daily_realized_pnl/total_usdt*100:.1f}%) "
                    f">= {DAILY_PROFIT_LOCK_PCT}% — trading suspendu pour aujourd'hui"
                )
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return

        # Max trades par jour — empêche surtrading (ex: 40 trades le 08/07)
        if bot_info["daily_trade_count"] >= MAX_DAILY_TRADES:
            logger.info(
                f"[{user_id}] Max trades journaliers atteint ({bot_info['daily_trade_count']}/{MAX_DAILY_TRADES}) — pause"
            )
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # Vérifier solde Futures séparément — wallet indépendant du Spot
        _futures_balance = 0.0
        if FUTURES_ENABLED:
            try:
                _futures_balance = await asyncio.get_event_loop().run_in_executor(
                    None, futures_service.get_futures_balance
                )
            except Exception:
                pass

        _spot_ok    = available_usdt >= 5.5
        _futures_ok = FUTURES_ENABLED and _futures_balance >= FUTURES_BALANCE_RESERVE + 1.0

        if not _spot_ok and not _futures_ok:
            logger.info(
                f"[{user_id}] Capital insuffisant — Spot={available_usdt:.2f} "
                f"Futures={_futures_balance:.2f} USDT — pas de nouveau trade"
            )
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return
        if not _spot_ok:
            logger.info(
                f"[{user_id}] Spot insuffisant ({available_usdt:.2f}) — "
                f"Futures={_futures_balance:.2f} USDT disponible — scan en cours"
            )

        # ── 3. Prix courants en parallèle ────────────────────────────────────
        prices = await self._fetch_prices(all_scan)

        # ── 3b. Vérifier entrée en attente (pullback) ─────────────────────────
        executed_pending = await self._check_pending_entries(user_id, prices, db, portfolio_data)
        if executed_pending:
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 4. Circuit breaker — arrêt après MAX pertes consécutives ─────────
        cb_reset_at = bot_info.get("cb_last_reset_at")
        cb_query = {"user_id": user_id, "status": "CLOSED"}
        if cb_reset_at:
            cb_query["created_at"] = {"$gt": cb_reset_at}

        recent_trades = await db.trades.find(cb_query).sort("created_at", -1).limit(50).to_list(50)

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

        # ── 4b. Mode marché BTC ──────────────────────────────────────────────────
        market_mode, btc_reason = await self._get_market_mode()
        bot_info["market_mode"] = market_mode
        logger.info(f"[{user_id}] Mode marche: {market_mode} — {btc_reason}")

        # ── BEAR = arrêt total des nouvelles positions Spot ───────────────────────
        # WR 27% en BEAR prouve que les rebonds sont imprévisibles en downtrend.
        # On préserve le capital et on attend BULL ou RANGE.
        if market_mode == "BEAR":
            logger.info(f"[{user_id}] BEAR market — aucun nouveau trade Spot (capital préservé)")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── NEUTRAL = seuils BEAR stricts, pas de blocage total ─────────────────
        # Le marché NEUTRAL peut offrir de bons setups — on applique les filtres BEAR
        if market_mode == "NEUTRAL":
            logger.info(f"[{user_id}] NEUTRAL market — scan avec seuils BEAR conservateurs")

        # ── Garde BTC chute rapide — dump en cours ───────────────────────────────
        btc_fast_drop = await self._btc_fast_drop()
        if btc_fast_drop:
            logger.warning(f"[{user_id}] BTC chute rapide — entrées suspendues")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 5. Positions ouvertes + limite adaptative ─────────────────────────
        open_trades = await db.trades.find(
            {"user_id": user_id, "status": "OPEN"}
        ).to_list(20)
        open_symbols  = {t.get("symbol") for t in open_trades}
        max_positions = self._get_max_positions(total_usdt)

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
        signal_data["symbol"] = symbol          # requis par risk_manager (veto TradingAgents + corrélations)
        indicators    = best["indicators"]
        current_price = prices.get(symbol, 0)

        # Hot pairs (ex: ZECUSDT) peuvent ne pas être dans prices — fetch direct
        if current_price <= 0:
            try:
                current_price = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_current_price(symbol)
                )
            except Exception as e_price:
                logger.warning(f"[{user_id}] Prix {symbol} introuvable ({e_price}) — signal ignoré")
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return

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
            # RSI capitulation obligatoire (≤ 30) pour tous les signaux BEAR
            rsi_chk    = indicators.get("momentum", {}).get("rsi", 50)
            bb_pct_chk = indicators.get("volatility", {}).get("bb_pct", 0.5)
            if rsi_chk > RSI_OVERSOLD_BEAR:
                logger.info(f"[{user_id}] BEAR RSI {rsi_chk:.1f} > {RSI_OVERSOLD_BEAR} — pas en capitulation, refuse")
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return
            if bb_pct_chk > BB_PCT_MAX_BEAR:
                logger.info(f"[{user_id}] BEAR bb_pct={bb_pct_chk:.2f} > {BB_PCT_MAX_BEAR} — hors zone rebond, refuse")
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return
        elif market_mode == "RANGE":
            min_conf  = MIN_CONFIDENCE_RANGE
            min_score = MIN_COMPOSITE_SCORE_RANGE
            # RANGE : acheter uniquement en zone basse (RSI < RSI_MAX_RANGE = 52)
            if not is_btd:
                rsi_chk = indicators.get("momentum", {}).get("rsi", 50)
                if rsi_chk > RSI_MAX_RANGE:
                    logger.info(f"[{user_id}] RANGE RSI {rsi_chk:.1f} > {RSI_MAX_RANGE} — hors zone achat range, ignore")
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

        # ── 8b. Filtre régime TRENDING_DOWN — non présent dans le scan ─────────────
        # EMA9/MACD/4h/RSI/Stoch déjà filtrés dans _scan_best_opportunity — pas de doublon
        market_regime = signal_data.get("market_regime", "RANGING")
        if market_mode == "BULL" and market_regime == "TRENDING_DOWN" and not is_btd:
            logger.info(f"[{user_id}] BULL: régime TRENDING_DOWN sans BTD — skip")
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

        # ── 10. SL/TP — stratégie adaptée au mode marché ────────────────────
        conf = signal_data["confidence"]
        atr  = indicators.get("volatility", {}).get("atr", 0)

        if market_mode == "BEAR":
            # Bounce scalping : TP réaliste 1%, SL serré 0.5% — R:R 2:1
            # En downtrend les rebonds durent peu — prendre profit vite
            sl_pct = BEAR_SL_PCT   # 0.5%
            tp_pct = BEAR_TP_PCT   # 1.0%
            logger.info(f"[{user_id}] BEAR bounce-scalp SL={sl_pct}% TP={tp_pct}% R:R=2:1")

        elif market_mode == "RANGE":
            # Range scalping : SL ATR ajusté [0.4-0.7%], TP vers milieu BB, R:R minimum 4:1
            if atr > 0 and current_price > 0:
                raw_sl = (atr / current_price) * 100
                sl_pct = round(max(min(raw_sl, 0.7), RANGE_SL_PCT), 3)  # [0.4%, 0.7%]
            else:
                sl_pct = RANGE_SL_PCT  # 0.4%
            bb_upper_v = indicators.get("volatility", {}).get("bb_upper", 0)
            bb_lower_v = indicators.get("volatility", {}).get("bb_lower", 0)
            if bb_upper_v > 0 and bb_lower_v > 0 and current_price > 0:
                bb_mid_v = (bb_upper_v + bb_lower_v) / 2
                dist_mid = max((bb_mid_v - current_price) / current_price * 100, 0)
                # 95% du chemin vers la médiane, min R:R 4:1 — plus ambitieux
                tp_pct = round(max(dist_mid * 0.95, sl_pct * 4.0), 3)
            else:
                tp_pct = round(sl_pct * 4.0, 3)
            logger.info(f"[{user_id}] RANGE scalp SL={sl_pct}% TP={tp_pct}% R:R={tp_pct/sl_pct:.1f}:1")

        else:
            # BULL — trend following ATR dynamique, R:R minimum 3:1
            if atr > 0 and current_price > 0:
                raw_sl = (1.0 * atr / current_price) * 100
                raw_tp = (6.0 * atr / current_price) * 100
                sl_pct = round(max(min(raw_sl, 0.9), 0.6), 3)    # SL [0.6%, 0.9%]
                tp_pct = round(max(min(raw_tp, 8.0), max(3.5, sl_pct * 5.0)), 3)
                logger.info(
                    f"[{user_id}] ATR SL={sl_pct}% TP={tp_pct}% ratio={tp_pct/sl_pct:.1f}:1 conf={conf:.0%} [BULL]"
                )
            else:
                sl_pct = STOP_LOSS_PCT    # 0.9%
                tp_pct = TAKE_PROFIT_PCT  # 2.5%
                logger.info(f"[{user_id}] Default SL={sl_pct}% TP={tp_pct}% [BULL]")

            # R:R minimal 5:1 en BULL — garanti
            if tp_pct / sl_pct < 5.0:
                tp_pct = round(sl_pct * 5.0, 3)
                logger.info(f"[{user_id}] R:R ajusté → SL={sl_pct}% TP={tp_pct}% (5:1 garanti)")

        # TP étendu pour setups de très haute qualité (BB Squeeze + ADX fort + triple bull)
        _bb_sq = indicators.get("volatility", {}).get("bb_squeeze", False)
        _bb_ex = indicators.get("volatility", {}).get("bb_expanding", False)
        _adx_v = indicators.get("trend", {}).get("adx", 0)
        _adx_r = indicators.get("trend", {}).get("adx_rising", False)
        _triple = best.get("triple_bull", False) if best else False
        if _bb_sq and _bb_ex and _adx_v > 25 and _triple:
            extended_tp = round(sl_pct * 7.0, 3)
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

        # BEAR : position réduite — on scalpe des rebonds, on ne mise pas gros
        if market_mode == "BEAR":
            position_usdt *= 0.65
            position_usdt = max(position_usdt, 5.50)
            logger.info(f"[{user_id}] BEAR mode — position ×0.65 (rebond scalping)")

        # Multiplicateur qualité setup : BB Squeeze ou ADX fort → position plus grande
        bb_squeeze_now  = indicators.get("volatility", {}).get("bb_squeeze", False)
        bb_expanding_now = indicators.get("volatility", {}).get("bb_expanding", False)
        adx_now_val     = indicators.get("trend", {}).get("adx", 0)
        adx_rising_now  = indicators.get("trend", {}).get("adx_rising", False)
        conf_now        = signal_data.get("confidence", 0)

        quality_label = ""
        # Multiplicateurs qualité désactivés en BEAR — position déjà réduite à ×0.65
        if market_mode != "BEAR":
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

        # ── 12. Futures — évalué EN PREMIER, indépendamment de la position vs EMA9 ──
        # Bug corrigé : Futures était dans le bloc else du pullback → jamais évalué en bull
        adx_for_agg    = indicators.get("trend", {}).get("adx", 0)
        triple_for_agg = best.get("triple_bull", False) if best else False
        use_futures = self._is_aggressive_bull(
            market_mode, signal_data.get("confidence", 0), adx_for_agg, triple_for_agg
        )
        if use_futures:
            futures_ok = await self._execute_futures_long(
                user_id, db, symbol, signal_data, current_price, indicators
            )
            if futures_ok:
                bot_info["last_trade_at"]     = datetime.now(timezone.utc)
                bot_info["daily_trade_count"] = bot_info.get("daily_trade_count", 0) + 1
                logger.info(f"[{user_id}] Futures LONG execute — skip trade Spot")
                bot_info["cycles_count"] += 1
                bot_info["last_cycle_at"] = datetime.now(timezone.utc)
                return  # Futures pris → pas de trade Spot en plus

        # ── 13. Spot — Pullback entry si prix très au-dessus EMA9 (>1.2%) ────
        # Seuil relevé 0.3%→1.2% : évite de bloquer tous les trades en bull market
        ema9 = indicators.get("trend", {}).get("ema_9", 0)
        if ema9 > 0 and current_price > ema9 * 1.012 and market_mode not in ("RANGE", "BEAR"):
            # Prix > EMA9 + 1.2% → attendre un pullback vers EMA9
            bot_info["pending_entry"] = {
                "symbol":        symbol,
                "target_entry":  round(ema9, 8),
                "signal_data":   signal_data,
                "indicators":    indicators,
                "sl_pct":        sl_pct,
                "tp_pct":        tp_pct,
                "config":        config,
                "created_at":    datetime.now(timezone.utc),
                "expires_at":    datetime.now(timezone.utc) + timedelta(minutes=15),
            }
            logger.info(
                f"[{user_id}] Pullback pending {symbol}: "
                f"prix={current_price:.4f} > EMA9+1.2%={ema9*1.012:.4f} — attente retour"
            )
        else:
            # Prix proche ou sous EMA9+1.2% → entrée Spot immédiate
            if available_usdt >= 5.5:
                buy_ok = await self._execute_buy(
                    user_id, db, symbol, position_usdt, current_price,
                    sl_pct, tp_pct, signal_data, portfolio_data, config,
                    indicators=indicators,
                )
                if buy_ok:
                    bot_info["last_trade_at"]     = datetime.now(timezone.utc)
                    bot_info["daily_trade_count"] = bot_info.get("daily_trade_count", 0) + 1
            else:
                logger.info(f"[{user_id}] Spot insuffisant ({available_usdt:.2f}) — trade Spot ignore")

        bot_info["cycles_count"] += 1
        bot_info["last_cycle_at"] = datetime.now(timezone.utc)

    # ══════════════════════════════════════════════════════════════════════════
    # FILTRE BTC MACRO
    # ══════════════════════════════════════════════════════════════════════════

    async def _get_market_mode(self) -> Tuple[str, str]:
        """
        Détermine le mode de marché : BULL / BEAR / RANGE / NEUTRAL.
        Utilise 15m + 1h pour éviter les faux changements de mode sur micro-mouvements.
        """
        try:
            # ── 15m : tendance court terme ────────────────────────────────────
            key_df15 = "klines:BTCUSDT:15m"
            df15 = cache.get(key_df15)
            if df15 is None:
                df15 = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines("BTCUSDT", "15m", limit=100)
                )
                cache.set(key_df15, df15, ttl_seconds=KLINE_TTL.get("15m", 900))

            key_i15 = "indicators:BTCUSDT:15m"
            ind15 = cache.get(key_i15)
            if ind15 is None:
                ind15 = analysis_service.compute_indicators(df15, symbol="BTCUSDT")
                cache.set(key_i15, ind15, ttl_seconds=INDICATOR_TTL.get("15m", 900))

            # ── 1h : tendance moyen terme ─────────────────────────────────────
            key_df1h = "klines:BTCUSDT:1h"
            df1h = cache.get(key_df1h)
            if df1h is None:
                df1h = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines("BTCUSDT", "1h", limit=50)
                )
                cache.set(key_df1h, df1h, ttl_seconds=KLINE_TTL.get("1h", 3600))

            key_i1h = "indicators:BTCUSDT:1h"
            ind1h = cache.get(key_i1h)
            if ind1h is None:
                ind1h = analysis_service.compute_indicators(df1h, symbol="BTCUSDT")
                cache.set(key_i1h, ind1h, ttl_seconds=INDICATOR_TTL.get("1h", 3600))

            trend15   = ind15.get("trend", {})
            candles15 = ind15.get("candles_summary", [])
            btc_close = candles15[-1]["close"] if candles15 else 0
            ema21_15  = trend15.get("ema_21", 0)
            adx_15    = trend15.get("adx", 0)

            trend1h   = ind1h.get("trend", {})
            ema21_1h  = trend1h.get("ema_21", 0)
            adx_1h    = trend1h.get("adx", 0)

            if btc_close <= 0 or ema21_15 <= 0:
                return "NEUTRAL", "donnees BTC indisponibles"

            # RANGE : ADX faible sur les 2 TF = vraie consolidation
            if adx_15 > 0 and adx_15 < 18 and adx_1h < 22:
                return "RANGE", f"BTC ADX15={adx_15:.1f} ADX1h={adx_1h:.1f} — consolidation confirmée"

            # BULL : 15m ET 1h au-dessus EMA21 = tendance haussière solide
            bull_15m = btc_close > ema21_15
            # ema21_1h doit être > 0 : si données absentes → NEUTRAL (pas de faux BULL)
            bull_1h  = (ema21_1h > 0 and btc_close > ema21_1h)
            if bull_15m and bull_1h:
                return "BULL", f"BTC {btc_close:.0f} > EMA21_15m={ema21_15:.0f} + 1h haussier"

            # BEAR : 15m ET 1h sous EMA21 = downtrend confirmé
            bear_15m = btc_close < ema21_15
            bear_1h  = (ema21_1h > 0 and btc_close < ema21_1h)
            if bear_15m and bear_1h:
                return "BEAR", f"BTC {btc_close:.0f} < EMA21_15m={ema21_15:.0f} + 1h baissier"

            # Signaux contradictoires ou données 1h absentes → NEUTRAL
            return "NEUTRAL", f"BTC 15m={'haussier' if bull_15m else 'baissier'} — confirmation 1h insuffisante"

        except Exception as e:
            logger.warning(f"Market mode check failed: {e}")
            return "NEUTRAL", "erreur — mode NEUTRAL par defaut"

    # ══════════════════════════════════════════════════════════════════════════
    # GARDE BTC CHUTE RAPIDE
    # ══════════════════════════════════════════════════════════════════════════

    async def _btc_fast_drop(self) -> bool:
        """Détecte une chute BTC > 1.5% sur les 3 dernières bougies 5m.
        Si vrai : dump actif, aucun rebond ne tient — suspendre tous les BUY."""
        try:
            key = "klines:BTCUSDT:5m"
            df = cache.get(key)
            if df is None:
                df = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines("BTCUSDT", "5m", limit=20)
                )
                cache.set(key, df, ttl_seconds=60)
            if df is None or len(df) < 4:
                return False
            recent = df.tail(4)
            price_now  = float(recent.iloc[-1]["close"])
            price_3ago = float(recent.iloc[0]["close"])
            drop_pct = (price_now - price_3ago) / price_3ago * 100
            if drop_pct < -1.5:
                logger.warning(f"BTC fast drop: {drop_pct:.2f}% en 15min — entrées suspendues")
                return True
            return False
        except Exception:
            return False

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

        # Respecte les mêmes gardes que run_cycle (circuit breaker, daily limit, cooldown inter-trades)
        if bot_info.get("circuit_breaker_active"):
            return False

        if bot_info.get("daily_trade_count", 0) >= MAX_DAILY_TRADES:
            logger.info(f"[{user_id}] Pending entry annulé — max trades journalier atteint")
            bot_info["pending_entry"] = None
            return False

        last_trade_at = bot_info.get("last_trade_at")
        if last_trade_at:
            elapsed = (datetime.now(timezone.utc) - last_trade_at).total_seconds()
            if elapsed < MIN_TRADE_INTERVAL_SECS:
                return False  # trop tôt, pending entry conservée pour le prochain cycle

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
        max_pos = self._get_max_positions(portfolio_data.get("total_usdt", 0))
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

            max_positions = self._get_max_positions(portfolio_data.get("total_usdt", available_usdt))
            position_usdt = (available_usdt / max_positions) * 0.90
            # Réduction taille après pertes consécutives — même logique que run_cycle
            consecutive_now = bot_info.get("consecutive_losses", 0)
            if consecutive_now >= 3:
                position_usdt *= 0.5
                logger.info(f"[{user_id}] Pending entry taille ×0.5 ({consecutive_now} pertes consécutives)")
            elif consecutive_now >= 2:
                position_usdt *= 0.7
            position_usdt = max(position_usdt, 5.50)
            position_usdt = min(position_usdt, available_usdt * 0.95)

            logger.info(
                f"[{user_id}] Pullback atteint {symbol}: "
                f"prix={current_price:.4f} <= cible={target:.4f} — execution"
            )

            config = pending["config"]
            buy_ok = await self._execute_buy(
                user_id, db, symbol, position_usdt, current_price,
                pending["sl_pct"], pending["tp_pct"],
                pending["signal_data"], portfolio_data, config,
                indicators=pending.get("indicators"),
            )
            bot_info["pending_entry"] = None
            if buy_ok:
                bot_info["last_trade_at"]    = datetime.now(timezone.utc)
                bot_info["daily_trade_count"] = bot_info.get("daily_trade_count", 0) + 1
            return bool(buy_ok)

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
                    None, lambda: binance_service.get_top_pairs(min_volume_usdt=20_000_000)
                )
                # Hot pairs : variation > 3% + volume > 20M + symbole <= 10 chars
                # Seuils stricts pour éviter tokens manipulés (SPCXB/DEXE/WLD type)
                scan_set = set(SCAN_PAIRS)
                # Hot pairs désactivées — ajouter des paires ayant déjà +3% = chasser les pompes
                # On reste strictement sur SCAN_PAIRS (18 paires liquides et connues)
                hot = []
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
            and s not in BANNED_PAIRS
            and (s not in HIGH_NOTIONAL_PAIRS or available >= 15.0)
            and (
                s not in sl_cooldown
                or now > sl_cooldown[s]
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
            macd_h         = trend_5m.get("macd_histogram", 0)
            volatility_ind = indicators_5m.get("volatility", {})
            bb_pct_5m      = volatility_ind.get("bb_pct", 0.5)

            # ── Déterminer le signal effectif ────────────────────────────
            effective_action = action_5m
            effective_score  = score_5m

            # RSI oversold extrême (< 20) → rebond forcé — flash crash recovery uniquement
            # RSI 20-28 = simplement oversold, géré par BTD logic ci-dessous
            if rsi < 20:
                effective_action = "BUY"
                effective_score  = max(score_5m, 6)
                logger.info(f"[{user_id}] {sym}: OVERSOLD EXTREME RSI={rsi:.0f} < 20 → BUY force")

            elif action_5m in ("SELL", "HOLD"):
                # Buy-the-dip : 5m baissier mais TFs supérieurs haussiers
                # BULL : exige DEUX TFs haussiers (15m ET 1h) + RSI < 50
                # BEAR/NEUTRAL : un seul TF suffit + RSI très oversold
                if market_mode == "BULL":
                    btd = (action_15m == "BUY" and action_1h == "BUY" and rsi < 50)
                elif market_mode == "BEAR":
                    btd = ((action_15m == "BUY" or action_1h == "BUY") and rsi < 30 and bb_pct_5m < BB_PCT_MAX_BEAR)
                elif market_mode == "RANGE":
                    # RANGE : RSI très oversold + bas de BB = rebond probable même sans TF haussier
                    btd = (rsi < 32 and bb_pct_5m < 0.20)
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
            if market_mode == "BEAR":
                min_raw = MIN_SCORE_RAW_BEAR
            elif market_mode == "RANGE":
                min_raw = MIN_SCORE_RAW_RANGE
            else:
                min_raw = MIN_SCORE_RAW_BULL
            if effective_score < min_raw:
                logger.info(f"[{user_id}] {sym}: score {effective_score} < {min_raw} ({market_mode}) — skip")
                continue

            # ── Filtres qualité pour BUY ──────────────────────────────────
            if effective_action == "BUY":
                # ADX minimum — abaissé pour OVERSOLD EXTREME (RSI<20) car flash crash = ADX naturellement bas
                is_extreme_oversold = (rsi < 20)
                if market_mode == "BEAR":
                    adx_min = MIN_ADX_BEAR
                elif market_mode == "RANGE":
                    adx_min = MIN_ADX_RANGE
                elif is_extreme_oversold:
                    adx_min = 12  # flash crash : pas de tendance mais rebond quasi-certain
                else:
                    adx_min = MIN_ADX_BULL
                if adx < adx_min:
                    logger.info(f"[{user_id}] {sym}: ADX={adx:.1f} < {adx_min} ({market_mode}{'  EXTREME' if is_extreme_oversold else ''}) — skip")
                    continue

                # Filtres renforcés pour paires à historique difficile
                extra = PAIR_EXTRA_FILTERS.get(sym)
                if extra:
                    adx_req   = adx_min + extra.get("min_adx_bonus", 0)
                    score_req = min_raw  + extra.get("min_score_bonus", 0)
                    if adx < adx_req:
                        logger.info(f"[{user_id}] {sym}: ADX={adx:.1f} < {adx_req} (filtre renforce) — skip")
                        continue
                    if effective_score < score_req:
                        logger.info(f"[{user_id}] {sym}: score {effective_score} < {score_req} (filtre renforce) — skip")
                        continue

                # Volume minimum — BTD = pullback naturellement à faible volume
                # EXTREME OVERSOLD (RSI<20) = flash crash, volume faible = normal
                is_btd_signal = (action_5m != "BUY" and effective_action == "BUY")
                if market_mode == "RANGE":
                    vol_min = 0.5 if is_btd_signal else MIN_VOLUME_RATIO_RANGE
                elif is_extreme_oversold:
                    vol_min = 0.5   # flash crash recovery : volume toujours faible pendant le crash
                elif market_mode == "BULL":
                    vol_min = 0.8 if is_btd_signal else 1.5
                else:  # BEAR / NEUTRAL
                    vol_min = 0.8 if is_btd_signal else 1.2
                if vol_ratio < vol_min:
                    logger.info(f"[{user_id}] {sym}: vol={vol_ratio:.1f}x < {vol_min}x ({market_mode}) — skip")
                    continue

                # RSI — zone selon mode
                if market_mode == "BEAR":
                    # RSI capitulation obligatoire (≤ 30) — plus de signaux neutres à RSI 42
                    if rsi > RSI_OVERSOLD_BEAR:
                        logger.debug(f"{sym}: BEAR RSI={rsi:.1f} > {RSI_OVERSOLD_BEAR} — pas en capitulation, skip")
                        continue
                    # Bas de BB obligatoire — prix doit être en zone de rebond réel
                    if bb_pct_5m > BB_PCT_MAX_BEAR:
                        logger.info(f"[{user_id}] {sym}: BEAR bb_pct={bb_pct_5m:.2f} > {BB_PCT_MAX_BEAR} — hors zone rebond BB, skip")
                        continue
                elif market_mode == "BULL":
                    # RSI > 60 = déjà trop monté, pas de place pour atteindre TP — skip
                    if rsi > 60:
                        logger.info(f"[{user_id}] {sym}: BULL RSI={rsi:.1f} > 60 (étendu, plus de place pour TP) — skip")
                        continue
                    # RSI 20-27 sans contexte extrême → refuser (signal faible)
                    if rsi < 28 and not is_extreme_oversold:
                        logger.debug(f"{sym}: BULL RSI={rsi:.1f} < 28 non-extreme — skip")
                        continue
                    # Prix en haut des BB (bb_pct > 0.65) = près de la résistance — skip
                    if bb_pct_5m > 0.65 and not is_btd_signal:
                        logger.info(f"[{user_id}] {sym}: BULL bb_pct={bb_pct_5m:.2f} > 0.65 (haut BB, près résistance) — skip")
                        continue
                elif market_mode == "RANGE":
                    if rsi > RSI_MAX_RANGE:
                        logger.info(f"[{user_id}] {sym}: RANGE RSI={rsi:.1f} > {RSI_MAX_RANGE} — hors zone achat range, skip")
                        continue
                    # RANGE : exiger zone basse BB OU RSI très oversold pour rebond fiable
                    if bb_pct_5m > BB_PCT_MAX_RANGE and rsi > 38:
                        logger.info(f"[{user_id}] {sym}: RANGE bb_pct={bb_pct_5m:.2f}>{BB_PCT_MAX_RANGE} RSI={rsi:.1f}>38 — hors zone rebond, skip")
                        continue
                else:
                    if not (25 <= rsi <= 75):
                        logger.info(f"[{user_id}] {sym}: NEUTRAL RSI={rsi:.1f} hors 25-75 — skip")
                        continue

                # MACD requis en BULL pour signaux directs — BTD exempté car pullback = MACD momentanément négatif
                if market_mode == "BULL" and macd_h <= 0 and not is_btd_signal:
                    logger.debug(f"{sym}: BULL MACD_h={macd_h:.6f} <= 0 (non-BTD) — skip")
                    continue

                # Anti-chasing — désactivé pour buy_the_dip (on veut les rebonds)
                # Actif uniquement pour les signaux BUY classiques (5m=BUY)
                if effective_action == "BUY" and action_5m == "BUY":
                    if len(candles) >= 2:
                        last_c = candles[-1]
                        prev_c = candles[-2]
                        if prev_c.get("close", 0) > 0:
                            last_move = (last_c.get("close",0) - prev_c.get("close",0)) / prev_c.get("close",0) * 100
                            if last_move > 1.2:
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

                # 15m confirmation bloquante — relaxé en RANGE (oscillations normales dans le range)
                if action_5m == "BUY" and action_15m == "SELL" and market_mode != "RANGE":
                    logger.info(f"[{user_id}] {sym}: 5m BUY mais 15m SELL — contre-tendance, skip")
                    continue

                # Anti-pump : éviter les entrées tardives (prix +7% sur 1h)
                # Anti-dump : éviter couperet en chute (> -8% sur 1h) sauf RANGE oversold
                if len(candles) >= 12:
                    price_1h_ago = candles[-12].get("close", 0)
                    if price_1h_ago > 0:
                        move_1h = (candles[-1]["close"] - price_1h_ago) / price_1h_ago * 100
                        if move_1h > 7.0:
                            logger.info(f"[{user_id}] {sym}: pump +{move_1h:.1f}% (1h) — entrée tardive, skip")
                            continue
                        # En RANGE : dump -8% = opportunité de rebond si RSI oversold (< 40)
                        if move_1h < -8.0 and not (market_mode == "RANGE" and rsi < 40):
                            logger.info(f"[{user_id}] {sym}: dump {move_1h:.1f}% (1h) — skip BUY")
                            continue

                # Confirmation bougie fermée au-dessus EMA9 — BUY classiques uniquement
                # Non applicable : buy_the_dip = sous EMA9, RANGE = achat au bas de BB
                if action_5m == "BUY" and market_mode != "RANGE":
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

            # 4h SELL en BULL sans BTD = contre-tendance macro → refuse
            # (n'affecte pas BEAR/NEUTRAL ni les buy-the-dip)
            if action_4h == "SELL" and market_mode == "BULL" and not is_btd_signal:
                logger.info(f"[{user_id}] {sym}: 4h SELL en BULL — contre-tendance macro, skip")
                continue

            # En BULL : exiger au moins une confirmation TF supérieur (15m OU 1h = BUY)
            # Sans confluence → signal 5m isolé = trop risqué
            if market_mode == "BULL" and confluence_mult < 1.1 and not is_btd_signal:
                logger.info(f"[{user_id}] {sym}: BULL sans confluence 15m/1h — skip")
                continue

            # Bonus RSI entrée : RSI bas = plus de place pour monter au TP
            if rsi <= 38:
                rsi_quality = 1.20   # zone oversold = excellent point d'entrée
            elif rsi <= 45:
                rsi_quality = 1.12
            elif rsi <= 52:
                rsi_quality = 1.05
            else:
                rsi_quality = 0.90   # RSI 52-60 = entrée suboptimale, score réduit

            score = round(effective_score * confluence_mult * rsi_quality, 1)

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

        # Phase 2 : essayer les 3 meilleurs candidats — si Claude rejette #1, tenter #2 et #3
        # Corrige le problème : un refus Claude = 5 min gaspillées. Maintenant on essaie les suivants.
        scored.sort(key=lambda x: x["score"], reverse=True)
        pf = portfolio_data or {}

        # NEUTRAL utilise seuils BEAR (cohérent avec run_cycle ligne 538)
        min_conf_seuil  = (MIN_CONFIDENCE_RANGE    if market_mode == "RANGE"
                           else MIN_CONFIDENCE_BEAR  if market_mode in ("BEAR", "NEUTRAL")
                           else MIN_CONFIDENCE_BULL)
        min_score_seuil = (MIN_COMPOSITE_SCORE_RANGE if market_mode == "RANGE"
                           else MIN_COMPOSITE_SCORE_BEAR if market_mode in ("BEAR", "NEUTRAL")
                           else MIN_COMPOSITE_SCORE_BULL)

        for best_candidate in scored[:3]:
            sym        = best_candidate["symbol"]
            indicators = best_candidate["indicators"]
            rule_sig   = best_candidate["rule_sig"]
            price = prices.get(sym, 0)
            if price <= 0:
                try:
                    price = await asyncio.get_event_loop().run_in_executor(
                        None, lambda s=sym: binance_service.get_current_price(s)
                    )
                except Exception:
                    price = 0
            if price <= 0:
                logger.debug(f"[{user_id}] {sym}: prix introuvable — candidat ignoré")
                continue

            tag = "[BUY-THE-DIP]" if best_candidate.get("buy_the_dip") else ("[TRIPLE BULL]" if best_candidate.get("triple_bull") else "")
            logger.info(
                f"[{user_id}] Candidat: {sym} score={best_candidate['score']:.1f} "
                f"5m={best_candidate['action']} 15m={best_candidate['confluence_15m']} "
                f"1h={best_candidate.get('confluence_1h','?')} 4h={best_candidate.get('confluence_4h','?')} {tag}"
            )

            # Données MTF passées à Claude — améliore ses décisions BUY/HOLD
            mtf_data = {
                "15m": best_candidate.get("confluence_15m", "HOLD"),
                "1h":  best_candidate.get("confluence_1h",  "HOLD"),
                "4h":  best_candidate.get("confluence_4h",  "HOLD"),
            }
            try:
                signal_data = await claude_service.analyze_market(sym, indicators, price, pf, mtf_data=mtf_data, market_mode=market_mode)
            except Exception as e:
                logger.warning(f"Claude failed for {sym}: {e} — using rule-based")
                signal_data = claude_service._from_rule(rule_sig, indicators)

            # Override Claude si buy_the_dip ET 1h confirme BUY
            if best_candidate.get("buy_the_dip") and signal_data.get("action") != "BUY":
                rule_score = best_candidate["rule_sig"].get("score", 0)
                confirm_1h = best_candidate.get("confluence_1h") == "BUY"
                if confirm_1h and rule_score >= 10:
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
                f"× bonus={bonus} = {composite:.2f} (seuil={min_score_seuil} [{market_mode}])"
            )

            # Ce candidat passe les seuils → retourner immédiatement
            if (signal_data["action"] == "BUY"
                    and signal_data["confidence"] >= min_conf_seuil
                    and composite >= min_score_seuil):
                return {
                    "symbol":          sym,
                    "signal":          signal_data,
                    "indicators":      indicators,
                    "composite_score": composite,
                    "buy_the_dip":     best_candidate.get("buy_the_dip", False),
                }

            logger.info(
                f"[{user_id}] {sym} rejeté "
                f"(action={signal_data['action']} conf={signal_data['confidence']:.0%} "
                f"composite={composite:.1f} < {min_score_seuil}) — essai candidat suivant"
            )

        return None

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

        bot_info       = self._running_bots.get(user_id, {})
        market_mode    = bot_info.get("market_mode", "NEUTRAL")
        portfolio_data = {}
        try:
            portfolio_data = await self._get_portfolio(db, user_id)
        except Exception:
            pass

        for trade in open_trades:
            sym   = trade.get("symbol", "")
            if not sym:
                continue

            # Positions Futures gérées séparément (SL/TP côté Binance)
            if trade.get("futures"):
                await self._manage_futures_position(user_id, trade, db)
                continue

            price = prices.get(sym)
            if not price:
                continue

            # ── BLOC B : Pyramiding — ajouter sur les gagnants ───────────────
            pnl_pct_check = (price - float(trade.get("price", price))) / float(trade.get("price", price)) * 100 if trade.get("price") else 0
            await self._check_pyramiding(
                user_id, trade, price, pnl_pct_check, db, portfolio_data, market_mode
            )

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

        # ── PARTIAL TP : à +2.0%, ferme 50% et monte SL au breakeven ────────
        # Laisse le momentum s'établir avant de fermer — évite de couper les vrais gagnants
        partial_tp_price    = float(trade.get("partial_tp_price", 0))
        partial_tp_executed = bool(trade.get("partial_tp_executed", False))
        if partial_tp_price > 0 and not partial_tp_executed and price >= partial_tp_price:
            logger.info(
                f"[{user_id}] Partial TP {trade.get('symbol','')}: "
                f"prix={price:.4f} >= cible={partial_tp_price:.4f} (+2.0%) — fermeture 50%, SL→breakeven"
            )
            await self._partial_close_position(user_id, trade, price)
            return

        # Trailing step adaptatif — TP cible 5%, trailing doit laisser respirer la position
        # Avec TP à 5% les micro-retracements 0.3-0.5% sont normaux → steps plus larges
        adx_entry  = float(trade.get("signal_adx", 0) or 0)
        if pnl_pct >= 4.0:
            trail_step = 0.35   # très près du TP 5% : serré pour protéger les gains
        elif pnl_pct >= 3.0:
            trail_step = 0.50   # bon profit : laisse respirer le trend
        elif pnl_pct >= 2.0:
            trail_step = 0.65   # trailing vient d'activer : large marge pour éviter faux trigger
        elif adx_entry > 35:
            trail_step = 0.75   # tendance forte : le prix peut retracer plus sans inverser
        else:
            trail_step = 0.80   # position naissante : hors zone trailing de toute façon

        # Mettre à jour le plus haut
        updates = {}
        if price > highest:
            updates["highest_price_seen"] = price
            highest = price

        # ── BREAKEVEN : dès +1.5%, SL monte à l'entrée + 0.1% (BREAKEVEN_PCT=1.5) ──
        if pnl_pct >= BREAKEVEN_PCT and sl_price < entry:
            new_sl = round(entry * 1.001, 8)
            updates["stop_loss_price"] = new_sl
            sl_price = new_sl
            logger.info(f"[{user_id}] Breakeven {trade['symbol']}: SL → {new_sl:.4f} (+0.1%)")

        # ── TRAILING SL : dès +2.0%, SL suit le plus haut à -trail_step% ────
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

        # ── SORTIE ANTICIPÉE BEAR — lock profit à +0.7% ───────────────────────
        # En BEAR mode le prix revient rarement au TP (1.0%) → fermer à +0.7%
        bot_info = self._running_bots.get(user_id, {})
        if (bot_info.get("market_mode") == "BEAR"
                and pnl_pct >= 0.7
                and price < tp_price):
            logger.info(
                f"[{user_id}] BEAR early exit {trade.get('symbol','')}: "
                f"+{pnl_pct:.2f}% — lock profit avant retournement"
            )
            await self._close_position(user_id, trade, price, "bear_early_exit")
            return

        # ── SORTIE ANTICIPÉE — retournement de signal (avant SL) ─────────────
        # Si 15m + 1h retournent tous les deux SELL pendant qu'on est en perte
        # → sortir tôt à -0.1% plutôt qu'attendre le SL complet à -0.9%
        if pnl_pct < -0.10 and price > sl_price:
            now_ts    = datetime.now(timezone.utc).timestamp()
            last_check = float(trade.get("last_signal_check_ts") or 0)
            if now_ts - last_check >= 120:  # vérif max toutes les 2 min
                try:
                    sym    = trade.get("symbol", "")
                    result = await self._analyze_pair_fast(sym)
                    if result and len(result) >= 4:
                        _, _, rule_15m, rule_1h = result[:4]
                        a15 = rule_15m.get("action", "HOLD")
                        a1h = rule_1h.get("action", "HOLD")
                        if a15 == "SELL" and a1h == "SELL":
                            logger.info(
                                f"[{user_id}] {sym}: retournement 15m+1h SELL "
                                f"pnl={pnl_pct:.2f}% — sortie anticipee"
                            )
                            await self._close_position(user_id, trade, price, "signal_reversal")
                            return
                except Exception as e_rev:
                    logger.debug(f"[{user_id}] Signal reversal check: {e_rev}")
                await db.trades.update_one(
                    {"_id": trade["_id"]},
                    {"$set": {"last_signal_check_ts": now_ts}}
                )

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
    ) -> bool:
        """Place un ordre BUY et sauvegarde le trade."""
        # Garde-fou spread — évite les achats en marché illiquide (mauvais prix d'exécution)
        try:
            spread_pct = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.get_spread_pct(symbol)
            )
            if spread_pct > 0.25:
                logger.warning(f"[{user_id}] {symbol}: spread {spread_pct:.3f}% trop large — achat annulé")
                return False
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
            return False

        tp_price = ex_price * (1 + tp_pct / 100)
        sl_price = ex_price * (1 - sl_pct / 100)

        # Résistance : ne pas abaisser le TP — le trailing SL protège si le prix stagne à la résistance

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
            "partial_tp_price":    0,  # partial TP désactivé — toute la position court jusqu'au vrai TP
            "partial_tp_executed": False,
        })

        trade_id = None
        try:
            res = await db.trades.insert_one(doc)
            trade_id = str(res.inserted_id)
        except Exception as e:
            logger.error(f"[{user_id}] Trade save failed: {e}")
            return False

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
        return True

    # ══════════════════════════════════════════════════════════════════════════
    # FUTURES — AGGRESSIVE BULL MODE (BLOC A)
    # ══════════════════════════════════════════════════════════════════════════

    def _is_aggressive_bull(
        self,
        market_mode: str,
        confidence: float,
        adx: float,
        triple_bull: bool,
    ) -> bool:
        """Détecte le mode Aggressive Bull : BULL fort + haute conviction + levier justifié."""
        return (
            FUTURES_ENABLED
            and market_mode == "BULL"
            and confidence >= AGGRESSIVE_BULL_MIN_CONF
            and adx >= AGGRESSIVE_BULL_MIN_ADX
            and triple_bull  # 5m+15m+1h tous BUY
        )

    async def _execute_futures_long(
        self,
        user_id: str,
        db,
        symbol: str,
        signal_data: Dict,
        current_price: float,
        indicators: Dict,
    ) -> bool:
        """
        Ouvre un LONG Futures avec levier en Aggressive Bull Mode.
        TP=5%, SL=1%, 10x levier, position 65% du capital Futures.
        """
        try:
            # Une seule position Futures à la fois — évite le sur-levier
            open_futures = await db.trades.find(
                {"user_id": user_id, "status": "OPEN", "futures": True}
            ).to_list(5)
            if open_futures:
                logger.info(
                    f"[{user_id}] Futures deja ouvert sur "
                    f"{open_futures[0].get('symbol', '?')} — pas de 2e position"
                )
                return False

            # Solde Futures disponible
            futures_balance = await asyncio.get_event_loop().run_in_executor(
                None, futures_service.get_futures_balance
            )
            if futures_balance < FUTURES_BALANCE_RESERVE + 1.0:
                logger.warning(f"[{user_id}] Futures balance insuffisant: {futures_balance:.2f} USDT")
                return False

            usable = max(futures_balance - FUTURES_BALANCE_RESERVE, 0)
            # Position = 65% du capital Futures disponible
            notional = usable * AGGRESSIVE_BULL_POSITION_PCT * FUTURES_LEVERAGE
            if notional < 5.0:
                logger.warning(f"[{user_id}] Futures notional trop faible: {notional:.2f} USDT")
                return False

            tp_price = round(current_price * (1 + AGGRESSIVE_BULL_TP_PCT / 100), 8)
            sl_price = round(current_price * (1 - AGGRESSIVE_BULL_SL_PCT / 100), 8)

            logger.info(
                f"[{user_id}] 🚀 AGGRESSIVE BULL FUTURES {symbol}: "
                f"notional={notional:.2f} USDT levier={FUTURES_LEVERAGE}x "
                f"TP={AGGRESSIVE_BULL_TP_PCT}% SL={AGGRESSIVE_BULL_SL_PCT}%"
            )

            order = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: futures_service.open_long(symbol, notional, current_price, FUTURES_LEVERAGE)
            )
            if not order:
                return False

            ex_price = order["price"]
            ex_qty   = order["qty"]
            tp_final = round(ex_price * (1 + AGGRESSIVE_BULL_TP_PCT / 100), 4)
            sl_final = round(ex_price * (1 - AGGRESSIVE_BULL_SL_PCT / 100), 4)

            # Placer SL + TP sur Binance Futures (ordres serveur)
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: futures_service.set_stop_loss_tp(symbol, ex_qty, sl_final, tp_final)
            )

            # Sauvegarder en MongoDB comme trade classique (tag futures=True)
            margin_used = round(notional / FUTURES_LEVERAGE, 4)
            now_utc = datetime.now(timezone.utc)
            doc = {
                "user_id":        user_id,
                "symbol":         symbol,
                "side":           "BUY",
                "quantity":       ex_qty,
                "price":          ex_price,
                "total_usdt":     margin_used,
                "status":         "OPEN",
                "futures":        True,
                "leverage":       FUTURES_LEVERAGE,
                "notional":       round(ex_qty * ex_price, 4),
                "take_profit_price": tp_final,
                "stop_loss_price":   sl_final,
                "signal_confidence": signal_data.get("confidence", 0),
                "binance_order_id":  order["order_id"],
                "created_at":     now_utc,
                "opened_at":      now_utc,
                "partial_tp_price":    0,
                "partial_tp_executed": False,
            }
            await db.trades.insert_one(doc)

            gain_potentiel = round(ex_qty * ex_price * AGGRESSIVE_BULL_TP_PCT / 100, 2)
            logger.info(
                f"[{user_id}] ✅ Futures LONG ouvert {symbol} @ {ex_price:.4f} "
                f"levier={FUTURES_LEVERAGE}x marge={margin_used:.2f} USDT "
                f"gain potentiel=+${gain_potentiel}"
            )
            return True

        except Exception as e:
            logger.error(f"[{user_id}] _execute_futures_long {symbol}: {e}")
            return False

    async def _manage_futures_position(self, user_id: str, trade: Dict, db) -> None:
        """
        Gère une position Futures ouverte.
        Binance ferme automatiquement via SL/TP serveur.
        On détecte la clôture en vérifiant si la position existe encore sur Binance.
        """
        symbol = trade.get("symbol", "")
        try:
            position = await asyncio.get_event_loop().run_in_executor(
                None, lambda: futures_service.get_futures_position(symbol)
            )

            if position is None:
                # Erreur API Binance — état inconnu, ne pas modifier MongoDB
                logger.debug(f"[{user_id}] Futures API error {symbol} — skip")
                return

            if position:
                # Position toujours ouverte — SL/TP serveur Binance actifs, rien à faire
                return

            # position == {} : position fermée sur Binance (SL ou TP touché)
            entry     = float(trade.get("price", 0))
            notional  = float(trade.get("notional", 0))
            margin    = float(trade.get("total_usdt", 0))
            leverage  = int(trade.get("leverage", FUTURES_LEVERAGE))
            tp_stored = float(trade.get("take_profit_price", 0))
            sl_stored = float(trade.get("stop_loss_price", 0))

            # Récupérer le fill réel depuis Binance (prix exact + PnL réalisé)
            opened_ts = trade.get("opened_at") or trade.get("created_at")
            opened_ts_unix = opened_ts.timestamp() if opened_ts else 0.0

            fill = await asyncio.get_event_loop().run_in_executor(
                None, lambda: futures_service.get_last_close_fill(symbol, opened_ts_unix)
            )

            if fill and fill["price"] > 0:
                close_price_used = fill["price"]
                pnl_approx       = round(fill["realized_pnl"], 4)
                pnl_pct          = round(pnl_approx / margin * 100, 4) if margin > 0 else 0
                close_reason     = "futures_tp" if pnl_approx >= 0 else "futures_sl"
                logger.debug(f"[{user_id}] Futures fill exact {symbol}: prix={close_price_used} PnL={pnl_approx}")
            else:
                # Fallback : estimation via mark_price si l'API fill échoue
                mark_price = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: futures_service.get_mark_price(symbol)
                )
                if tp_stored > 0 and mark_price >= tp_stored * 0.98:
                    close_price_used = tp_stored
                    close_reason     = "futures_tp"
                elif sl_stored > 0 and mark_price <= sl_stored * 1.02:
                    close_price_used = sl_stored
                    close_reason     = "futures_sl"
                else:
                    close_price_used = mark_price
                    close_reason     = "futures_tp" if mark_price > entry else "futures_sl"
                pnl_approx = round((close_price_used - entry) / entry * notional, 4) if entry > 0 else 0
                pnl_pct    = round(pnl_approx / margin * 100, 4) if margin > 0 else 0

            await db.trades.update_one(
                {"_id": trade["_id"]},
                {"$set": {
                    "status":       "CLOSED",
                    "pnl":          pnl_approx,
                    "pnl_pct":      pnl_pct,
                    "close_price":  close_price_used,
                    "close_reason": close_reason,
                    "closed_at":    datetime.now(timezone.utc),
                }}
            )
            sign = "+" if pnl_approx >= 0 else ""
            logger.info(
                f"[{user_id}] Futures {symbol} fermée ({close_reason}) "
                f"PnL={sign}{pnl_approx:.4f} USDT ({sign}{pnl_pct:.1f}%) "
                f"levier={leverage}x"
            )

        except Exception as e:
            logger.debug(f"[{user_id}] _manage_futures_position {symbol}: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    # PYRAMIDING ENGINE (BLOC B)
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_pyramiding(
        self,
        user_id: str,
        trade: Dict,
        price: float,
        pnl_pct: float,
        db,
        portfolio_data: Dict,
        market_mode: str,
    ) -> None:
        """
        Si position en profit >= 1.5% et signal 15m toujours BUY :
        ouvre une 2e position (25% capital restant) sur la même paire.
        Multiplie le gain si le trend continue.
        """
        # Conditions pyramiding
        if pnl_pct < 1.5:
            return
        if trade.get("pyramided"):
            return  # déjà pyramidé
        if market_mode != "BULL":
            return  # pyramiding uniquement en BULL

        bot_info = self._running_bots.get(user_id, {})
        available = portfolio_data.get("available_usdt", 0)
        if available < 11.0:
            return  # pas assez de capital pour une 2e position ($5.5 min + marge)

        symbol = trade.get("symbol", "")
        try:
            result = await self._analyze_pair_fast(symbol)
            if not result or len(result) < 3:
                return
            _, _, rule_15m = result[0], result[1], result[2]
            if rule_15m.get("action") != "BUY":
                logger.debug(f"[{user_id}] Pyramiding {symbol}: 15m pas BUY — skip")
                return

            # Taille de la 2e position : 25% du capital restant
            pyramid_usdt = min(available * 0.25, 30.0)
            pyramid_usdt = max(pyramid_usdt, 5.50)

            sl_pct = 0.8
            tp_pct = float(trade.get("take_profit_price", 0))
            if tp_pct > 0 and price > 0:
                tp_pct = (tp_pct - price) / price * 100
            else:
                tp_pct = 3.0

            logger.info(
                f"[{user_id}] 🔺 PYRAMIDING {symbol}: pnl={pnl_pct:.1f}% "
                f"15m=BUY → 2e position {pyramid_usdt:.1f} USDT"
            )

            # Ouvrir 2e position avec tag pyramid=True
            config = bot_info.get("config", {})
            ok = await self._execute_buy(
                user_id, db, symbol, pyramid_usdt, price,
                sl_pct, tp_pct,
                {"action": "BUY", "confidence": 0.80, "source": "pyramid"},
                portfolio_data, config,
                indicators=None,
            )
            if ok:
                # Marquer la position originale comme pyramidée
                await db.trades.update_one(
                    {"_id": trade["_id"]}, {"$set": {"pyramided": True}}
                )
                bot_info["daily_trade_count"] = bot_info.get("daily_trade_count", 0) + 1
                logger.info(f"[{user_id}] ✅ Pyramide ouverte sur {symbol}")

        except Exception as e:
            logger.debug(f"[{user_id}] Pyramiding check {symbol}: {e}")

    async def _partial_close_position(
        self, user_id: str, trade: dict, close_price: float, fraction: float = 0.50
    ) -> None:
        """Ferme 50% de la position au TP partiel — protège le capital, laisse le reste courir."""
        db     = get_database()
        symbol = trade.get("symbol", "")
        entry  = float(trade.get("price", 0))
        cost   = float(trade.get("total_usdt", 0))
        qty    = float(trade.get("quantity", 0))

        # ── Lock atomique — empêche double-exécution si monitor + cycle tournent en même temps ──
        lock_result = await db.trades.update_one(
            {"_id": trade["_id"], "partial_tp_executed": False},
            {"$set": {"partial_tp_executed": True}},
        )
        if lock_result.modified_count == 0:
            logger.info(f"[{user_id}] Partial TP {symbol} déjà verrouillé — skip concurrent")
            return

        try:
            base = symbol.replace("USDT", "").replace("BUSD", "")
            bals = await asyncio.get_event_loop().run_in_executor(
                None, binance_service.get_account_balance
            )
            avail = float(bals.get(base, {}).get("free", 0.0))
            if avail <= 0:
                logger.warning(f"[{user_id}] Pas de {base} pour partial TP — skip")
                # Rollback le lock si pas de balance
                await db.trades.update_one(
                    {"_id": trade["_id"]}, {"$set": {"partial_tp_executed": False}}
                )
                return
            from decimal import Decimal, ROUND_DOWN
            info      = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.get_symbol_info(symbol)
            )
            step      = Decimal(info["step_size"])
            # Limiter à la quantité du trade (pas tout le solde wallet si pyramidé)
            trade_qty  = float(trade.get("quantity", avail))
            to_sell    = min(avail, trade_qty) * fraction
            sell_qty   = float((Decimal(str(to_sell)) // step) * step)
            if sell_qty <= 0:
                await db.trades.update_one(
                    {"_id": trade["_id"]}, {"$set": {"partial_tp_executed": False}}
                )
                return
        except Exception as e:
            logger.error(f"[{user_id}] Partial TP balance check {symbol}: {e}")
            await db.trades.update_one(
                {"_id": trade["_id"]}, {"$set": {"partial_tp_executed": False}}
            )
            return

        try:
            order      = await asyncio.get_event_loop().run_in_executor(
                None, lambda q=sell_qty: binance_service.place_market_order(symbol, "SELL", q)
            )
            fills      = order.get("fills", [])
            sell_price = float(fills[0]["price"]) if fills else close_price
            gross      = sell_qty * sell_price
            fee        = gross * BINANCE_FEE
            net        = gross - fee
            partial_cost    = cost * fraction
            partial_pnl     = net - partial_cost
            partial_pnl_pct = (partial_pnl / partial_cost * 100) if partial_cost > 0 else 0

            # SL : garde le meilleur entre breakeven et le trailing SL existant
            # (si trailing déjà à +1.14%, ne pas le dégrader à +0.1%)
            breakeven_sl = round(entry * 1.001, 8)
            current_sl   = float(trade.get("stop_loss_price", 0) or 0)
            new_sl       = max(breakeven_sl, current_sl)  # toujours le plus haut des deux

            # partial_tp_executed est déjà True (posé par le lock atomique ci-dessus)
            update_fields = {
                "partial_tp_price_executed":  sell_price,
                "partial_pnl":                round(partial_pnl, 6),
                "total_usdt":                 round(cost * (1 - fraction), 6),
                "quantity":                   round(qty * (1 - fraction), 8),
                "stop_loss_price":            new_sl,
            }
            await db.trades.update_one(
                {"_id": trade["_id"]},
                {"$set": update_fields},
            )

            sl_label = "breakeven" if new_sl == breakeven_sl else "trailing"
            logger.info(
                f"[{user_id}] 💰 PARTIAL TP {symbol} @ {sell_price:.4f} "
                f"PnL={partial_pnl:+.4f} USDT ({partial_pnl_pct:+.2f}%) "
                f"SL → {sl_label} {new_sl:.4f}"
            )

            await notification_service.send(user_id, "trade_executed", {
                "symbol": symbol, "side": "PARTIAL_SELL",
                "price": sell_price, "pnl": partial_pnl,
                "pnl_pct": partial_pnl_pct, "reason": "partial_take_profit",
            })

        except Exception as e:
            logger.error(f"[{user_id}] Partial close {symbol} failed: {e}")
            # Rollback le lock si le sell a échoué
            await db.trades.update_one(
                {"_id": trade["_id"]}, {"$set": {"partial_tp_executed": False}}
            )

    async def _close_position(
        self, user_id: str, trade: dict, close_price: float, reason: str
    ) -> None:
        """Ferme une position avec le vrai solde Binance."""
        db     = get_database()
        symbol = trade.get("symbol", "")
        if not symbol:
            logger.error(f"[{user_id}] _close_position: symbole manquant dans le trade")
            return
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

            # Précision Binance — vendre au max la quantité enregistrée dans le trade
            # (évite de fermer une position pyramidée en même temps)
            from decimal import Decimal, ROUND_DOWN
            info    = await asyncio.get_event_loop().run_in_executor(
                None, lambda: binance_service.get_symbol_info(symbol)
            )
            step     = Decimal(info["step_size"])
            trade_qty = float(trade.get("quantity", avail))
            to_sell   = min(avail, trade_qty)
            sell_qty  = float((Decimal(str(to_sell)) // step) * step)
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
            # Additionner le gain du TP partiel (50% déjà encaissé) si applicable
            partial_pnl = float(trade.get("partial_pnl", 0) or 0)
            total_pnl   = round(pnl + partial_pnl, 6)
            orig_cost   = cost / (1 - 0.50) if trade.get("partial_tp_executed") else cost
            pnl_pct     = (total_pnl / orig_cost * 100) if orig_cost > 0 else 0

            await db.trades.update_one(
                {"_id": trade["_id"]},
                {"$set": {
                    "status": "CLOSED", "pnl": total_pnl,
                    "pnl_pct": round(pnl_pct, 4),
                    "closed_at": datetime.now(timezone.utc),
                    "close_price": sell_price, "close_reason": reason,
                }},
            )

            emoji = "✅" if total_pnl > 0 else "❌"
            partial_tag = f" (+{partial_pnl:.4f} partial)" if partial_pnl != 0 else ""
            logger.info(
                f"[{user_id}] {emoji} CLOSE {symbol} @ {sell_price:.4f} "
                f"PnL={total_pnl:+.4f} USDT ({pnl_pct:+.2f}%){partial_tag} [{reason}]"
            )

            await notification_service.send(user_id, "trade_executed", {
                "symbol": symbol, "side": "SELL",
                "price": sell_price, "pnl": total_pnl, "pnl_pct": pnl_pct, "reason": reason,
            })
            await self._broadcast(user_id, "trade_executed", {
                "symbol": symbol, "side": "SELL",
                "price": sell_price, "pnl": total_pnl, "pnl_pct": pnl_pct, "reason": reason,
            })

            bot_info = self._running_bots.get(user_id, {})
            bot_info["consecutive_losses"] = (
                bot_info.get("consecutive_losses", 0) + 1 if total_pnl < 0 else 0
            )

            # Cooldown adaptatif par paire : 45min → 3h → 8h selon pertes consécutives
            if reason in ("stop_loss", "signal_reversal"):
                if "sl_cooldown" not in bot_info:
                    bot_info["sl_cooldown"] = {}
                if "pair_losses" not in bot_info:
                    bot_info["pair_losses"] = {}
                pair_count = bot_info["pair_losses"].get(symbol, 0) + 1
                bot_info["pair_losses"][symbol] = pair_count
                secs = 8 * 3600 if pair_count >= 3 else (3 * 3600 if pair_count >= 2 else 45 * 60)
                bot_info["sl_cooldown"][symbol] = datetime.now(timezone.utc) + timedelta(seconds=secs)
                logger.info(f"[{user_id}] {symbol}: cooldown {secs//60}min ({pair_count} SL consecutifs)")
            elif total_pnl > 0:
                # Victoire sur cette paire → reset son compteur de pertes
                if "pair_losses" in bot_info:
                    bot_info["pair_losses"].pop(symbol, None)

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
                "vol_ratio": indicators.get("volume", {}).get("vol_ratio"),
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
    def _get_max_positions(total_usdt: float) -> int:
        """Nombre max de positions simultanées — 1 position par tranche de 12$ de capital total."""
        MIN_PAR_POSITION = 12.0
        MAX_POSITIONS    = 6
        return max(1, min(MAX_POSITIONS, int(total_usdt // MIN_PAR_POSITION)))

    async def _broadcast(self, user_id: str, message_type: str, data: Dict[str, Any]) -> None:
        from routers.websocket import send_update
        try:
            await send_update(user_id, message_type, data)
        except Exception:
            pass


bot_engine = BotEngine()
