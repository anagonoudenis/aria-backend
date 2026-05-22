import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
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

# Paires scannées à chaque cycle — le bot choisit automatiquement la meilleure
SCAN_PAIRS = ["BNBUSDT", "SOLUSDT", "ETHUSDT", "BTCUSDT"]

# Gestion des positions
TRAIL_TRIGGER_PCT  = 3.0    # déclencher le trailing TP quand +3%
TRAIL_STEP_PCT     = 1.5    # trailing step de 1.5%
BREAKEVEN_PCT      = 2.0    # breakeven quand +2%
STOP_LOSS_PCT      = 2.0
TAKE_PROFIT_PCT    = 6.0

# Scalping rapide
SCALP_CONFIDENCE   = 0.85   # seuil pour scalping 1m
SCALP_SL_PCT       = 0.5
SCALP_TP_PCT       = 1.5

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
            "scan_results": {},  # dernier scan multi-paires
        }

        db = get_database()
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": {"bot_config.is_running": True}})
        logger.info(f"Bot started: user={user_id} interval={interval} scanning={SCAN_PAIRS}")
        asyncio.create_task(self._bot_loop(user_id, interval_seconds))

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
            logger.warning(f"[{user_id}] Circuit breaker — skipping")
            return

        # Déterminer les paires à scanner
        config_symbol = config.get("symbol", "BNBUSDT")
        scan_symbols  = list({config_symbol} | set(SCAN_PAIRS))

        logger.info(f"[{user_id}] Cycle start — scanning {len(scan_symbols)} pairs: {scan_symbols}")

        # ── 1. Gérer les positions ouvertes AVANT d'ouvrir de nouvelles ─────
        await self._manage_all_positions(user_id, scan_symbols)

        # ── 2. Prix courants en parallèle ────────────────────────────────────
        prices = await self._fetch_prices(scan_symbols)

        # ── 3. Portefeuille ──────────────────────────────────────────────────
        db = get_database()
        portfolio_data = await self._get_portfolio(db, user_id)

        if portfolio_data["available_usdt"] < 5.0:
            logger.info(f"[{user_id}] Capital insuffisant ({portfolio_data['available_usdt']:.2f}) — pas de nouveau trade")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 4. Historique (circuit breaker) ──────────────────────────────────
        recent_trades = await db.trades.find(
            {"user_id": user_id, "status": "CLOSED"}
        ).sort("created_at", -1).limit(50).to_list(50)

        consecutive = risk_manager.count_consecutive_losses(recent_trades)
        bot_info["consecutive_losses"] = consecutive

        if consecutive >= 3:
            reason = f"Circuit breaker: {consecutive} pertes"
            bot_info["circuit_breaker_active"] = True
            bot_info["circuit_breaker_reason"]  = reason
            await notification_service.send(user_id, "circuit_breaker", {"reason": reason})
            logger.warning(f"[{user_id}] {reason}")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 5. Positions déjà ouvertes ────────────────────────────────────────
        open_trades = await db.trades.find(
            {"user_id": user_id, "status": "OPEN"}
        ).to_list(20)
        open_symbols = {t.get("symbol") for t in open_trades}
        max_positions = config.get("max_open_trades", 2)

        if len(open_trades) >= max_positions:
            logger.info(f"[{user_id}] Max positions atteint ({len(open_trades)}/{max_positions})")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 6. SCAN MULTI-PAIRES — cherche la meilleure opportunité ──────────
        best = await self._scan_best_opportunity(
            user_id, scan_symbols, open_symbols, prices, config, portfolio_data
        )

        if best is None:
            logger.info(f"[{user_id}] Scan échoué — pas de données")
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

        # ── 8. Validation + exécution ─────────────────────────────────────────
        if signal_data["action"] != "BUY":
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        is_valid, reason = risk_manager.validate_trade(
            signal_data, portfolio_data, config,
            len(open_trades), list(open_symbols), consecutive, indicators,
        )

        if not is_valid:
            logger.info(f"[{user_id}] Trade refusé: {reason}")
            bot_info["cycles_count"] += 1
            bot_info["last_cycle_at"] = datetime.now(timezone.utc)
            return

        # ── 9. Déterminer SL/TP selon confiance ──────────────────────────────
        conf = signal_data["confidence"]
        if conf >= SCALP_CONFIDENCE:
            # Mode scalping rapide
            sl_pct = SCALP_SL_PCT
            tp_pct = SCALP_TP_PCT
            logger.info(f"[{user_id}] Mode SCALPING ({conf:.0%}): SL={sl_pct}% TP={tp_pct}%")
        else:
            sl_pct = signal_data.get("suggested_stop_loss_pct",   STOP_LOSS_PCT)
            tp_pct = signal_data.get("suggested_take_profit_pct", TAKE_PROFIT_PCT)

        # Taille de position
        position_usdt = risk_manager.calculate_position_size(
            portfolio_data["available_usdt"],
            config.get("risk_per_trade_pct", 90),
            current_price, sl_pct,
        )

        # Bonus de taille sur signal fort (mais ne dépasse jamais 80% du dispo)
        if conf >= 0.80:
            max_allowed = portfolio_data["available_usdt"] * 0.80
            bonus = min(position_usdt * 0.2, max_allowed - position_usdt)
            if bonus > 0:
                position_usdt = min(position_usdt + bonus, max_allowed)
                logger.info(f"[{user_id}] Bonus taille +20% sur signal fort ({conf:.0%})")

        await self._execute_buy(
            user_id, db, symbol, position_usdt, current_price,
            sl_pct, tp_pct, signal_data, portfolio_data, config
        )

        bot_info["cycles_count"] += 1
        bot_info["last_cycle_at"] = datetime.now(timezone.utc)

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
    ) -> Optional[Dict[str, Any]]:
        """
        Analyse toutes les paires en parallèle et retourne la meilleure opportunité.
        Utilise d'abord le score rule-based pour filtrer, puis Claude sur le gagnant.
        """
        interval = config.get("interval", "5m")

        # Filtrer les paires déjà en position
        candidates = [s for s in symbols if s not in open_symbols]
        if not candidates:
            return None

        # Phase 1 : score rule-based rapide sur toutes les paires (parallèle)
        tasks = [self._analyze_pair_fast(s, interval) for s in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        scored = []
        for sym, result in zip(candidates, results):
            if isinstance(result, Exception):
                logger.debug(f"Scan {sym} error: {result}")
                continue
            if result is None:
                continue
            indicators, rule_sig = result
            score  = rule_sig.get("score", 0)
            action = rule_sig.get("action", "HOLD")
            # Pré-filtre très léger : score >= 1 suffit pour passer à Claude
            # On laisse Claude décider si c'est vraiment une opportunité
            scored.append({
                "symbol": sym, "score": score, "action": action,
                "indicators": indicators, "rule_sig": rule_sig,
            })

        if not scored:
            return None

        # Trier par score décroissant — prendre le meilleur TOUJOURS
        # On envoie toujours le meilleur candidat à Claude même si HOLD
        # Claude peut changer HOLD → BUY si les conditions globales sont bonnes

        # Phase 2 : trier et prendre le meilleur, appeler Claude dessus
        scored.sort(key=lambda x: x["score"], reverse=True)
        best_candidate = scored[0]
        sym       = best_candidate["symbol"]
        indicators= best_candidate["indicators"]
        rule_sig  = best_candidate["rule_sig"]
        price     = prices.get(sym, 0)

        # Utilise le portfolio déjà fetché — évite un 2e appel Binance
        pf = portfolio_data or {}

        try:
            signal_data = await claude_service.analyze_market(
                sym, indicators, price, pf
            )
        except Exception as e:
            logger.warning(f"Claude failed for {sym}: {e} — using rule-based")
            signal_data = claude_service._from_rule_signal(rule_sig, indicators)

        # Score composite : confiance × score
        composite = signal_data["confidence"] * 10 + best_candidate["score"]

        # Toujours retourner le meilleur signal — run_cycle décide quoi faire
        return {
            "symbol":          sym,
            "signal":          signal_data,
            "indicators":      indicators,
            "composite_score": composite,
        }

    async def _analyze_pair_fast(
        self, symbol: str, interval: str
    ) -> Optional[Tuple[Dict, Dict]]:
        """Analyse rapide d'une paire : klines + indicateurs + rule-based."""
        try:
            kline_key = f"klines:{symbol}:{interval}"
            df = cache.get(kline_key)
            if df is None:
                df = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: binance_service.get_klines(symbol, interval, limit=100)
                )
                cache.set(kline_key, df, ttl_seconds=KLINE_TTL.get(interval, 300))

            ind_key = f"indicators:{symbol}:{interval}"
            indicators = cache.get(ind_key)
            if indicators is None:
                indicators = analysis_service.compute_indicators(df)
                cache.set(ind_key, indicators, ttl_seconds=INDICATOR_TTL.get(interval, 300))

            rule_sig = indicators.get("rule_signal", {})
            return indicators, rule_sig
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
            logger.info(f"[{user_id}] Breakeven {trade['symbol']}: SL → ${new_sl:.4f}")

        # ── TRAILING TAKE-PROFIT : quand +3%, le TP suit le prix ─────────────
        if pnl_pct >= TRAIL_TRIGGER_PCT:
            new_trailing_tp = price * (1 + TRAIL_STEP_PCT / 100)
            if new_trailing_tp > trailing_tp:
                updates["trailing_tp_price"] = new_trailing_tp
                trailing_tp = new_trailing_tp
                logger.info(
                    f"[{user_id}] Trailing TP {trade['symbol']}: "
                    f"+{pnl_pct:.1f}% → TP monté à ${new_trailing_tp:.4f}"
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

    async def _broadcast(self, user_id: str, message_type: str, data: Dict[str, Any]) -> None:
        from routers.websocket import send_update
        try:
            await send_update(user_id, message_type, data)
        except Exception:
            pass


bot_engine = BotEngine()
