# NOUVEAU FICHIER — à créer
"""
Service de notifications en temps réel via WebSocket + rapport quotidien APScheduler.
"""
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from utils.logger import get_logger

logger = get_logger(__name__)

NOTIFICATION_TYPES = {
    "trade_executed":       "Trade exécuté",
    "stop_loss_triggered":  "Stop-loss déclenché",
    "take_profit_reached":  "Take-profit atteint",
    "circuit_breaker":      "Circuit breaker activé",
    "high_confidence_signal": "Signal haute confiance",
    "daily_report":         "Rapport quotidien",
    "trailing_stop":        "Trailing stop déclenché",
}

NOTIFICATION_COLORS = {
    "trade_executed":       "#2962ff",
    "stop_loss_triggered":  "#ef5350",
    "take_profit_reached":  "#26a69a",
    "circuit_breaker":      "#ff9800",
    "high_confidence_signal": "#26a69a",
    "daily_report":         "#2962ff",
    "trailing_stop":        "#ff9800",
}


class NotificationService:
    """
    Envoie des notifications via WebSocket.
    Stocke en DB pour l'historique.
    Peut envoyer un rapport quotidien via APScheduler.
    """

    async def send(
        self,
        user_id: str,
        event_type: str,
        data: Dict[str, Any],
    ) -> None:
        """Envoie une notification via WebSocket ET la persiste en DB."""
        try:
            from routers.websocket import send_update
            from database import get_database

            notification = {
                "type":       event_type,
                "title":      NOTIFICATION_TYPES.get(event_type, event_type),
                "color":      NOTIFICATION_COLORS.get(event_type, "#787b86"),
                "data":       data,
                "created_at": datetime.now(timezone.utc),
                "read":       False,
                "user_id":    user_id,
            }

            # Persister en DB
            try:
                db = get_database()
                await db.notifications.insert_one({**notification})
            except Exception as e:
                logger.warning(f"Notification persist failed: {e}")

            # Envoyer via WebSocket
            await send_update(user_id, "notification", {
                "event_type": event_type,
                "title":      notification["title"],
                "color":      notification["color"],
                "data":       data,
                "timestamp":  notification["created_at"].isoformat(),
            })

        except Exception as e:
            logger.warning(f"Notification send failed ({event_type}): {e}")

    async def get_recent(self, user_id: str, limit: int = 20) -> list:
        try:
            from database import get_database
            db = get_database()
            docs = await db.notifications.find(
                {"user_id": user_id}
            ).sort("created_at", -1).limit(limit).to_list(length=limit)
            for d in docs:
                d["id"] = str(d.pop("_id"))
                if "created_at" in d:
                    d["created_at"] = d["created_at"].isoformat()
            return docs
        except Exception as e:
            logger.warning(f"Get notifications failed: {e}")
            return []

    async def mark_read(self, user_id: str, notification_id: Optional[str] = None) -> None:
        try:
            from database import get_database
            from bson import ObjectId
            db = get_database()
            query = {"user_id": user_id}
            if notification_id:
                query["_id"] = ObjectId(notification_id)
            await db.notifications.update_many(query, {"$set": {"read": True}})
        except Exception as e:
            logger.warning(f"Mark read failed: {e}")

    async def send_daily_report(self, user_id: str) -> None:
        """
        Rapport quotidien généré automatiquement à 23h00.
        Calcule les stats du jour et les envoie via WebSocket.
        """
        try:
            from database import get_database
            db = get_database()

            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            trades_today = await db.trades.find(
                {"user_id": user_id, "created_at": {"$gte": today_start}}
            ).to_list(length=200)

            portfolio = await db.portfolio_history.find_one(
                {"user_id": user_id}, sort=[("recorded_at", -1)]
            )

            closed_today = [t for t in trades_today if t.get("status") == "CLOSED"]
            pnl_today    = sum(t.get("pnl", 0.0) or 0.0 for t in closed_today)
            wins_today   = sum(1 for t in closed_today if (t.get("pnl") or 0.0) > 0)
            signals_today = await db.signals.count_documents(
                {"created_at": {"$gte": today_start}}
            )

            report_data = {
                "date":            today_start.strftime("%Y-%m-%d"),
                "trades_count":    len(trades_today),
                "closed_count":    len(closed_today),
                "pnl_today":       round(pnl_today, 2),
                "wins_today":      wins_today,
                "signals_analyzed":signals_today,
                "portfolio_total": portfolio.get("total_usdt", 0.0) if portfolio else 0.0,
                "portfolio_pnl_pct": portfolio.get("total_pnl_pct", 0.0) if portfolio else 0.0,
            }

            await self.send(user_id, "daily_report", report_data)
            logger.info(f"Daily report sent to user {user_id}: {report_data}")

        except Exception as e:
            logger.error(f"Daily report failed for {user_id}: {e}")

    def schedule_daily_reports(self, scheduler, user_id: str) -> None:
        """Planifie le rapport quotidien à 23h00."""
        from apscheduler.triggers.cron import CronTrigger
        try:
            scheduler.add_job(
                self.send_daily_report,
                trigger=CronTrigger(hour=23, minute=0),
                id=f"daily_report_{user_id}",
                args=[user_id],
                replace_existing=True,
            )
            logger.info(f"Daily report scheduled for user {user_id}")
        except Exception as e:
            logger.warning(f"Daily report scheduling failed: {e}")


notification_service = NotificationService()
