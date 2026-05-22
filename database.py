# FICHIER MODIFIÉ — remplace l'ancien
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_client: AsyncIOMotorClient = None
_db: AsyncIOMotorDatabase = None


async def connect_db():
    global _client, _db
    try:
        _client = AsyncIOMotorClient(
            settings.MONGODB_URL,
            tls=True,
            tlsAllowInvalidCertificates=False,
            serverSelectionTimeoutMS=30000,
        )
        _db = _client[settings.MONGODB_DB_NAME]
        await _client.admin.command("ping")
        await _create_indexes()
        logger.info("Connected to MongoDB Atlas successfully")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


async def disconnect_db():
    global _client
    if _client:
        _client.close()
        logger.info("Disconnected from MongoDB")


def get_database() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("Database not initialized. Call connect_db() first.")
    return _db


async def _create_indexes():
    db = _db
    try:
        # ── Users ───────────────────────────────────────────────────────────
        await db.users.create_index([("email", ASCENDING)], unique=True)

        # ── Trades — requêtes fréquentes par user + date + status ──────────
        await db.trades.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
        await db.trades.create_index([("user_id", ASCENDING), ("created_at", DESCENDING), ("status", ASCENDING)])
        await db.trades.create_index([("user_id", ASCENDING), ("status", ASCENDING)])
        await db.trades.create_index([("user_id", ASCENDING), ("symbol", ASCENDING), ("status", ASCENDING)])

        # ── Signals — requêtes par date + action ───────────────────────────
        await db.signals.create_index([("created_at", DESCENDING)])
        await db.signals.create_index([("created_at", DESCENDING), ("action", ASCENDING)])
        await db.signals.create_index([("symbol", ASCENDING), ("created_at", DESCENDING)])

        # ── Portfolio history — requêtes par user + date ───────────────────
        await db.portfolio_history.create_index([("user_id", ASCENDING), ("recorded_at", DESCENDING)])

        # ── Backtests ──────────────────────────────────────────────────────
        await db.backtests.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
        await db.backtests.create_index([("symbol", ASCENDING), ("interval", ASCENDING)])

        # ── Notifications ─────────────────────────────────────────────────
        await db.notifications.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
        await db.notifications.create_index([("user_id", ASCENDING), ("read", ASCENDING)])

        # ── Bot config ────────────────────────────────────────────────────
        await db.bot_config.create_index([("user_id", ASCENDING)], unique=True)

        logger.info("Database indexes created successfully")
    except Exception as e:
        logger.warning(f"Index creation warning: {e}")
