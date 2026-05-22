from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from bson import ObjectId

from database import get_database
from models.user import UserInDB, BotConfig
from routers.auth import get_current_user
from services.bot_engine import bot_engine
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/bot", tags=["Bot"])


class BotConfigUpdate(BaseModel):
    symbol: Optional[str] = None
    interval: Optional[str] = None
    risk_per_trade_pct: Optional[float] = None
    max_open_trades: Optional[int] = None
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None


class BotStatusResponse(BaseModel):
    is_running: bool
    symbol: str
    interval: str
    last_cycle_at: Optional[datetime] = None
    cycles_count: int = 0


VALID_SYMBOLS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT", "DOGEUSDT"}
VALID_INTERVALS = {"1m", "5m", "15m", "1h", "4h"}


@router.post("/start")
async def start_bot(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    user_id = current_user.id
    if bot_engine.is_running(user_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Le bot est déjà en cours d'exécution")

    config = current_user.bot_config.model_dump()
    # Paramètres optimaux par défaut si l'user n'a pas configuré
    if not config.get("symbol"):
        config["symbol"] = "BNBUSDT"
    if not config.get("interval"):
        config["interval"] = "5m"
    config.setdefault("risk_per_trade_pct", 90.0)
    config.setdefault("stop_loss_pct",      2.0)
    config.setdefault("take_profit_pct",    6.0)
    config.setdefault("max_open_trades",    2)   # 2 positions simultanées max
    config.setdefault("trailing_stop_pct",  2.0)
    await bot_engine.start(user_id, config)
    logger.info(f"Bot started by user {user_id}")
    return {"message": "Bot démarré avec succès", "symbol": config["symbol"], "interval": config["interval"]}


@router.post("/stop")
async def stop_bot(
    current_user: UserInDB = Depends(get_current_user),
):
    user_id = current_user.id
    if not bot_engine.is_running(user_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Le bot n'est pas en cours d'exécution")

    await bot_engine.stop(user_id)
    logger.info(f"Bot stopped by user {user_id}")
    return {"message": "Bot arrêté avec succès"}


@router.get("/status", response_model=BotStatusResponse)
async def get_bot_status(
    current_user: UserInDB = Depends(get_current_user),
):
    user_id = current_user.id
    status_data = bot_engine.get_status(user_id)
    config = current_user.bot_config

    if status_data:
        return BotStatusResponse(
            is_running=True,
            symbol=status_data["config"].get("symbol", config.symbol),
            interval=status_data["config"].get("interval", config.interval),
            last_cycle_at=status_data.get("last_cycle_at"),
            cycles_count=status_data.get("cycles_count", 0),
        )

    return BotStatusResponse(
        is_running=False,
        symbol=config.symbol,
        interval=config.interval,
    )


@router.put("/config", response_model=BotConfig)
async def update_bot_config(
    payload: BotConfigUpdate,
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    updates = payload.model_dump(exclude_none=True)

    if "symbol" in updates and updates["symbol"] not in VALID_SYMBOLS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Symbole invalide. Valides: {VALID_SYMBOLS}")

    if "interval" in updates and updates["interval"] not in VALID_INTERVALS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Interval invalide. Valides: {VALID_INTERVALS}")

    if "risk_per_trade_pct" in updates:
        v = updates["risk_per_trade_pct"]
        if not (0.1 <= v <= 20.0):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Risque par trade doit être entre 0.1% et 20%")

    if "stop_loss_pct" in updates:
        v = updates["stop_loss_pct"]
        if not (0.1 <= v <= 30.0):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Stop-loss doit être entre 0.1% et 30%")

    if "take_profit_pct" in updates:
        v = updates["take_profit_pct"]
        if not (0.1 <= v <= 100.0):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Take-profit doit être entre 0.1% et 100%")

    mongo_updates = {f"bot_config.{k}": v for k, v in updates.items()}
    await db.users.update_one(
        {"_id": ObjectId(current_user.id)},
        {"$set": mongo_updates},
    )

    if bot_engine.is_running(current_user.id):
        current_config = bot_engine.get_status(current_user.id)["config"]
        current_config.update(updates)
        logger.info(f"Bot config updated while running for user {current_user.id}")

    updated_config = current_user.bot_config.model_dump()
    updated_config.update(updates)
    return BotConfig(**updated_config)
