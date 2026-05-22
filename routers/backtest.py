# NOUVEAU FICHIER — à créer
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

from database import get_database
from models.user import UserInDB
from routers.auth import get_current_user
from services.backtest_service import backtest_engine
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/backtest", tags=["Backtest"])

VALID_SYMBOLS   = {"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","ADAUSDT","XRPUSDT","DOGEUSDT"}
VALID_INTERVALS = {"1m","5m","15m","1h","4h","1d"}


class BacktestRequest(BaseModel):
    symbol:          str   = Field(default="BTCUSDT")
    interval:        str   = Field(default="1h")
    start_date:      str   = Field(description="Format YYYY-MM-DD")
    end_date:        str   = Field(description="Format YYYY-MM-DD")
    initial_capital: float = Field(default=1000.0, ge=100.0, le=1_000_000.0)
    stop_loss_pct:   float = Field(default=2.0,  ge=0.5, le=20.0)
    take_profit_pct: float = Field(default=4.0,  ge=0.5, le=50.0)
    risk_per_trade_pct: float = Field(default=2.0, ge=0.1, le=10.0)


@router.post("/run")
async def run_backtest(
    payload: BacktestRequest,
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    if payload.symbol.upper() not in VALID_SYMBOLS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Symbole invalide. Valides: {sorted(VALID_SYMBOLS)}")
    if payload.interval not in VALID_INTERVALS:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Interval invalide. Valides: {sorted(VALID_INTERVALS)}")

    # Validate dates
    try:
        start = datetime.strptime(payload.start_date, "%Y-%m-%d")
        end   = datetime.strptime(payload.end_date,   "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Format de date invalide. Utilisez YYYY-MM-DD")

    if start >= end:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="start_date doit être avant end_date")

    days = (end - start).days
    if days > 730:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Période maximale: 2 ans (730 jours)")

    try:
        bot_config = {
            "stop_loss_pct":    payload.stop_loss_pct,
            "take_profit_pct":  payload.take_profit_pct,
            "risk_per_trade_pct": payload.risk_per_trade_pct,
        }
        result = await backtest_engine.run(
            symbol=payload.symbol.upper(),
            interval=payload.interval,
            start_date=payload.start_date,
            end_date=payload.end_date,
            initial_capital=payload.initial_capital,
            bot_config=bot_config,
        )

        # Persister le résultat
        doc = {
            "user_id":        current_user.id,
            "symbol":         payload.symbol.upper(),
            "interval":       payload.interval,
            "start_date":     payload.start_date,
            "end_date":       payload.end_date,
            "initial_capital":payload.initial_capital,
            "config":         bot_config,
            "summary":        result["summary"],
            "created_at":     datetime.utcnow(),
        }
        insert_result = await db.backtests.insert_one(doc)
        result["id"] = str(insert_result.inserted_id)

        logger.info(
            f"Backtest {payload.symbol} {payload.start_date}→{payload.end_date} "
            f"return={result['summary']['total_return_pct']}%"
        )
        return result

    except ValueError as e:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur lors du backtest")


@router.get("/history")
async def get_backtest_history(
    limit: int = 10,
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    try:
        docs = await db.backtests.find(
            {"user_id": current_user.id}
        ).sort("created_at", -1).limit(limit).to_list(length=limit)
    except Exception as e:
        logger.error(f"DB error fetching backtest history: {e}")
        return []

    result = []
    for d in docs:
        try:
            d["id"] = str(d.pop("_id", None) or "")
            if "created_at" in d and d["created_at"]:
                d["created_at"] = d["created_at"].isoformat()
            result.append(d)
        except Exception:
            continue
    return result


@router.get("/notifications")
async def get_notifications(
    limit: int = 20,
    current_user: UserInDB = Depends(get_current_user),
):
    from services.notification_service import notification_service
    return await notification_service.get_recent(current_user.id, limit)


@router.post("/notifications/read")
async def mark_notifications_read(
    current_user: UserInDB = Depends(get_current_user),
):
    from services.notification_service import notification_service
    await notification_service.mark_read(current_user.id)
    return {"message": "Notifications marquées comme lues"}
