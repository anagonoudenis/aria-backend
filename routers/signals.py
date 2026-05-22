from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional
import re

from database import get_database

VALID_SYMBOLS = re.compile(r'^[A-Z]{2,10}USDT$')
from models.signal import SignalResponse, SignalStats
from models.user import UserInDB
from routers.auth import get_current_user
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/signals", tags=["Signals"])


@router.get("", response_model=List[SignalResponse])
async def get_signals(
    limit: int = Query(default=50, ge=1, le=200),
    symbol: Optional[str] = Query(default=None),
    action: Optional[str] = Query(default=None),
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    query = {}
    if symbol:
        sym = symbol.upper()
        if not VALID_SYMBOLS.match(sym):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Symbole invalide")
        query["symbol"] = sym
    if action and action.upper() in {"BUY", "SELL", "HOLD"}:
        query["action"] = action.upper()

    cursor = db.signals.find(query).sort("created_at", -1).limit(limit)
    signals = await cursor.to_list(length=limit)

    result = []
    for s in signals:
        s["id"] = str(s.pop("_id"))
        result.append(SignalResponse(**s))
    return result


@router.get("/latest", response_model=SignalResponse)
async def get_latest_signal(
    symbol: Optional[str] = Query(default=None),
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    query = {}
    if symbol:
        sym = symbol.upper()
        if not VALID_SYMBOLS.match(sym):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Symbole invalide")
        query["symbol"] = sym

    signal_doc = await db.signals.find_one(query, sort=[("created_at", -1)])
    if not signal_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aucun signal disponible")

    signal_doc["id"] = str(signal_doc.pop("_id"))
    return SignalResponse(**signal_doc)


@router.get("/stats", response_model=SignalStats)
async def get_signal_stats(
    symbol: Optional[str] = Query(default=None),
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    query = {}
    if symbol:
        sym = symbol.upper()
        if not VALID_SYMBOLS.match(sym):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Symbole invalide")
        query["symbol"] = sym

    signals = await db.signals.find(query).sort("created_at", -1).to_list(length=1000)

    if not signals:
        return SignalStats(
            total_signals=0,
            buy_pct=0.0,
            sell_pct=0.0,
            hold_pct=0.0,
            avg_confidence=0.0,
            high_confidence_signals=0,
        )

    total = len(signals)
    buy_count = sum(1 for s in signals if s.get("action") == "BUY")
    sell_count = sum(1 for s in signals if s.get("action") == "SELL")
    hold_count = sum(1 for s in signals if s.get("action") == "HOLD")

    confidences = [s.get("confidence", 0.0) for s in signals]
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
    high_confidence = sum(1 for c in confidences if c >= 0.8)

    return SignalStats(
        total_signals=total,
        buy_pct=buy_count / total * 100,
        sell_pct=sell_count / total * 100,
        hold_pct=hold_count / total * 100,
        avg_confidence=avg_confidence,
        high_confidence_signals=high_confidence,
    )
