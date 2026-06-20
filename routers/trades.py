from fastapi import APIRouter, Depends, Query, HTTPException, status
from typing import Optional, List
from datetime import datetime
from bson import ObjectId
import re

from database import get_database
from models.trade import TradeResponse, TradeStats, TradeSide, TradeStatus
from models.user import UserInDB
from routers.auth import get_current_user
from utils.logger import get_logger

VALID_SYMBOLS = re.compile(r'^[A-Z]{2,10}USDT$')

logger = get_logger(__name__)

router = APIRouter(prefix="/trades", tags=["Trades"])


@router.get("", response_model=List[TradeResponse])
async def get_trades(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=20, ge=1, le=100),
    symbol: Optional[str] = Query(default=None),
    side: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    date_from: Optional[datetime] = Query(default=None),
    date_to: Optional[datetime] = Query(default=None),
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    query = {"user_id": current_user.id}

    if symbol:
        sym = symbol.upper()
        if not VALID_SYMBOLS.match(sym):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Symbole invalide")
        query["symbol"] = sym
    if side and side.upper() in {"BUY", "SELL"}:
        query["side"] = side.upper()
    if status and status.upper() in {"OPEN", "CLOSED"}:
        query["status"] = status.upper()
    if date_from or date_to:
        query["created_at"] = {}
        if date_from:
            query["created_at"]["$gte"] = date_from
        if date_to:
            query["created_at"]["$lte"] = date_to

    skip = (page - 1) * limit
    cursor = db.trades.find(query).sort("created_at", -1).skip(skip).limit(limit)
    trades = await cursor.to_list(length=limit)

    result = []
    for t in trades:
        t["id"] = str(t.pop("_id"))
        result.append(TradeResponse(**t))
    return result


@router.get("/stats", response_model=TradeStats)
async def get_trade_stats(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    # ── Agrégation MongoDB côté serveur — O(1) mémoire Python ────────────
    pipeline = [
        {"$match": {"user_id": current_user.id}},
        {"$facet": {
            "counts": [
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "total_invested": {"$sum": {"$cond": [{"$eq": ["$status", "CLOSED"]}, {"$ifNull": ["$total_usdt", 0]}, 0]}},
                }}
            ],
            "pnl_stats": [
                {"$match": {"status": "CLOSED", "pnl": {"$ne": None}}},
                {"$group": {
                    "_id": None,
                    "total_pnl":  {"$sum": "$pnl"},
                    "best_trade": {"$max": "$pnl"},
                    "worst_trade": {"$min": "$pnl"},
                    "winning":    {"$sum": {"$cond": [{"$gt": ["$pnl", 0]}, 1, 0]}},
                    "closed_cnt": {"$sum": 1},
                    "total_inv":  {"$sum": {"$ifNull": ["$total_usdt", 0]}},
                }}
            ],
        }}
    ]

    result = await db.trades.aggregate(pipeline).to_list(length=1)
    if not result:
        return TradeStats(total_trades=0, open_trades=0, closed_trades=0, win_rate=0,
                          total_pnl=0, total_pnl_pct=0)

    counts     = {c["_id"]: c["count"] for c in result[0].get("counts", [])}
    pnl_list   = result[0].get("pnl_stats", [])
    pnl        = pnl_list[0] if pnl_list else {}

    open_cnt   = counts.get("OPEN", 0)
    closed_cnt = pnl.get("closed_cnt", 0)
    total      = open_cnt + closed_cnt

    total_pnl  = pnl.get("total_pnl", 0.0)
    total_inv  = pnl.get("total_inv", 0.0)
    winning    = pnl.get("winning", 0)
    win_rate   = (winning / closed_cnt * 100) if closed_cnt > 0 else 0.0
    pnl_pct    = (total_pnl / total_inv * 100) if total_inv > 0 else 0.0

    return TradeStats(
        total_trades=total,
        open_trades=open_cnt,
        closed_trades=closed_cnt,
        win_rate=win_rate,
        total_pnl=total_pnl,
        total_pnl_pct=pnl_pct,
        best_trade_pnl=pnl.get("best_trade"),
        worst_trade_pnl=pnl.get("worst_trade"),
        avg_duration_minutes=None,
    )


@router.get("/open")
@router.get("/open/live")
async def get_open_trades_live(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    """Positions ouvertes avec P&L temps réel depuis Binance."""
    import asyncio
    open_trades = await db.trades.find(
        {"user_id": current_user.id, "status": "OPEN"}
    ).to_list(length=20)

    result = []
    for t in open_trades:
        symbol     = t.get("symbol", "BNBUSDT")
        entry      = float(t.get("price", 0))
        qty        = float(t.get("quantity", 0))
        total_usdt = float(t.get("total_usdt", 0))
        try:
            current_price = await asyncio.get_event_loop().run_in_executor(
                None, lambda s=symbol: binance_service.get_current_price(s)
            )
            gross   = qty * current_price
            fee     = gross * 0.001
            net     = gross - fee
            pnl     = net - total_usdt
            pnl_pct = (pnl / total_usdt * 100) if total_usdt > 0 else 0
        except Exception:
            current_price = entry
            pnl = 0.0
            pnl_pct = 0.0

        result.append({
            "id":            str(t["_id"]),
            "symbol":        symbol,
            "side":          t.get("side", "BUY"),
            "entry_price":   entry,
            "current_price": current_price,
            "quantity":      qty,
            "total_usdt":    total_usdt,
            "pnl":           round(pnl, 4),
            "pnl_pct":       round(pnl_pct, 2),
            "take_profit":   t.get("take_profit_price"),
            "stop_loss":     t.get("stop_loss_price"),
            "created_at":    str(t.get("created_at", "")),
        })
    return result


@router.get("/{trade_id}", response_model=TradeResponse)
async def get_trade(
    trade_id: str,
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    try:
        oid = ObjectId(trade_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ID de trade invalide")

    trade_doc = await db.trades.find_one({"_id": oid, "user_id": current_user.id})
    if not trade_doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Trade introuvable")

    trade_doc["id"] = str(trade_doc.pop("_id"))
    return TradeResponse(**trade_doc)
