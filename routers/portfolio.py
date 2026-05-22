from fastapi import APIRouter, Depends, HTTPException, status
from typing import List
from datetime import datetime, timedelta, timezone

from database import get_database
from models.portfolio import PortfolioResponse, PortfolioMetrics, PortfolioSnapshot
from models.user import UserInDB
from routers.auth import get_current_user
from services.binance_service import binance_service
from services.risk_manager import risk_manager
from utils.logger import get_logger
import asyncio

logger = get_logger(__name__)

router = APIRouter(prefix="/portfolio", tags=["Portfolio"])


@router.get("", response_model=PortfolioResponse)
async def get_portfolio(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    try:
        balances = await asyncio.get_event_loop().run_in_executor(
            None, binance_service.get_account_balance
        )
        usdt_free = balances.get("USDT", {}).get("free", 0.0)
        binance_balance_data = balances
    except Exception as e:
        logger.warning(f"Could not fetch Binance balance: {e}")
        usdt_free = 0.0
        binance_balance_data = {}

    try:
        trades = await db.trades.find({"user_id": current_user.id}).to_list(length=5000)
    except Exception as e:
        logger.error(f"DB error fetching trades for portfolio: {e}")
        trades = []
    closed_trades = [t for t in trades if t.get("status") == "CLOSED"]
    open_trades   = [t for t in trades if t.get("status") == "OPEN"]

    invested = sum(t.get("total_usdt", 0.0) for t in open_trades)
    pnl_values = [t.get("pnl", 0.0) or 0.0 for t in closed_trades]
    total_pnl = sum(pnl_values)
    winning = [p for p in pnl_values if p > 0]
    win_rate = (len(winning) / len(closed_trades) * 100) if closed_trades else 0.0

    total_usdt = usdt_free + invested
    # Capital initial = premier snapshot enregistré
    first_snap = await db.portfolio_history.find_one({"user_id": current_user.id}, sort=[("recorded_at", 1)])
    initial_capital = first_snap.get("total_usdt", total_usdt) if first_snap else total_usdt
    if initial_capital <= 0:
        initial_capital = total_usdt
    total_pnl_pct = ((total_usdt - initial_capital) / initial_capital * 100) if initial_capital > 0 else 0.0

    return PortfolioResponse(
        total_usdt=total_usdt,
        available_usdt=usdt_free,
        invested_usdt=invested,
        total_pnl=total_pnl,
        total_pnl_pct=total_pnl_pct,
        win_rate=win_rate,
        total_trades=len(trades),
        winning_trades=len(winning),
        binance_balance=binance_balance_data,
    )


@router.get("/history")
async def get_portfolio_history(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    try:
        cursor  = db.portfolio_history.find(
            {"user_id": current_user.id, "recorded_at": {"$gte": cutoff}}
        ).sort("recorded_at", 1)
        history = await cursor.to_list(length=1000)
    except Exception as e:
        logger.error(f"DB error fetching portfolio history: {e}")
        return []

    result = []
    for h in history:
        try:
            result.append({
                "date":       h["recorded_at"].isoformat(),
                "total_usdt": h.get("total_usdt", 0.0),
                "pnl":        h.get("total_pnl", 0.0),
                "pnl_pct":    h.get("total_pnl_pct", 0.0),
            })
        except Exception:
            continue
    return result


@router.get("/metrics", response_model=PortfolioMetrics)
async def get_portfolio_metrics(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    try:
        trades = await db.trades.find({"user_id": current_user.id}).to_list(length=5000)
    except Exception as e:
        logger.error(f"DB error fetching trades for metrics: {e}")
        trades = []
    trade_dicts = []
    for t in trades:
        t["id"] = str(t.pop("_id", None) or "")
        trade_dicts.append(t)

    metrics = risk_manager.get_risk_metrics(trade_dicts)

    closed = [t for t in trade_dicts if t.get("status") == "CLOSED" and t.get("pnl") is not None]
    total_invested = sum(t.get("total_usdt", 0.0) for t in closed)
    total_pnl = sum(t.get("pnl", 0.0) or 0.0 for t in closed)
    total_return_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0.0

    return PortfolioMetrics(
        sharpe_ratio=metrics.get("sharpe_ratio"),
        max_drawdown=metrics.get("max_drawdown", 0.0),
        avg_win=metrics.get("avg_win", 0.0),
        avg_loss=metrics.get("avg_loss", 0.0),
        risk_reward_ratio=metrics.get("risk_reward_ratio", 0.0),
        profit_factor=metrics.get("profit_factor", 0.0),
        total_return_pct=total_return_pct,
    )
