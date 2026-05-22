from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timezone
from enum import Enum


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TradeStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class TradeCreate(BaseModel):
    user_id: str
    symbol: str
    side: TradeSide
    quantity: float
    price: float
    total_usdt: float
    binance_order_id: Optional[str] = None


class TradeInDB(BaseModel):
    id: Optional[str] = None
    user_id: str
    symbol: str
    side: TradeSide
    quantity: float
    price: float
    total_usdt: float
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    status: TradeStatus = TradeStatus.OPEN
    binance_order_id: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: Optional[datetime] = None

    @classmethod
    def from_mongo(cls, data: dict) -> "TradeInDB":
        if data and "_id" in data:
            data["id"] = str(data.pop("_id"))
        return cls(**data)

    def to_mongo(self) -> dict:
        d = self.model_dump(exclude={"id"})
        return d


class TradeResponse(BaseModel):
    id: str
    user_id: str
    symbol: str
    side: TradeSide
    quantity: float
    price: float
    total_usdt: float
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None
    status: TradeStatus
    binance_order_id: Optional[str] = None
    created_at: datetime
    closed_at: Optional[datetime] = None


class TradeStats(BaseModel):
    total_trades: int
    open_trades: int
    closed_trades: int
    win_rate: float
    total_pnl: float
    total_pnl_pct: float
    best_trade_pnl: Optional[float] = None
    worst_trade_pnl: Optional[float] = None
    avg_duration_minutes: Optional[float] = None
