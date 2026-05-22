from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timezone


class PortfolioSnapshot(BaseModel):
    id: Optional[str] = None
    user_id: str
    total_usdt: float
    available_usdt: float
    invested_usdt: float
    total_pnl: float
    total_pnl_pct: float
    win_rate: float
    total_trades: int
    winning_trades: int
    recorded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_mongo(cls, data: dict) -> "PortfolioSnapshot":
        if data and "_id" in data:
            data["id"] = str(data.pop("_id"))
        return cls(**data)

    def to_mongo(self) -> dict:
        d = self.model_dump(exclude={"id"})
        return d


class PortfolioResponse(BaseModel):
    total_usdt: float
    available_usdt: float
    invested_usdt: float
    total_pnl: float
    total_pnl_pct: float
    win_rate: float
    total_trades: int
    winning_trades: int
    binance_balance: Optional[dict] = None


class PortfolioMetrics(BaseModel):
    sharpe_ratio: Optional[float] = None
    max_drawdown: float
    avg_win: float
    avg_loss: float
    risk_reward_ratio: float
    profit_factor: float
    total_return_pct: float
