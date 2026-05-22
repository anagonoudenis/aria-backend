from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from enum import Enum


class SignalAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class MarketRegime(str, Enum):
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGING = "RANGING"
    VOLATILE = "VOLATILE"


class TimeframeAlignment(str, Enum):
    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"


class SignalInDB(BaseModel):
    id: Optional[str] = None
    symbol: str
    action: SignalAction
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    key_factors: List[str] = Field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.MEDIUM
    suggested_stop_loss_pct: float = 2.0
    suggested_take_profit_pct: float = 4.0
    market_regime: MarketRegime = MarketRegime.RANGING
    timeframe_alignment: TimeframeAlignment = TimeframeAlignment.MODERATE
    entry_quality: float = Field(default=0.5, ge=0.0, le=1.0)
    indicators: Dict[str, Any] = Field(default_factory=dict)
    price_at_signal: float
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_mongo(cls, data: dict) -> "SignalInDB":
        if data and "_id" in data:
            data["id"] = str(data.pop("_id"))
        return cls(**data)

    def to_mongo(self) -> dict:
        d = self.model_dump(exclude={"id"})
        return d


class SignalResponse(BaseModel):
    id: str
    symbol: str
    action: SignalAction
    confidence: float
    reasoning: str
    key_factors: List[str]
    risk_level: RiskLevel
    suggested_stop_loss_pct: float
    suggested_take_profit_pct: float
    market_regime: MarketRegime
    timeframe_alignment: TimeframeAlignment
    entry_quality: float
    indicators: Dict[str, Any]
    price_at_signal: float
    created_at: datetime


class SignalStats(BaseModel):
    total_signals: int
    buy_pct: float
    sell_pct: float
    hold_pct: float
    avg_confidence: float
    high_confidence_signals: int
