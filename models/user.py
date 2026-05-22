from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime, timezone
from bson import ObjectId


class BotConfig(BaseModel):
    is_running: bool = False
    symbol: str = "BTCUSDT"
    interval: str = "15m"
    risk_per_trade_pct: float = 2.0
    max_open_trades: int = 3
    stop_loss_pct: float = 2.0
    take_profit_pct: float = 4.0


class UserCreate(BaseModel):
    email: EmailStr
    password: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserInDB(BaseModel):
    id: Optional[str] = None
    email: str
    hashed_password: str
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    bot_config: BotConfig = Field(default_factory=BotConfig)

    @classmethod
    def from_mongo(cls, data: dict) -> "UserInDB":
        if data and "_id" in data:
            data["id"] = str(data.pop("_id"))
        return cls(**data)

    def to_mongo(self) -> dict:
        d = self.model_dump(exclude={"id"})
        return d


class UserResponse(BaseModel):
    id: str
    email: str
    is_active: bool
    created_at: datetime
    bot_config: BotConfig


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse
