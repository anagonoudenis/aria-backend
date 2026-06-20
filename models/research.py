from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class PairRating(BaseModel):
    ticker:           str
    rating:           str                    # BUY / OVERWEIGHT / HOLD / UNDERWEIGHT / SELL
    conviction:       float = 0.5            # 0.0 – 1.0
    price_target:     Optional[float] = None
    time_horizon:     str = "1-2 semaines"
    bias_score:       int = 0                # -5 … +5
    size_multiplier:  float = 1.0            # multiplicateur Kelly
    error:            Optional[str] = None


class ResearchReport(BaseModel):
    date:              str
    created_at:        str
    expires_at:        str
    pairs_analyzed:    List[str] = []
    ratings:           Dict[str, PairRating] = {}
    ticker:            str = ""
    rating:            str = "HOLD"
    conviction:        float = 0.5
    investment_thesis: str = ""
    key_risks:         List[str] = []
    aria_alignment:    Dict[str, Any] = {}


class ResearchReportIn(BaseModel):
    """Payload reçu depuis le runner TradingAgents."""
    date:              str
    created_at:        str
    expires_at:        str
    pairs_analyzed:    List[str] = []
    ratings:           Dict[str, Any] = {}
    ticker:            str = ""
    rating:            str = "HOLD"
    conviction:        float = 0.5
    investment_thesis: str = ""
    key_risks:         List[str] = []
    aria_alignment:    Dict[str, Any] = {}


class ResearchReportOut(BaseModel):
    id:                Optional[str] = None
    date:              str
    created_at:        str
    pairs_analyzed:    List[str] = []
    ratings:           Dict[str, Any] = {}
    ticker:            str = ""
    rating:            str = "HOLD"
    conviction:        float = 0.5
    investment_thesis: str = ""
    key_risks:         List[str] = []
    aria_alignment:    Dict[str, Any] = {}
