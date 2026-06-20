"""
Service de lecture des rapports TradingAgents depuis MongoDB.
Utilisé par analysis_service.py (sync) et risk_manager.py (sync)
pour enrichir les signaux ARIA avec l'intelligence multi-agents.

Architecture cache :
  - _report_cache : dict en mémoire, mis à jour async via set_cache()
  - get_bias_sync() : lecture synchrone depuis le cache (usage dans analysis_service)
  - refresh_cache() : chargement async depuis MongoDB (appelé au démarrage + après update)
"""

import time
from typing import Optional, Dict
from utils.logger import get_logger

logger = get_logger(__name__)

_SYMBOL_MAP = {
    "BTCUSDT": "BTCUSDT",
    "ETHUSDT": "ETHUSDT",
    "BNBUSDT": "BNBUSDT",
    "SOLUSDT": "SOLUSDT",
    "XRPUSDT": "XRPUSDT",
    "ADAUSDT": "ADAUSDT",
    "DOGEUSDT": "DOGEUSDT",
}

_report_cache: Optional[Dict] = None
_cache_ts: float = 0.0
_CACHE_TTL = 3600.0


def set_cache(doc: Dict) -> None:
    """Met à jour le cache depuis le router (appelé après POST /research/update)."""
    global _report_cache, _cache_ts
    _report_cache = doc
    _cache_ts = time.time()
    logger.info(f"research_reader: cache mis à jour — {doc.get('date', '?')}")


def get_bias_sync(symbol: str) -> Optional[Dict]:
    """
    Retourne le biais TradingAgents pour un symbol ARIA (version synchrone).
    Retourne None si aucun rapport disponible ou symbol inconnu.
    Format :
        {
            "rating":          "BUY",
            "conviction":      0.82,
            "bias_score":      5,       # -5 … +5
            "size_multiplier": 1.5,
            "price_target":    72000.0,
            "time_horizon":    "1-2 semaines",
        }
    """
    if _report_cache is None:
        return None
    key = _SYMBOL_MAP.get(symbol)
    if not key:
        return None
    ratings = _report_cache.get("ratings", {})
    pair_data = ratings.get(key)
    if not pair_data:
        return None
    return {
        "rating":          pair_data.get("rating", "HOLD"),
        "conviction":      float(pair_data.get("conviction", 0.5)),
        "bias_score":      int(pair_data.get("bias_score", 0)),
        "size_multiplier": float(pair_data.get("size_multiplier", 1.0)),
        "price_target":    pair_data.get("price_target"),
        "time_horizon":    pair_data.get("time_horizon", "—"),
    }


async def refresh_cache() -> None:
    """Charge le dernier rapport depuis MongoDB dans le cache mémoire."""
    global _report_cache, _cache_ts
    try:
        from database import get_database
        db = get_database()
        doc = await db.research_reports.find_one(sort=[("created_at", -1)])
        if doc:
            doc.pop("_id", None)
            set_cache(doc)
        else:
            logger.debug("research_reader: aucun rapport en MongoDB.")
    except Exception as e:
        logger.warning(f"research_reader: impossible de charger le rapport: {e}")
