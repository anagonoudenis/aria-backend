from fastapi import APIRouter, Depends, HTTPException, status
from typing import Optional

from database import get_database
from models.research import ResearchReportIn, ResearchReportOut
from models.user import UserInDB
from routers.auth import get_current_user
from utils.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/research", tags=["Research Intelligence"])


@router.post("/update", status_code=200)
async def update_research(
    report: ResearchReportIn,
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    """Reçoit un rapport TradingAgents depuis le runner et l'enregistre en MongoDB."""
    try:
        doc = report.model_dump()
        await db.research_reports.delete_many({"date": doc["date"]})
        await db.research_reports.insert_one(doc)
        # Mise à jour immédiate du cache en mémoire pour ARIA
        from services.research_reader import set_cache
        set_cache(doc)
        logger.info(f"Rapport TradingAgents enregistré — {doc['date']} — {len(doc.get('pairs_analyzed', []))} paires")
        return {"status": "ok", "date": doc["date"]}
    except Exception as e:
        logger.error(f"Erreur update_research: {e}")
        raise HTTPException(status_code=500, detail="Erreur lors de l'enregistrement du rapport")


@router.get("/latest", response_model=Optional[ResearchReportOut])
async def get_latest_research(
    current_user: UserInDB = Depends(get_current_user),
    db=Depends(get_database),
):
    """Retourne le rapport TradingAgents le plus récent (pour le monitor CLI et le frontend)."""
    try:
        doc = await db.research_reports.find_one(sort=[("created_at", -1)])
        if not doc:
            return None
        doc["id"] = str(doc.pop("_id", ""))
        return ResearchReportOut(**doc)
    except Exception as e:
        logger.error(f"Erreur get_latest_research: {e}")
        raise HTTPException(status_code=500, detail="Erreur lors de la récupération du rapport")
