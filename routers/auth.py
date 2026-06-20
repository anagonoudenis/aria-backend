# FICHIER MODIFIÉ — Sécurité maximale : rate limiting + timing attack + password fort
from fastapi import APIRouter, HTTPException, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import timedelta
from bson import ObjectId, errors as bson_errors
from passlib.context import CryptContext

from database import get_database
from models.user import UserCreate, UserLogin, UserInDB, UserResponse, TokenResponse, BotConfig
from utils.jwt import create_access_token, verify_token
from utils.security import validate_password_strength, validate_email, auth_rate_limit, check_rate_limit
from utils.cache import cache
from config import settings
from utils.logger import get_logger

_USER_CACHE_TTL = 300  # 5 minutes

logger = get_logger(__name__)

router   = APIRouter(prefix="/auth", tags=["Authentication"])
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db=Depends(get_database),
) -> UserInDB:
    token   = credentials.credentials
    user_id = verify_token(token)

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide ou expiré",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        oid = ObjectId(user_id)
    except (bson_errors.InvalidId, Exception):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide")

    # ── Cache hit : évite un aller-retour MongoDB par requête ─────────────
    cache_key = f"user:{user_id}"
    cached_user = cache.get(cache_key)
    if cached_user is not None:
        return cached_user

    user_doc = await db.users.find_one({"_id": oid})
    if not user_doc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable")
    if not user_doc.get("is_active", True):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Compte désactivé")

    user = UserInDB.from_mongo(user_doc)
    cache.set(cache_key, user, ttl_seconds=_USER_CACHE_TTL)
    return user


# ── REGISTER ───────────────────────────────────────────────────────────────────
@router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(auth_rate_limit)],   # ← rate limiting IP
)
async def register(
    payload: UserCreate,
    request: Request,
    db=Depends(get_database),
):
    # Validation email
    if not validate_email(payload.email):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Format d'email invalide",
        )

    # Validation mot de passe fort
    ok, msg = validate_password_strength(payload.password)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=msg,
        )

    # Vérifier existence — message identique pour éviter l'énumération
    existing = await db.users.find_one({"email": payload.email.lower()})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Impossible de créer ce compte. Vérifiez vos informations.",
        )

    user = UserInDB(
        email=payload.email.lower(),
        hashed_password=hash_password(payload.password),
        bot_config=BotConfig(),
    )
    result  = await db.users.insert_one(user.to_mongo())
    user_id = str(result.inserted_id)

    token = create_access_token(
        data={"sub": user_id},
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )

    logger.info(f"New user registered (ip={_get_ip(request)})")
    return TokenResponse(
        access_token=token,
        user=UserResponse(
            id=user_id, email=user.email, is_active=user.is_active,
            created_at=user.created_at, bot_config=user.bot_config,
        ),
    )


# ── LOGIN ─────────────────────────────────────────────────────────────────────
@router.post(
    "/login",
    response_model=TokenResponse,
    dependencies=[Depends(auth_rate_limit)],   # ← rate limiting IP
)
async def login(
    payload: UserLogin,
    request: Request,
    db=Depends(get_database),
):
    # Toujours exécuter verify_password (même si l'user n'existe pas)
    # pour éviter les attaques par timing
    _DUMMY_HASH = "$2b$12$EIXbf8mGXGy5vZXvqFSHQeZH0.3.wNWVKFzMqJzVZhKNLTxUbGrq2"
    user_doc     = await db.users.find_one({"email": payload.email.lower()})
    stored_hash  = user_doc["hashed_password"] if user_doc else _DUMMY_HASH
    password_ok  = verify_password(payload.password, stored_hash)

    # Message d'erreur identique — évite l'énumération des comptes
    if not user_doc or not password_ok:
        logger.warning(f"Failed login attempt for email={payload.email[:3]}*** (ip={_get_ip(request)})")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email ou mot de passe incorrect",
        )

    user = UserInDB.from_mongo(user_doc)
    if not user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Compte désactivé")

    token = create_access_token(
        data={"sub": user.id},
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )

    logger.info(f"User logged in (ip={_get_ip(request)})")
    return TokenResponse(
        access_token=token,
        user=UserResponse(
            id=user.id, email=user.email, is_active=user.is_active,
            created_at=user.created_at, bot_config=user.bot_config,
        ),
    )


# ── CLI TOKEN — limite séparée, pour le monitor SIRIUS ───────────────────────
@router.post("/cli-token", response_model=TokenResponse)
async def cli_token(
    payload: UserLogin,
    request: Request,
    db=Depends(get_database),
):
    """Endpoint dédié au CLI monitor — rate limit souple (20/heure par IP)."""
    await check_rate_limit(request, max_requests=20, window_seconds=3600, endpoint_name="cli-token")
    _DUMMY_HASH = "$2b$12$EIXbf8mGXGy5vZXvqFSHQeZH0.3.wNWVKFzMqJzVZhKNLTxUbGrq2"
    user_doc    = await db.users.find_one({"email": payload.email.lower()})
    stored_hash = user_doc["hashed_password"] if user_doc else _DUMMY_HASH
    password_ok = verify_password(payload.password, stored_hash)
    if not user_doc or not password_ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Email ou mot de passe incorrect")
    user  = UserInDB.from_mongo(user_doc)
    token = create_access_token(
        data={"sub": user.id},
        expires_delta=timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    return TokenResponse(
        access_token=token,
        user=UserResponse(id=user.id, email=user.email, is_active=user.is_active,
                          created_at=user.created_at, bot_config=user.bot_config),
    )


# ── ME ────────────────────────────────────────────────────────────────────────
@router.get("/me", response_model=UserResponse)
async def get_me(current_user: UserInDB = Depends(get_current_user)):
    return UserResponse(
        id=current_user.id, email=current_user.email,
        is_active=current_user.is_active, created_at=current_user.created_at,
        bot_config=current_user.bot_config,
    )


def _get_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    return forwarded.split(",")[0].strip() if forwarded else (request.client.host or "unknown")
