# FICHIER MODIFIÉ — Validation startup + production checks (ASCII-safe pour Windows)
from pydantic_settings import BaseSettings
from typing import List
import sys


class Settings(BaseSettings):
    MONGODB_URL:               str
    MONGODB_DB_NAME:           str  = "trading_bot"
    ANTHROPIC_API_KEY:         str
    BINANCE_API_KEY:           str
    BINANCE_SECRET_KEY:        str
    BINANCE_TESTNET:           bool = True
    SECRET_KEY:                str
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60   # 1 heure (sécurité)
    ENVIRONMENT:               str  = "development"
    CORS_ORIGINS:              str  = "http://localhost:5173"

    @property
    def cors_origins_list(self) -> List[str]:
        raw = self.CORS_ORIGINS.strip()
        if raw.startswith("["):
            import json
            try:
                return json.loads(raw)
            except Exception:
                pass
        return [o.strip() for o in raw.split(",") if o.strip()]

    def validate_for_production(self) -> None:
        """
        Vérifie que les clés critiques sont configurées.
        Arrête le serveur proprement si des placeholders sont détectés.
        """
        from utils.logger import get_logger
        log = get_logger("config")

        errors   = []
        warnings = []

        # Anthropic API Key
        ak = self.ANTHROPIC_API_KEY or ""
        if not ak or ak.startswith("sk-ant-VOTRE") or "XXXXXXX" in ak or len(ak) < 20:
            errors.append("ANTHROPIC_API_KEY non configuree - ajoutez votre cle sur console.anthropic.com")

        # Binance API Key
        bk = self.BINANCE_API_KEY or ""
        if not bk or "VOTRE" in bk.upper() or len(bk) < 10:
            errors.append("BINANCE_API_KEY non configuree - ajoutez votre cle Binance dans .env")

        # Binance Secret Key
        bs = self.BINANCE_SECRET_KEY or ""
        if not bs or "VOTRE" in bs.upper() or len(bs) < 10:
            errors.append("BINANCE_SECRET_KEY non configuree - ajoutez votre secret Binance dans .env")

        # Secret JWT
        if len(self.SECRET_KEY) < 32:
            errors.append("SECRET_KEY trop courte (minimum 32 caracteres aleatoires)")

        # MongoDB URL
        if "<username>" in self.MONGODB_URL or "<password>" in self.MONGODB_URL:
            errors.append("MONGODB_URL non configuree - remplacez les placeholders")

        # Avertissements
        if self.BINANCE_TESTNET:
            warnings.append("BINANCE_TESTNET=true - mode testnet actif (fonds fictifs)")
        if self.ACCESS_TOKEN_EXPIRE_MINUTES > 120:
            warnings.append(
                f"ACCESS_TOKEN_EXPIRE_MINUTES={self.ACCESS_TOKEN_EXPIRE_MINUTES}"
                " - recommande: 60 min en dev, 15 min en production"
            )

        for w in warnings:
            log.warning(f"[CONFIG] {w}")

        if errors:
            log.error("=" * 55)
            log.error("ERREURS DE CONFIGURATION - demarrage impossible :")
            for e in errors:
                log.error(f"  [X] {e}")
            log.error("=" * 55)
            log.error("Editez backend/.env et remplissez toutes les cles.")
            log.error("Modele disponible dans backend/.env.example")
            sys.exit(1)

        log.info("Configuration validee [OK]")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
settings.validate_for_production()
