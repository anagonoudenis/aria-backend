"""
Script de réinitialisation du mot de passe ARIA.
Usage : python reset_password.py
"""

import asyncio
import os
from dotenv import load_dotenv
from passlib.context import CryptContext
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL", "")
MONGODB_DB  = os.getenv("MONGODB_DB_NAME", "trading_bot")
EMAIL       = "denisanagonou259@gmail.com"
NEW_PASSWORD = "Sirius@2026!"   # ← change ici si tu veux un autre mot de passe

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def reset():
    if not MONGODB_URL:
        print("MONGODB_URL manquant dans .env")
        return

    client = AsyncIOMotorClient(MONGODB_URL, serverSelectionTimeoutMS=10000)
    db     = client[MONGODB_DB]

    hashed = pwd_context.hash(NEW_PASSWORD)

    result = await db.users.update_one(
        {"email": EMAIL},
        {"$set": {"hashed_password": hashed}}
    )

    if result.matched_count == 0:
        print(f"Utilisateur {EMAIL} introuvable.")
        print("Utilisateurs existants :")
        async for u in db.users.find({}, {"email": 1}):
            print(f"  - {u.get('email')}")
    else:
        print(f"Mot de passe réinitialisé pour {EMAIL}")
        print(f"Nouveau mot de passe : {NEW_PASSWORD}")

    client.close()


asyncio.run(reset())
