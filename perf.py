#!/usr/bin/env python3
"""Performance ARIA Bot -- interroge l'API Railway en live."""
import sys
import os
import io
import requests
from datetime import datetime
from pathlib import Path

# Force UTF-8 sur la console Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Charge le .env de TradingAgents
env_path = Path(__file__).parent.parent / "TradingAgents" / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

BASE     = os.environ.get("ARIA_BASE_URL", "https://web-production-94b99.up.railway.app").rstrip("/") + "/api/v1"
EMAIL    = os.environ.get("ARIA_USERNAME", "")
PASSWORD = os.environ.get("ARIA_PASSWORD", "")


def login(email: str, password: str) -> str:
    r = requests.post(f"{BASE}/auth/login", json={"email": email, "password": password}, timeout=10)
    if r.status_code != 200:
        print(f"Erreur login: {r.status_code} {r.text}")
        sys.exit(1)
    return r.json()["access_token"]


def get_stats(token: str) -> dict:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{BASE}/trades/stats", headers=h, timeout=10)
    return r.json() if r.status_code == 200 else {}


def get_open(token: str) -> list:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{BASE}/trades/open/live", headers=h, timeout=10)
    return r.json() if r.status_code == 200 else []


def get_trades(token: str, limit: int = 5) -> list:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{BASE}/trades?limit={limit}&status=CLOSED", headers=h, timeout=10)
    return r.json() if r.status_code == 200 else []


def bar(value: float, max_val: float = 100, width: int = 20) -> str:
    filled = int((value / max_val) * width) if max_val > 0 else 0
    return "[" + "#" * filled + "." * (width - filled) + "]"


SEP  = "-" * 55
SEP2 = "=" * 55


def main():
    print(SEP2)
    print("         ARIA BOT - PERFORMANCE LIVE")
    print(SEP2)
    print(f"  URL : {BASE}")
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print(f"  Compte : {EMAIL}")
    print("  Connexion...")
    token = login(EMAIL, PASSWORD)
    print("  Connecte OK\n")

    # Stats globales
    stats    = get_stats(token)
    total    = stats.get("total_trades", 0)
    open_c   = stats.get("open_trades", 0)
    closed_c = stats.get("closed_trades", 0)
    win_rate = stats.get("win_rate", 0.0)
    pnl      = stats.get("total_pnl", 0.0)
    pnl_pct  = stats.get("total_pnl_pct", 0.0)
    best     = stats.get("best_trade_pnl")
    worst    = stats.get("worst_trade_pnl")

    print(SEP)
    print("  STATISTIQUES GLOBALES")
    print(SEP)
    print(f"  Total trades    : {total:>4}  (ouverts={open_c}  fermes={closed_c})")
    print(f"  Win rate        : {win_rate:>5.1f}%  {bar(win_rate)}")
    sign = "+" if pnl >= 0 else ""
    print(f"  PnL total       : {sign}{pnl:.4f} USDT  ({sign}{pnl_pct:.2f}%)")
    if best is not None:
        print(f"  Meilleur trade  : +{best:.4f} USDT")
    if worst is not None:
        print(f"  Pire trade      :  {worst:.4f} USDT")

    # Positions ouvertes
    open_trades = get_open(token)
    print()
    print(SEP)
    print("  POSITION(S) OUVERTE(S)")
    print(SEP)
    if not open_trades:
        print("  Aucune position ouverte.")
    else:
        for t in open_trades:
            sym   = t.get("symbol", "?")
            entry = t.get("entry_price", 0)
            cur   = t.get("current_price", 0)
            qty   = t.get("quantity", 0)
            inv   = t.get("total_usdt", 0)
            pnl_t = t.get("pnl", 0)
            pnl_p = t.get("pnl_pct", 0)
            tp    = t.get("take_profit")
            sl    = t.get("stop_loss")
            opened = t.get("created_at", "")[:16]
            s     = "+" if pnl_t >= 0 else ""
            arrow = "^UP^" if pnl_t >= 0 else "vDOWNv"
            print(f"  {arrow} {sym}")
            print(f"     Entree  : {entry:.6f}   Actuel : {cur:.6f}")
            print(f"     Qte     : {qty:.1f}     Investi: {inv:.2f} USDT")
            print(f"     P&L     : {s}{pnl_t:.4f} USDT  ({s}{pnl_p:.2f}%)")
            if tp:
                print(f"     TP/SL   : {tp:.6f} / {sl:.6f}")
            print(f"     Ouvert  : {opened}")

    # Derniers trades fermes
    recent = get_trades(token)
    print()
    print(SEP)
    print("  5 DERNIERS TRADES FERMES")
    print(SEP)
    if not recent:
        print("  Aucun trade ferme.")
    else:
        for t in recent:
            sym   = t.get("symbol", "?")
            pnl_t = t.get("pnl", 0) or 0
            s     = "+" if pnl_t >= 0 else ""
            ok    = "WIN" if pnl_t >= 0 else "LOSS"
            date  = str(t.get("closed_at") or t.get("created_at", ""))[:10]
            print(f"  [{ok}]  {sym:<12} {s}{pnl_t:.4f} USDT   {date}")

    print()
    print(SEP2)


if __name__ == "__main__":
    main()
