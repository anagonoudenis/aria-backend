import pandas as pd
import numpy as np
import ta
from typing import Dict, Any, List, Tuple, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# INDICATEURS TECHNIQUES COMPLETS
# ══════════════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame) -> Dict[str, Any]:
    try:
        df = df.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        indicators: Dict[str, Any] = {}

        # Tendance
        df["sma_20"]  = ta.trend.sma_indicator(df["close"], window=20)
        df["sma_50"]  = ta.trend.sma_indicator(df["close"], window=50)
        df["sma_200"] = ta.trend.sma_indicator(df["close"], window=200)
        df["ema_9"]   = ta.trend.ema_indicator(df["close"], window=9)
        df["ema_21"]  = ta.trend.ema_indicator(df["close"], window=21)

        macd = ta.trend.MACD(df["close"])
        df["macd_line"]      = macd.macd()
        df["macd_signal"]    = macd.macd_signal()
        df["macd_histogram"] = macd.macd_diff()

        adx_ind = ta.trend.ADXIndicator(df["high"], df["low"], df["close"], window=14)
        df["adx"]     = adx_ind.adx()
        df["adx_pos"] = adx_ind.adx_pos()
        df["adx_neg"] = adx_ind.adx_neg()

        df["psar"] = ta.trend.PSARIndicator(df["high"], df["low"], df["close"]).psar()

        indicators["trend"] = {
            "sma_20":  _f(df["sma_20"].iloc[-1]),
            "sma_50":  _f(df["sma_50"].iloc[-1]),
            "sma_200": _f(df["sma_200"].iloc[-1]),
            "ema_9":   _f(df["ema_9"].iloc[-1]),
            "ema_21":  _f(df["ema_21"].iloc[-1]),
            "macd_line":      _f(df["macd_line"].iloc[-1]),
            "macd_signal":    _f(df["macd_signal"].iloc[-1]),
            "macd_histogram": _f(df["macd_histogram"].iloc[-1]),
            "adx":     _f(df["adx"].iloc[-1]),
            "adx_pos": _f(df["adx_pos"].iloc[-1]),
            "adx_neg": _f(df["adx_neg"].iloc[-1]),
            "psar":    _f(df["psar"].iloc[-1]),
        }

        # Momentum
        df["rsi"]        = ta.momentum.RSIIndicator(df["close"], window=14).rsi()
        stoch            = ta.momentum.StochRSIIndicator(df["close"], window=14, smooth1=3, smooth2=3)
        df["stoch_k"]    = stoch.stochrsi_k()
        df["stoch_d"]    = stoch.stochrsi_d()
        df["williams_r"] = ta.momentum.WilliamsRIndicator(df["high"], df["low"], df["close"], lbp=14).williams_r()
        df["cci"]        = ta.trend.CCIIndicator(df["high"], df["low"], df["close"], window=20).cci()
        df["mfi"]        = ta.volume.MFIIndicator(df["high"], df["low"], df["close"], df["volume"], window=14).money_flow_index()

        indicators["momentum"] = {
            "rsi":       _f(df["rsi"].iloc[-1]),
            "rsi_prev":  _f(df["rsi"].iloc[-2]) if len(df) > 2 else _f(df["rsi"].iloc[-1]),
            "stoch_k":   _f(df["stoch_k"].iloc[-1]),
            "stoch_d":   _f(df["stoch_d"].iloc[-1]),
            "williams_r":_f(df["williams_r"].iloc[-1]),
            "cci":       _f(df["cci"].iloc[-1]),
            "mfi":       _f(df["mfi"].iloc[-1]),
        }

        # Volatilité
        bb = ta.volatility.BollingerBands(df["close"], window=20, window_dev=2)
        df["bb_upper"] = bb.bollinger_hband()
        df["bb_lower"] = bb.bollinger_lband()
        df["bb_pct"]   = bb.bollinger_pband()
        df["atr"]      = ta.volatility.AverageTrueRange(df["high"], df["low"], df["close"], window=14).average_true_range()
        df["atr_sma"]  = df["atr"].rolling(window=20).mean()

        atr_val   = _f(df["atr"].iloc[-1])
        atr_sma   = _f(df["atr_sma"].iloc[-1])
        atr_ratio = atr_val / atr_sma if atr_sma > 0 else 1.0

        indicators["volatility"] = {
            "bb_upper":  _f(df["bb_upper"].iloc[-1]),
            "bb_lower":  _f(df["bb_lower"].iloc[-1]),
            "bb_pct":    _f(df["bb_pct"].iloc[-1]),
            "atr":       atr_val,
            "atr_sma20": atr_sma,
            "atr_ratio": round(atr_ratio, 3),
        }

        # Volume
        df["obv"]       = ta.volume.OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()
        df["cmf"]       = ta.volume.ChaikinMoneyFlowIndicator(df["high"], df["low"], df["close"], df["volume"], window=20).chaikin_money_flow()
        df["vol_sma"]   = df["volume"].rolling(window=20).mean()
        vol_cur         = _f(df["volume"].iloc[-1])
        vol_sma         = _f(df["vol_sma"].iloc[-1])

        # Pente OBV (trend du volume)
        obv_series = df["obv"].dropna()
        obv_slope  = float(obv_series.iloc[-1] - obv_series.iloc[-5]) / (abs(obv_series.iloc[-5]) + 1e-9) if len(obv_series) >= 5 else 0

        indicators["volume"] = {
            "obv":        _f(df["obv"].iloc[-1]),
            "cmf":        _f(df["cmf"].iloc[-1]),
            "vol_sma":    vol_sma,
            "current":    vol_cur,
            "vol_ratio":  round(vol_cur / vol_sma, 3) if vol_sma > 0 else 1.0,
            "obv_slope":  round(obv_slope, 4),
        }

        # Niveaux
        r20 = df.tail(20)
        r50 = df.tail(50)
        indicators["levels"] = {
            "resistance": float(r20["high"].max()),
            "support":    float(r20["low"].min()),
            "high_50":    float(r50["high"].max()),
            "low_50":     float(r50["low"].min()),
        }

        # Résumé bougies
        indicators["candles_summary"] = [
            {"open": _f(r["open"]), "high": _f(r["high"]),
             "low":  _f(r["low"]),  "close": _f(r["close"]), "volume": _f(r["volume"])}
            for _, r in df.tail(5).iterrows()
        ]

        # Patterns + signal rule-based
        indicators["patterns"]    = detect_patterns(df)
        indicators["rule_signal"] = generate_momentum_signal(indicators, df)

        return indicators

    except Exception as e:
        logger.error(f"compute_indicators error: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# GÉNÉRATEUR DE SIGNAL MOMENTUM — stratégie principale
# ══════════════════════════════════════════════════════════════════════════════

def generate_momentum_signal(indicators: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    """
    Stratégie Momentum Scalper 5m.
    Basée sur la convergence de 5 facteurs techniques.
    Expectation positive dès 55% win rate avec ratio 1:2.
    """
    try:
        rsi      = indicators.get("momentum", {}).get("rsi", 50)
        rsi_prev = indicators.get("momentum", {}).get("rsi_prev", 50)
        macd_h   = indicators.get("trend", {}).get("macd_histogram", 0)
        macd_l   = indicators.get("trend", {}).get("macd_line", 0)
        macd_s   = indicators.get("trend", {}).get("macd_signal", 0)
        ema9     = indicators.get("trend", {}).get("ema_9", 0)
        ema21    = indicators.get("trend", {}).get("ema_21", 0)
        adx      = indicators.get("trend", {}).get("adx", 0)
        bb_pct   = indicators.get("volatility", {}).get("bb_pct", 0.5)
        atr_r    = indicators.get("volatility", {}).get("atr_ratio", 1.0)
        vol_r    = indicators.get("volume", {}).get("vol_ratio", 1.0)
        cmf      = indicators.get("volume", {}).get("cmf", 0)
        obv_sl   = indicators.get("volume", {}).get("obv_slope", 0)
        patterns = indicators.get("patterns", {})

        candles = indicators.get("candles_summary", [])
        close   = candles[-1]["close"] if candles else 0

        # ── Volatilité excessive → neutre ────────────────────────────────────
        if atr_r > 4.0:
            return {"action": "HOLD", "confidence": 0.25, "score": 0,
                    "momentum_score": 0, "reason": "Volatilite anormale"}

        # ── Score BUY — convergence momentum haussier ────────────────────────
        buy_pts = []

        # RSI
        if rsi < 25:        buy_pts.append(("RSI tres survendu", 4))
        elif rsi < 38:      buy_pts.append(("RSI survendu", 3))
        elif rsi < 48:      buy_pts.append(("RSI neutre bas", 2))
        elif rsi < 55:      buy_pts.append(("RSI leger achat", 1))

        # RSI en remontée (divergence haussière)
        if rsi > rsi_prev + 1:  buy_pts.append(("RSI en remontee", 1))

        # MACD
        if macd_h > 0:          buy_pts.append(("MACD positif", 2))
        if macd_l > macd_s:     buy_pts.append(("MACD croissement", 1))

        # EMA structure
        if close > ema9:        buy_pts.append(("Prix>EMA9", 1))
        if ema9 > ema21:        buy_pts.append(("EMA9>EMA21", 1))

        # Volume
        if vol_r > 1.15:        buy_pts.append(("Volume confirme", 1))
        if cmf > 0:             buy_pts.append(("CMF positif", 1))
        if obv_sl > 0:          buy_pts.append(("OBV haussier", 1))

        # Bollinger
        if bb_pct < 0.25:       buy_pts.append(("BB zone achat", 2))
        elif bb_pct < 0.4:      buy_pts.append(("BB neutre bas", 1))

        # Patterns haussiers
        bull_p = ["Hammer","Bullish Engulfing","Morning Star","Three White Soldiers",
                  "Bullish Marubozu","Inverted Hammer","Bullish Doji","Bullish Harami"]
        for p in patterns.get("detected", []):
            if p in bull_p:
                buy_pts.append((p, 2))

        buy_score = sum(v for _, v in buy_pts)

        # ── Score SELL ────────────────────────────────────────────────────────
        sell_pts = []

        if rsi > 78:            sell_pts.append(("RSI tres suracheté", 4))
        elif rsi > 68:          sell_pts.append(("RSI suracheté", 3))
        elif rsi > 60:          sell_pts.append(("RSI neutre haut", 2))
        elif rsi > 55:          sell_pts.append(("RSI leger vente", 1))

        if rsi < rsi_prev - 1: sell_pts.append(("RSI en baisse", 1))

        if macd_h < 0:          sell_pts.append(("MACD negatif", 2))
        if macd_l < macd_s:     sell_pts.append(("MACD negatif croissement", 1))

        if close < ema9:        sell_pts.append(("Prix<EMA9", 1))
        if ema9 < ema21:        sell_pts.append(("EMA9<EMA21", 1))

        if vol_r > 1.15:        sell_pts.append(("Volume confirme", 1))
        if cmf < 0:             sell_pts.append(("CMF negatif", 1))
        if obv_sl < 0:          sell_pts.append(("OBV baissier", 1))

        if bb_pct > 0.75:       sell_pts.append(("BB zone vente", 2))
        elif bb_pct > 0.6:      sell_pts.append(("BB neutre haut", 1))

        bear_p = ["Shooting Star","Bearish Engulfing","Evening Star","Three Black Crows",
                  "Bearish Marubozu","Hanging Man","Bearish Doji","Bearish Harami"]
        for p in patterns.get("detected", []):
            if p in bear_p:
                sell_pts.append((p, 2))

        sell_score = sum(v for _, v in sell_pts)

        # ── Décision : seuil 4 pts minimum ───────────────────────────────────
        THRESHOLD = 4

        if buy_score >= THRESHOLD and buy_score > sell_score:
            conf = min(0.40 + (buy_score - THRESHOLD) * 0.04, 0.92)
            reasons = [r for r, _ in buy_pts[:5]]
            return {
                "action":         "BUY",
                "confidence":     round(conf, 2),
                "score":          buy_score,
                "momentum_score": buy_score - sell_score,
                "reason":         "BUY score=" + str(buy_score) + " " + ", ".join(reasons),
                "buy_factors":    buy_pts,
            }

        elif sell_score >= THRESHOLD and sell_score > buy_score:
            conf = min(0.40 + (sell_score - THRESHOLD) * 0.04, 0.92)
            reasons = [r for r, _ in sell_pts[:5]]
            return {
                "action":         "SELL",
                "confidence":     round(conf, 2),
                "score":          sell_score,
                "momentum_score": sell_score - buy_score,
                "reason":         "SELL score=" + str(sell_score) + " " + ", ".join(reasons),
                "sell_factors":   sell_pts,
            }

        else:
            # HOLD — retourner le momentum net pour le classement relatif
            net = buy_score - sell_score
            return {
                "action":         "HOLD",
                "confidence":     0.30,
                "score":          max(buy_score, sell_score),
                "momentum_score": net,
                "reason":         "HOLD BUY=" + str(buy_score) + " SELL=" + str(sell_score),
            }

    except Exception as e:
        logger.warning(f"generate_momentum_signal error: {e}")
        return {"action": "HOLD", "confidence": 0.0, "score": 0, "momentum_score": 0, "reason": "Erreur calcul"}


# ══════════════════════════════════════════════════════════════════════════════
# PATTERNS CHANDELLES
# ══════════════════════════════════════════════════════════════════════════════

def detect_patterns(df: pd.DataFrame) -> Dict[str, Any]:
    detected: List[str] = []
    bull_count = bear_count = 0

    if len(df) < 5:
        return {"detected": [], "bullish_count": 0, "bearish_count": 0,
                "dominant_signal": "NEUTRAL", "pattern_strength": 0.0}

    c = df.tail(5).reset_index(drop=True)

    def body(i):         return abs(c["close"].iloc[i] - c["open"].iloc[i])
    def upper(i):        return c["high"].iloc[i] - max(c["close"].iloc[i], c["open"].iloc[i])
    def lower(i):        return min(c["close"].iloc[i], c["open"].iloc[i]) - c["low"].iloc[i]
    def bull(i):         return c["close"].iloc[i] > c["open"].iloc[i]
    def bear(i):         return c["close"].iloc[i] < c["open"].iloc[i]
    def rng(i):          return c["high"].iloc[i] - c["low"].iloc[i]

    L = 4

    # Haussiers
    if body(L) > 0 and lower(L) >= 2*body(L) and upper(L) <= .3*body(L) and bear(L-1):
        detected.append("Hammer"); bull_count += 1
    if body(L) > 0 and upper(L) >= 2*body(L) and lower(L) <= .3*body(L) and bear(L-1):
        detected.append("Inverted Hammer"); bull_count += 1
    if (bear(L-1) and bull(L) and
        c["open"].iloc[L] <= c["close"].iloc[L-1] and
        c["close"].iloc[L] >= c["open"].iloc[L-1]):
        detected.append("Bullish Engulfing"); bull_count += 2
    if (L >= 2 and bear(L-2) and body(L-1) <= .3*body(L-2) and bull(L) and
        c["close"].iloc[L] > (c["open"].iloc[L-2]+c["close"].iloc[L-2])/2):
        detected.append("Morning Star"); bull_count += 2
    if (L >= 2 and all(bull(L-i) for i in range(3)) and
        c["close"].iloc[L] > c["close"].iloc[L-1] > c["close"].iloc[L-2]):
        detected.append("Three White Soldiers"); bull_count += 3
    if bull(L) and rng(L) > 0 and body(L)/rng(L) > .90:
        detected.append("Bullish Marubozu"); bull_count += 2

    # Baissiers
    if body(L) > 0 and upper(L) >= 2*body(L) and lower(L) <= .3*body(L) and bull(L-1):
        detected.append("Shooting Star"); bear_count += 1
    if body(L) > 0 and lower(L) >= 2*body(L) and upper(L) <= .3*body(L) and bull(L-1):
        detected.append("Hanging Man"); bear_count += 1
    if (bull(L-1) and bear(L) and
        c["open"].iloc[L] >= c["close"].iloc[L-1] and
        c["close"].iloc[L] <= c["open"].iloc[L-1]):
        detected.append("Bearish Engulfing"); bear_count += 2
    if (L >= 2 and bull(L-2) and body(L-1) <= .3*body(L-2) and bear(L) and
        c["close"].iloc[L] < (c["open"].iloc[L-2]+c["close"].iloc[L-2])/2):
        detected.append("Evening Star"); bear_count += 2
    if (L >= 2 and all(bear(L-i) for i in range(3)) and
        c["close"].iloc[L] < c["close"].iloc[L-1] < c["close"].iloc[L-2]):
        detected.append("Three Black Crows"); bear_count += 3
    if bear(L) and rng(L) > 0 and body(L)/rng(L) > .90:
        detected.append("Bearish Marubozu"); bear_count += 2

    total = bull_count + bear_count
    if total == 0:
        dominant, strength = "NEUTRAL", 0.0
    elif bull_count > bear_count:
        dominant = "BULLISH"
        strength = min(1.0, bull_count / max(total, 1))
    elif bear_count > bull_count:
        dominant = "BEARISH"
        strength = min(1.0, bear_count / max(total, 1))
    else:
        dominant, strength = "NEUTRAL", 0.3

    return {
        "detected":        detected,
        "bullish_count":   bull_count,
        "bearish_count":   bear_count,
        "dominant_signal": dominant,
        "pattern_strength":round(strength, 3),
    }


def _f(value) -> float:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 0.0
        return round(float(value), 8)
    except (TypeError, ValueError):
        return 0.0
