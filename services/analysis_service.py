import pandas as pd
import numpy as np
import ta
from typing import Dict, Any, List, Tuple, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# INDICATEURS TECHNIQUES COMPLETS
# ══════════════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame, symbol: str = "") -> Dict[str, Any]:
    MIN_CANDLES = 30
    if df is None or len(df) < MIN_CANDLES:
        logger.warning(f"compute_indicators: {symbol} — données insuffisantes ({len(df) if df is not None else 0} bougies < {MIN_CANDLES}) — skip")
        return {}
    try:
        df = df.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        indicators: Dict[str, Any] = {"_symbol": symbol}

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

        # ADX direction — tendance qui se renforce ou s'affaiblit
        adx_series = df["adx"].dropna()
        adx_now    = _f(adx_series.iloc[-1]) if len(adx_series) > 0 else 0
        adx_4ago   = _f(adx_series.iloc[-4]) if len(adx_series) >= 4 else adx_now
        adx_rising = adx_now > adx_4ago + 0.5

        indicators["trend"] = {
            "sma_20":  _f(df["sma_20"].iloc[-1]),
            "sma_50":  _f(df["sma_50"].iloc[-1]),
            "sma_200": _f(df["sma_200"].iloc[-1]),
            "ema_9":   _f(df["ema_9"].iloc[-1]),
            "ema_21":  _f(df["ema_21"].iloc[-1]),
            "macd_line":      _f(df["macd_line"].iloc[-1]),
            "macd_signal":    _f(df["macd_signal"].iloc[-1]),
            "macd_histogram": _f(df["macd_histogram"].iloc[-1]),
            "adx":     adx_now,
            "adx_pos": _f(df["adx_pos"].iloc[-1]),
            "adx_neg": _f(df["adx_neg"].iloc[-1]),
            "adx_rising": adx_rising,
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
            "rsi_3ago":  _f(df["rsi"].iloc[-4]) if len(df) > 4 else _f(df["rsi"].iloc[-1]),
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

        # BB Squeeze — consolidation avant explosion de prix
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / (df["close"].replace(0, 1))
        bb_width_now  = float(df["bb_width"].iloc[-1])
        bb_width_prev = float(df["bb_width"].iloc[-2]) if len(df) > 2 else bb_width_now
        bb_width_20lo = float(df["bb_width"].tail(20).min())
        bb_squeeze    = bb_width_now <= bb_width_20lo * 1.15   # dans les 15% du minimum 20 bougies
        bb_expanding  = bb_width_now > bb_width_prev * 1.02    # BB qui s'élargit = expansion en cours

        indicators["volatility"] = {
            "bb_upper":     _f(df["bb_upper"].iloc[-1]),
            "bb_lower":     _f(df["bb_lower"].iloc[-1]),
            "bb_pct":       _f(df["bb_pct"].iloc[-1]),
            "bb_width":     round(bb_width_now, 6),
            "bb_squeeze":   bb_squeeze,
            "bb_expanding": bb_expanding,
            "atr":          atr_val,
            "atr_sma20":    atr_sma,
            "atr_ratio":    round(atr_ratio, 3),
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

        # VWAP session — biais institutionnel intraday
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["vwap"] = (df["typical_price"] * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, 1)
        vwap_val = _f(df["vwap"].iloc[-1])

        indicators["volume"] = {
            "obv":        _f(df["obv"].iloc[-1]),
            "cmf":        _f(df["cmf"].iloc[-1]),
            "vol_sma":    vol_sma,
            "current":    vol_cur,
            "vol_ratio":  round(vol_cur / vol_sma, 3) if vol_sma > 0 else 1.0,
            "obv_slope":  round(obv_slope, 4),
            "vwap":       vwap_val,
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

        # Résumé bougies — 15 bougies pour l'anti-pump 1h
        indicators["candles_summary"] = [
            {"open": _f(r["open"]), "high": _f(r["high"]),
             "low":  _f(r["low"]),  "close": _f(r["close"]), "volume": _f(r["volume"])}
            for _, r in df.tail(15).iterrows()
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
    Stratégie Momentum Scalper 5m — v2 avec filtres ADX, volume directionnel, RSI confirmé.
    """
    try:
        rsi        = indicators.get("momentum", {}).get("rsi", 50)
        rsi_prev   = indicators.get("momentum", {}).get("rsi_prev", 50)
        rsi_3ago   = indicators.get("momentum", {}).get("rsi_3ago", rsi_prev)
        macd_h     = indicators.get("trend", {}).get("macd_histogram", 0)
        macd_l     = indicators.get("trend", {}).get("macd_line", 0)
        macd_s     = indicators.get("trend", {}).get("macd_signal", 0)
        ema9       = indicators.get("trend", {}).get("ema_9", 0)
        ema21      = indicators.get("trend", {}).get("ema_21", 0)
        adx        = indicators.get("trend", {}).get("adx", 0)
        adx_rising = indicators.get("trend", {}).get("adx_rising", False)
        bb_pct     = indicators.get("volatility", {}).get("bb_pct", 0.5)
        bb_squeeze  = indicators.get("volatility", {}).get("bb_squeeze", False)
        bb_expanding= indicators.get("volatility", {}).get("bb_expanding", False)
        atr_r      = indicators.get("volatility", {}).get("atr_ratio", 1.0)
        vol_r      = indicators.get("volume", {}).get("vol_ratio", 1.0)
        cmf        = indicators.get("volume", {}).get("cmf", 0)
        vwap       = indicators.get("volume", {}).get("vwap", 0)
        mfi        = indicators.get("momentum", {}).get("mfi", 50)
        stoch_k    = indicators.get("momentum", {}).get("stoch_k", 50)
        stoch_d    = indicators.get("momentum", {}).get("stoch_d", 50)
        support    = indicators.get("levels", {}).get("support", 0)
        resistance = indicators.get("levels", {}).get("resistance", 0)
        obv_sl   = indicators.get("volume", {}).get("obv_slope", 0)
        patterns = indicators.get("patterns", {})

        candles = indicators.get("candles_summary", [])
        close   = candles[-1]["close"] if candles else 0
        last_open = candles[-1]["open"] if candles else close
        last_candle_bull = close > last_open  # bougie haussière

        # ── Volatilité excessive → neutre ────────────────────────────────────
        if atr_r > 4.0:
            return {"action": "HOLD", "confidence": 0.25, "score": 0,
                    "momentum_score": 0, "reason": "Volatilite anormale"}

        # ADX < 20 → marché en range, indicateurs de tendance peu fiables
        trending = adx >= 20

        # ── Score BUY — convergence momentum haussier ────────────────────────
        buy_pts = []

        # RSI
        if rsi < 25:        buy_pts.append(("RSI tres survendu", 4))
        elif rsi < 38:      buy_pts.append(("RSI survendu", 3))
        elif rsi < 45:      buy_pts.append(("RSI neutre bas", 2))

        # RSI en remontée confirmée sur 2 bougies (plus fiable qu'une seule)
        rsi_rising_confirmed = rsi > rsi_prev + 0.5 and rsi_prev > rsi_3ago + 0.5
        if rsi_rising_confirmed:
            buy_pts.append(("RSI remontee confirmee 2 bougies", 2))
        elif rsi > rsi_prev + 1:
            buy_pts.append(("RSI en remontee", 1))

        # MACD — seulement si marché en tendance (ADX >= 20)
        if trending:
            if macd_h > 0:      buy_pts.append(("MACD positif", 2))
            if macd_l > macd_s: buy_pts.append(("MACD croissement", 1))

        # EMA structure
        if close > ema9:    buy_pts.append(("Prix>EMA9", 1))
        if trending and ema9 > ema21: buy_pts.append(("EMA9>EMA21", 1))

        # Volume directionnel — spike réel requis, bougie haussière
        if vol_r > 1.5 and last_candle_bull:   buy_pts.append(("Volume haussier fort", 2))
        elif vol_r > 1.2 and last_candle_bull: buy_pts.append(("Volume haussier", 1))
        if cmf > 0:             buy_pts.append(("CMF positif", 1))
        if obv_sl > 0:          buy_pts.append(("OBV haussier", 1))

        # Bollinger
        if bb_pct < 0.25:       buy_pts.append(("BB zone achat", 2))
        elif bb_pct < 0.4:      buy_pts.append(("BB neutre bas", 1))

        # BB Squeeze breakout — setup le plus puissant : consolidation → explosion
        if bb_squeeze and bb_expanding and last_candle_bull:
            buy_pts.append(("BB Squeeze breakout haussier", 3))
        elif bb_squeeze and bb_expanding:
            buy_pts.append(("BB Squeeze expansion", 1))

        # ADX croissant = trend qui se renforce = meilleur timing d'entrée
        if adx_rising and trending:
            if macd_h > 0: buy_pts.append(("ADX croissant + MACD positif", 2))
            else:          buy_pts.append(("ADX croissant", 1))

        # Proximité support — rebond potentiel
        if close > 0 and support > 0:
            dist_supp = (close - support) / close * 100
            if 0 < dist_supp <= 1.2:
                buy_pts.append(("Prix pres support 20p", 2))

        # Proximité résistance — penalite buy (risque de rejet)
        if close > 0 and resistance > 0 and resistance > close:
            dist_res = (resistance - close) / close * 100
            if 0 < dist_res <= 0.8:
                buy_pts.append(("Prix pres resistance (malus)", -2))

        # VWAP — biais institutionnel intraday (prix > VWAP = acheteurs dominants)
        if vwap > 0 and close > 0:
            if close > vwap * 1.001:
                buy_pts.append(("Prix au-dessus VWAP", 1))
            elif close < vwap * 0.999:
                pass  # sous VWAP → neutre pour BUY (traité dans sell_pts)

        # MFI — Money Flow Index (volume-weighted RSI)
        if mfi < 25:    buy_pts.append(("MFI survendu <25", 2))
        elif mfi < 40:  buy_pts.append(("MFI zone achat", 1))

        # StochRSI — oscillateur rapide pour timing entrée
        if stoch_k < 20 and stoch_k > stoch_d:
            buy_pts.append(("StochRSI croise haussier bas", 2))
        elif stoch_k < 25:
            buy_pts.append(("StochRSI survendu", 1))

        # Patterns haussiers
        bull_p = ["Hammer","Bullish Engulfing","Morning Star","Three White Soldiers",
                  "Bullish Marubozu","Inverted Hammer","Bullish Doji","Bullish Harami"]
        for p in patterns.get("detected", []):
            if p in bull_p:
                buy_pts.append((p, 2))

        # Divergence RSI haussière — signal BEAR le plus fiable
        # Prix fait un nouveau bas mais RSI ne suit pas → capitulation épuisée → rebond fort
        if len(df) >= 10:
            try:
                rsi_series   = df["rsi"].dropna()
                close_series = df["close"]
                if len(rsi_series) >= 10:
                    # Comparer la bougie actuelle vs 8 bougies en arrière
                    close_now  = float(close_series.iloc[-1])
                    close_back = float(close_series.iloc[-9])
                    rsi_now    = float(rsi_series.iloc[-1])
                    rsi_back   = float(rsi_series.iloc[-9])
                    # Divergence haussière : prix plus bas ET RSI plus haut
                    if close_now < close_back * 0.998 and rsi_now > rsi_back + 2.0 and rsi_now < 40:
                        buy_pts.append(("Divergence RSI haussiere", 3))
            except Exception:
                pass

        # RSI cross haussier — meilleur signal de rebond : capitulation CONFIRMÉE
        # Acheter la remontée confirmée, pas la chute (was < 30 → now > 32)
        if rsi_prev < 30 and rsi > 32:
            buy_pts.append(("RSI cross haussier — capitulation terminee", 5))

        buy_score = sum(v for _, v in buy_pts)

        # ── Score SELL ────────────────────────────────────────────────────────
        sell_pts = []

        if rsi > 78:            sell_pts.append(("RSI tres surachete", 4))
        elif rsi > 68:          sell_pts.append(("RSI surachete", 3))
        elif rsi > 60:          sell_pts.append(("RSI neutre haut", 2))
        elif rsi > 55:          sell_pts.append(("RSI leger vente", 1))

        rsi_falling_confirmed = rsi < rsi_prev - 0.5 and rsi_prev < rsi_3ago - 0.5
        if rsi_falling_confirmed:
            sell_pts.append(("RSI baisse confirmee 2 bougies", 2))
        elif rsi < rsi_prev - 1:
            sell_pts.append(("RSI en baisse", 1))

        if trending:
            if macd_h < 0:      sell_pts.append(("MACD negatif", 2))
            if macd_l < macd_s: sell_pts.append(("MACD negatif croissement", 1))

        if close < ema9:        sell_pts.append(("Prix<EMA9", 1))
        if trending and ema9 < ema21: sell_pts.append(("EMA9<EMA21", 1))

        if vol_r > 1.15 and not last_candle_bull: sell_pts.append(("Volume baissier", 1))
        if cmf < 0:             sell_pts.append(("CMF negatif", 1))
        if obv_sl < 0:          sell_pts.append(("OBV baissier", 1))

        if bb_pct > 0.75:       sell_pts.append(("BB zone vente", 2))
        elif bb_pct > 0.6:      sell_pts.append(("BB neutre haut", 1))

        # BB Squeeze breakout baissier
        if bb_squeeze and bb_expanding and not last_candle_bull:
            sell_pts.append(("BB Squeeze breakout baissier", 3))

        # ADX croissant en tendance baissière
        if adx_rising and trending and macd_h < 0:
            sell_pts.append(("ADX croissant + MACD negatif", 2))

        # VWAP — sous VWAP = vendeurs dominants
        if vwap > 0 and close > 0 and close < vwap * 0.999:
            sell_pts.append(("Prix sous VWAP", 1))

        # MFI surachetée
        if mfi > 75:    sell_pts.append(("MFI surachete >75", 2))
        elif mfi > 60:  sell_pts.append(("MFI zone vente", 1))

        # StochRSI croise baissier en haut
        if stoch_k > 80 and stoch_k < stoch_d:
            sell_pts.append(("StochRSI croise baissier haut", 2))
        elif stoch_k > 78:
            sell_pts.append(("StochRSI surachete", 1))

        # Proximité résistance — rejet probable
        if close > 0 and resistance > 0 and resistance > close:
            dist_res = (resistance - close) / close * 100
            if 0 < dist_res <= 1.2:
                sell_pts.append(("Prix pres resistance 20p", 2))

        bear_p = ["Shooting Star","Bearish Engulfing","Evening Star","Three Black Crows",
                  "Bearish Marubozu","Hanging Man","Bearish Doji","Bearish Harami"]
        for p in patterns.get("detected", []):
            if p in bear_p:
                sell_pts.append((p, 2))

        sell_score = sum(v for _, v in sell_pts)

        # ── Biais TradingAgents Intelligence ─────────────────────────────────
        from services.research_reader import get_bias_sync
        _bias = get_bias_sync(indicators.get("_symbol", ""))
        if _bias:
            _bs = _bias["bias_score"]
            if _bs > 0:
                buy_score  += _bs
            elif _bs < 0:
                sell_score += abs(_bs)

        # ── Décision : seuil 5 pts minimum ───────────────────────────────────
        THRESHOLD = 5

        if buy_score >= THRESHOLD and buy_score > sell_score:
            # Confiance multi-factorielle calibrée pour atteindre 0.65+ sur bons signaux
            base_conf = 0.48 + (buy_score - THRESHOLD) * 0.05
            separation = buy_score - sell_score
            # Bonus ADX — tendance confirmée
            if adx > 30:   base_conf += 0.08
            elif adx > 22: base_conf += 0.05
            elif adx > 15: base_conf += 0.02
            # Bonus séparation nette
            if separation >= 7: base_conf += 0.07
            elif separation >= 5: base_conf += 0.04
            elif separation >= 3: base_conf += 0.02
            # Bonus RSI oversold
            if rsi < 35: base_conf += 0.05
            elif rsi < 45: base_conf += 0.02
            # Bonus patterns haussiers
            patterns_data = indicators.get("patterns", {})
            if patterns_data.get("bullish_count", 0) >= 2: base_conf += 0.05
            conf = min(base_conf, 0.92)
            reasons = [r for r, _ in buy_pts[:5]]
            return {
                "action":         "BUY",
                "confidence":     round(conf, 2),
                "score":          buy_score,
                "momentum_score": buy_score - sell_score,
                "reason":         "BUY score=" + str(buy_score) + " " + ", ".join(reasons),
                "buy_factors":    buy_pts,
                "adx_trending":   trending,
            }

        elif sell_score >= THRESHOLD and sell_score > buy_score:
            base_conf = 0.48 + (sell_score - THRESHOLD) * 0.05
            separation = sell_score - buy_score
            if adx > 30:   base_conf += 0.08
            elif adx > 22: base_conf += 0.05
            elif adx > 15: base_conf += 0.02
            if separation >= 7: base_conf += 0.07
            elif separation >= 5: base_conf += 0.04
            elif separation >= 3: base_conf += 0.02
            if rsi > 68: base_conf += 0.05
            elif rsi > 60: base_conf += 0.02
            patterns_data = indicators.get("patterns", {})
            if patterns_data.get("bearish_count", 0) >= 2: base_conf += 0.05
            conf = min(base_conf, 0.92)
            reasons = [r for r, _ in sell_pts[:5]]
            return {
                "action":         "SELL",
                "confidence":     round(conf, 2),
                "score":          sell_score,
                "momentum_score": sell_score - buy_score,
                "reason":         "SELL score=" + str(sell_score) + " " + ", ".join(reasons),
                "sell_factors":   sell_pts,
                "adx_trending":   trending,
            }

        else:
            net = buy_score - sell_score
            return {
                "action":         "HOLD",
                "confidence":     0.30,
                "score":          max(buy_score, sell_score),
                "momentum_score": net,
                "reason":         "HOLD BUY=" + str(buy_score) + " SELL=" + str(sell_score),
                "adx_trending":   trending,
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
