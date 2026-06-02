import asyncio
import json
import anthropic
from typing import Dict, Any, Optional
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_client  = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
MAX_RETRY = 2

SYSTEM_PROMPT = """Tu es ARIA, un validateur de signaux de trading crypto expert.

Tu reçois un signal technique pré-calculé et tu dois le VALIDER ou l'INVALIDER.

Réponds UNIQUEMENT avec ce JSON exact :
{
  "action": "BUY" | "SELL" | "HOLD",
  "confidence": float 0.0 à 1.0,
  "reasoning": "1-2 phrases max en français",
  "key_factors": ["facteur 1", "facteur 2", "facteur 3"],
  "risk_level": "LOW" | "MEDIUM" | "HIGH",
  "suggested_stop_loss_pct": float,
  "suggested_take_profit_pct": float,
  "market_regime": "TRENDING_UP" | "TRENDING_DOWN" | "RANGING" | "VOLATILE",
  "timeframe_alignment": "STRONG" | "MODERATE" | "WEAK",
  "entry_quality": float 0.0 à 1.0,
  "timeframe_confluence": {
    "4h": "BULLISH" | "BEARISH" | "NEUTRAL",
    "1h": "BULLISH" | "BEARISH" | "NEUTRAL",
    "15m": "BULLISH" | "BEARISH" | "NEUTRAL",
    "alignment_score": float
  }
}

RÈGLES STRICTES :
- Si le signal technique est BUY avec score >= 4 : confirme BUY sauf contradiction MAJEURE
- Si le signal technique est SELL avec score >= 4 : confirme SELL sauf contradiction MAJEURE
- Sois DÉCISIF. Un marché qui se consolide offre quand même des opportunités de scalping.
- TP cible : 1.5% pour scalp rapide, 3% pour swing. SL : 0.8-1.5%.
- Ne jamais retourner HOLD si le signal technique est >= 6 pts."""


async def analyze_market(
    symbol: str,
    indicators: Dict[str, Any],
    current_price: float,
    portfolio: Dict[str, Any],
    mtf_data: Optional[Dict] = None,
    retry_count: int = 0,
) -> Dict[str, Any]:
    """
    Claude valide le signal technique pré-calculé.
    Si Claude échoue → utilise directement le signal rule-based.
    """
    safe_portfolio = {
        "available_usdt": float(portfolio.get("available_usdt", 0)),
        "total_pnl_pct":  float(portfolio.get("total_pnl_pct", 0)),
        "win_rate":       float(portfolio.get("win_rate", 0)),
    }

    rule_sig  = indicators.get("rule_signal", {})
    r_action  = rule_sig.get("action", "HOLD")
    r_score   = rule_sig.get("score", 0)
    r_conf    = rule_sig.get("confidence", 0.30)
    r_reason  = rule_sig.get("reason", "")

    # Données clés pour le prompt
    rsi    = indicators.get("momentum", {}).get("rsi", 50)
    macd_h = indicators.get("trend", {}).get("macd_histogram", 0)
    ema9   = indicators.get("trend", {}).get("ema_9", 0)
    ema21  = indicators.get("trend", {}).get("ema_21", 0)
    adx    = indicators.get("trend", {}).get("adx", 0)
    bb_pct = indicators.get("volatility", {}).get("bb_pct", 0.5)
    vol_r  = indicators.get("volume", {}).get("vol_ratio", 1.0)
    cmf    = indicators.get("volume", {}).get("cmf", 0)
    patterns = indicators.get("patterns", {}).get("detected", [])
    support    = indicators.get("levels", {}).get("support", 0)
    resistance = indicators.get("levels", {}).get("resistance", 0)

    prompt = f"""Signal technique {r_action} pour {symbol} — VALIDATION REQUISE

PRIX : ${current_price:.4f}  |  Capital dispo : ${safe_portfolio['available_usdt']:.2f}

SIGNAL PRÉ-CALCULÉ : {r_action} (score={r_score}/15, conf={r_conf:.0%})
Raisons : {r_reason}

INDICATEURS :
RSI={rsi:.1f} {'[SURVENDU]' if rsi < 35 else '[SURACHETÉ]' if rsi > 65 else '[neutre]'}
MACD histo={macd_h:.6f} | EMA9={ema9:.2f} {'>' if ema9>ema21 else '<'} EMA21={ema21:.2f}
ADX={adx:.1f} | BB_pct={bb_pct:.2f} | Volume={vol_r:.1f}x | CMF={cmf:.3f}
Support=${support:.2f} | Résistance=${resistance:.2f}
Patterns: {', '.join(patterns) if patterns else 'Aucun'}

PORTEFEUILLE : P&L={safe_portfolio['total_pnl_pct']:.1f}% | WinRate={safe_portfolio['win_rate']:.0f}%

VALIDE ce signal. Si score >= 6, confirme SANS HÉSITATION.
Pour le scalping 5m : TP=1.5%, SL=0.8% est optimal.
Retourne UNIQUEMENT le JSON."""

    try:
        response = await _client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

        if not response.content:
            return _from_rule(rule_sig, indicators)

        raw    = response.content[0].text.strip()
        result = _parse(raw)
        if result:
            validated = _validate(result)
            # Override si Claude devient trop conservateur sur un bon signal
            if validated["action"] == "HOLD" and r_action != "HOLD" and r_score >= 6:
                logger.info(f"Claude HOLD overridden: {r_action} score={r_score}")
                validated["action"]     = r_action
                validated["confidence"] = max(validated["confidence"], r_conf)
                validated["key_factors"].insert(0, f"Rule override score={r_score}")
            return validated

        return _from_rule(rule_sig, indicators)

    except anthropic.RateLimitError:
        if retry_count < MAX_RETRY:
            await asyncio.sleep(20)
            return await analyze_market(symbol, indicators, current_price, portfolio, mtf_data, retry_count+1)
        logger.warning("Rate limit max retries — using rule-based")
        return _from_rule(rule_sig, indicators)

    except anthropic.APIStatusError as e:
        if e.status_code == 529 and retry_count < MAX_RETRY:
            await asyncio.sleep(30)
            return await analyze_market(symbol, indicators, current_price, portfolio, mtf_data, retry_count+1)
        logger.warning(f"Claude {e.status_code} — using rule-based")
        return _from_rule(rule_sig, indicators)

    except Exception as e:
        logger.error(f"Claude error: {e}")
        return _from_rule(rule_sig, indicators)


def _from_rule(rule_sig: Dict, indicators: Dict) -> Dict:
    """Fallback direct sur le signal rule-based quand Claude est indisponible."""
    action     = rule_sig.get("action", "HOLD")
    confidence = rule_sig.get("confidence", 0.35)
    reason     = rule_sig.get("reason", "Signal technique")
    score      = rule_sig.get("score", 0)

    rsi    = indicators.get("momentum", {}).get("rsi", 50)
    macd_h = indicators.get("trend", {}).get("macd_histogram", 0)
    adx    = indicators.get("trend", {}).get("adx", 0)

    regime = "RANGING"
    if adx > 25:
        regime = "TRENDING_UP" if macd_h > 0 else "TRENDING_DOWN"

    # TP/SL adaptés au scalping
    sl = 0.8 if confidence >= 0.7 else 1.5
    tp = 1.5 if confidence >= 0.7 else 3.0

    logger.info(f"Rule-based signal: {action} conf={confidence:.0%} score={score} ({reason})")

    return {
        "action": action, "confidence": confidence,
        "reasoning": f"[Auto] {reason}. RSI={rsi:.0f} MACD={macd_h:.5f}",
        "key_factors": [reason, f"RSI={rsi:.0f}", f"score={score}"],
        "risk_level": "LOW" if confidence >= 0.7 else "MEDIUM",
        "suggested_stop_loss_pct":   sl,
        "suggested_take_profit_pct": tp,
        "market_regime":   regime,
        "timeframe_alignment": "MODERATE",
        "entry_quality": confidence,
        "timeframe_confluence": {
            "4h": "NEUTRAL", "1h": "NEUTRAL", "15m": "NEUTRAL", "alignment_score": 0.5
        },
        "source": "rule_based",
    }


def _parse(text: str) -> Optional[Dict]:
    try:
        for prefix in ["```json", "```"]:
            if prefix in text:
                text = text.split(prefix)[1].split("```")[0].strip()
                break
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            s, e = text.find("{"), text.rfind("}") + 1
            if s != -1 and e > s:
                return json.loads(text[s:e])
        except Exception:
            pass
    return None


def _validate(data: Dict) -> Dict:
    VALID_A = {"BUY", "SELL", "HOLD"}
    VALID_R = {"LOW", "MEDIUM", "HIGH"}
    VALID_M = {"TRENDING_UP", "TRENDING_DOWN", "RANGING", "VOLATILE"}
    VALID_AL= {"STRONG", "MODERATE", "WEAK"}
    VALID_TF= {"BULLISH", "BEARISH", "NEUTRAL"}

    def clamp(v, lo, hi, d):
        try: return max(lo, min(hi, float(v)))
        except: return d

    data["action"] = str(data.get("action","HOLD")).upper()
    if data["action"] not in VALID_A: data["action"] = "HOLD"

    data["confidence"]                = clamp(data.get("confidence"),              0.0, 1.0,  0.5)
    data["suggested_stop_loss_pct"]   = clamp(data.get("suggested_stop_loss_pct"), 0.3, 5.0,  0.8)
    data["suggested_take_profit_pct"] = clamp(data.get("suggested_take_profit_pct"),0.5, 15.0, 1.5)
    data["entry_quality"]             = clamp(data.get("entry_quality"),            0.0, 1.0,  0.5)

    data["risk_level"] = str(data.get("risk_level","MEDIUM")).upper()
    if data["risk_level"] not in VALID_R: data["risk_level"] = "MEDIUM"

    data["market_regime"] = str(data.get("market_regime","RANGING")).upper()
    if data["market_regime"] not in VALID_M: data["market_regime"] = "RANGING"

    data["timeframe_alignment"] = str(data.get("timeframe_alignment","MODERATE")).upper()
    if data["timeframe_alignment"] not in VALID_AL: data["timeframe_alignment"] = "MODERATE"

    data["reasoning"]   = str(data.get("reasoning",""))[:1000]
    raw_f = data.get("key_factors", [])
    data["key_factors"] = [str(f) for f in raw_f] if isinstance(raw_f, list) else []

    raw_mtf = data.get("timeframe_confluence", {})
    if not isinstance(raw_mtf, dict): raw_mtf = {}
    mtf = {}
    for tf in ["4h","1h","15m"]:
        v = str(raw_mtf.get(tf,"NEUTRAL")).upper()
        mtf[tf] = v if v in VALID_TF else "NEUTRAL"
    mtf["alignment_score"] = clamp(raw_mtf.get("alignment_score"), 0.0, 1.0, 0.5)
    data["timeframe_confluence"] = mtf
    data["source"] = "claude"

    return data


def _default_hold() -> Dict:
    return {
        "action": "HOLD", "confidence": 0.0,
        "reasoning": "Analyse indisponible.",
        "key_factors": [], "risk_level": "HIGH",
        "suggested_stop_loss_pct": 0.8,
        "suggested_take_profit_pct": 1.5,
        "market_regime": "RANGING",
        "timeframe_alignment": "WEAK",
        "entry_quality": 0.0,
        "timeframe_confluence": {"4h":"NEUTRAL","1h":"NEUTRAL","15m":"NEUTRAL","alignment_score":0.0},
        "source": "default",
    }
