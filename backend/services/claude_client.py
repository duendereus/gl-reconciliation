"""Claude API wrapper for GL reconciliation break analysis.

Sends each detected break to Claude for root-cause analysis, recommended
actions, traceability steps, and confidence scoring.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import time
from dataclasses import asdict, dataclass, field

import anthropic

from backend.services.reconciliation import Break

logger = logging.getLogger(__name__)

# Model — configurable via env. Default to Haiku for speed/cost in dev.
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")

# ---------------------------------------------------------------------------
# Data classes for Claude's analysis output
# ---------------------------------------------------------------------------

@dataclass
class TraceStep:
    label: str
    val: str  # HTML string
    conf: str = ""  # e.g. "96%"
    rule: str = ""  # e.g. "FX-001"


@dataclass
class BreakAnalysis:
    txn_id: str
    title: str
    body: str  # HTML with <strong>Root cause:</strong> and <strong>Recommended action:</strong>
    manual: str  # e.g. "~45 min"
    ai: str  # e.g. "3 sec"
    conf: str  # e.g. "96%"
    pri: str  # Low | Medium | High | Critical
    pri_color: str  # green | amber | red
    steps: list[TraceStep] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["priColor"] = d.pop("pri_color")
        return d


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a senior financial operations analyst specializing in GL reconciliation \
for a fintech company operating in Mexico. You analyze transaction breaks \
(discrepancies) and provide clear, actionable root-cause analysis.

You must respond with valid JSON only — no markdown, no commentary outside the JSON.

The JSON schema:
{
  "title": "string — break type + txn_id, e.g. 'FX Rate Discrepancy · TXN-4821'",
  "root_cause": "string — plain English explanation of WHY this break occurred, suitable for a COO",
  "recommended_action": "string — specific, actionable steps to resolve",
  "confidence": integer 0-100,
  "priority": "Low | Medium | High | Critical",
  "manual_time": "string — estimated manual resolution time, e.g. '~45 min'",
  "steps": [
    {
      "label": "string — step name, e.g. 'Transaction classification'",
      "value": "string — step detail, may include HTML <code> tags for values",
      "confidence": "string — optional, e.g. '96%'",
      "rule": "string — optional rule code, e.g. 'FX-001'"
    }
  ]
}
"""


def build_break_prompt(brk: Break, row_data: dict | None = None) -> str:
    """Build the user message for a single break analysis."""
    parts = [
        f"Analyze this GL reconciliation break:\n",
        f"- Transaction ID: {brk.txn_id}",
        f"- Break type: {brk.break_type}",
        f"- Description: {brk.description}",
        f"- Impact: {brk.impact_mxn:,.2f} MXN",
        f"- Severity: {brk.severity}",
    ]

    if brk.details:
        parts.append(f"- Detection details: {json.dumps(brk.details)}")

    if row_data:
        # Include relevant transaction fields
        relevant_keys = [
            "type", "amount_usd", "amount_mxn", "rate_applied",
            "rate_reference", "timestamp", "status", "counterparty",
            "client_tier", "client_id",
        ]
        tx_info = {k: v for k, v in row_data.items() if k in relevant_keys and v}
        if tx_info:
            parts.append(f"- Transaction data: {json.dumps(tx_info)}")

    parts.append(
        "\nProvide root cause analysis, recommended action, confidence score, "
        "priority level, estimated manual resolution time, and step-by-step "
        "traceability reasoning. Respond in JSON only."
    )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

_PRIORITY_COLOR = {
    "Low": "green",
    "Medium": "amber",
    "High": "red",
    "Critical": "red",
}


def _extract_json(raw: str) -> dict:
    """Extract a JSON object from Claude's response, tolerating markdown fences and preamble."""
    import re
    text = raw.strip()
    # Remove markdown fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Find first { ... last }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise json.JSONDecodeError("No JSON object found", text, 0)


def parse_claude_response(raw: str, brk: Break, elapsed_sec: float) -> BreakAnalysis:
    """Parse Claude's JSON response into a BreakAnalysis."""
    data = _extract_json(raw)

    priority = data.get("priority", brk.severity)
    confidence = data.get("confidence", 80)

    root_cause = data.get("root_cause", brk.description)
    action = data.get("recommended_action", "Review manually.")

    body = (
        f"<strong>Root cause:</strong> {root_cause}<br><br>"
        f"<strong>Recommended action:</strong> {action}"
    )

    steps = []
    for s in data.get("steps", []):
        steps.append(TraceStep(
            label=s.get("label", ""),
            val=s.get("value", ""),
            conf=s.get("confidence", ""),
            rule=s.get("rule", ""),
        ))

    return BreakAnalysis(
        txn_id=brk.txn_id,
        title=data.get("title", f"{brk.break_type} · {brk.txn_id}"),
        body=body,
        manual=data.get("manual_time", "~30 min"),
        ai=f"{math.ceil(elapsed_sec)} sec",
        conf=f"{confidence}%",
        pri=priority,
        pri_color=_PRIORITY_COLOR.get(priority, "amber"),
        steps=steps,
    )


def build_fallback_analysis(brk: Break) -> BreakAnalysis:
    """Return a reasonable analysis when Claude API is unavailable."""
    body = (
        f"<strong>Root cause:</strong> {brk.description}<br><br>"
        f"<strong>Recommended action:</strong> Review this {brk.break_type} break manually "
        f"and escalate if impact exceeds tolerance threshold."
    )
    return BreakAnalysis(
        txn_id=brk.txn_id,
        title=f"{brk.break_type} · {brk.txn_id}",
        body=body,
        manual="~30 min",
        ai="N/A",
        conf="N/A",
        pri=brk.severity,
        pri_color=_PRIORITY_COLOR.get(brk.severity, "amber"),
        steps=[
            TraceStep(label="Detection", val=f"Break type: <code>{brk.break_type}</code>"),
            TraceStep(
                label="Impact",
                val=f"<code>${brk.impact_mxn:,.2f} MXN</code>",
            ),
            TraceStep(label="Status", val="Pending manual review — F. AI temporarily unavailable"),
        ],
    )


# ---------------------------------------------------------------------------
# Main analysis function
# ---------------------------------------------------------------------------

BATCH_SIZE = 3  # Max concurrent Claude calls to stay under rate limits


async def _analyze_one(
    client: anthropic.AsyncAnthropic,
    brk: Break,
    row_data: dict | None,
    semaphore: asyncio.Semaphore | None = None,
) -> BreakAnalysis:
    """Analyze a single break with Claude. Retries on 429. Returns fallback on error."""
    user_msg = build_break_prompt(brk, row_data)
    last_raw = ""

    async def _call():
        nonlocal last_raw
        t0 = time.time()
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            temperature=0,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        elapsed = time.time() - t0
        last_raw = response.content[0].text
        return parse_claude_response(last_raw, brk, elapsed)

    for attempt in range(3):
        try:
            if semaphore:
                async with semaphore:
                    return await _call()
            else:
                return await _call()
        except json.JSONDecodeError:
            logger.warning("Parse fail for %s | raw: %.300s", brk.txn_id, last_raw)
            return build_fallback_analysis(brk)
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str or "rate_limit" in err_str:
                wait = 2 ** attempt * 3  # 3s, 6s, 12s
                logger.info("Rate limited for %s, retrying in %ds (attempt %d)", brk.txn_id, wait, attempt+1)
                await asyncio.sleep(wait)
                continue
            logger.warning("Claude API error for %s: %s", brk.txn_id, exc)
            return build_fallback_analysis(brk)

    logger.warning("Max retries for %s, using fallback", brk.txn_id)
    return build_fallback_analysis(brk)


def _cache_key(brk: Break) -> str:
    """Deterministic hash for a break to use as cache key."""
    raw = f"{brk.txn_id}|{brk.break_type}|{brk.impact_mxn}|{brk.description}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


async def analyze_breaks(
    breaks: list[Break],
    row_lookup: dict[str, dict] | None = None,
    db=None,
) -> list[BreakAnalysis]:
    """Analyze breaks with caching. Cached results are returned instantly.

    Only uncached breaks are sent to Claude (concurrently, max 5 at a time).
    Results are persisted to the ``analysis_cache`` table for future reuse.
    """
    from backend.models import AnalysisCache

    results: dict[str, BreakAnalysis] = {}  # keyed by txn_id
    uncached: list[Break] = []

    # 1. Check cache
    for brk in breaks:
        key = _cache_key(brk)
        cached = None
        if db:
            cached = db.query(AnalysisCache).filter(AnalysisCache.cache_key == key).first()
        if cached:
            try:
                d = json.loads(cached.analysis_json)
                results[brk.txn_id] = BreakAnalysis(
                    txn_id=d["txn_id"], title=d["title"], body=d["body"],
                    manual=d["manual"], ai=d["ai"], conf=d["conf"],
                    pri=d["pri"], pri_color=d.get("pri_color", d.get("priColor", "amber")),
                    steps=[TraceStep(**s) for s in d.get("steps", [])],
                )
            except Exception:
                uncached.append(brk)
        else:
            uncached.append(brk)

    if not uncached:
        logger.info("All %d analyses served from cache", len(breaks))
        return [results[b.txn_id] for b in breaks]

    logger.info("Cache: %d hit, %d miss — calling Claude (%s)", len(breaks)-len(uncached), len(uncached), CLAUDE_MODEL)

    # 2. Call Claude for uncached breaks
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — returning fallback analyses")
        for brk in uncached:
            results[brk.txn_id] = build_fallback_analysis(brk)
        return [results[b.txn_id] for b in breaks]

    try:
        client = anthropic.AsyncAnthropic(api_key=api_key)
    except Exception as exc:
        logger.error("Failed to create Anthropic client: %s", exc)
        for brk in uncached:
            results[brk.txn_id] = build_fallback_analysis(brk)
        return [results[b.txn_id] for b in breaks]

    sem = asyncio.Semaphore(BATCH_SIZE)
    tasks = [
        _analyze_one(client, brk, (row_lookup or {}).get(brk.txn_id), semaphore=sem)
        for brk in uncached
    ]
    new_analyses = await asyncio.gather(*tasks)

    # 3. Store in cache
    for brk, analysis in zip(uncached, new_analyses):
        results[brk.txn_id] = analysis
        if db:
            try:
                entry = AnalysisCache(
                    cache_key=_cache_key(brk),
                    txn_id=brk.txn_id,
                    analysis_json=json.dumps(analysis.to_dict()),
                )
                db.merge(entry)
                db.commit()
            except Exception:
                db.rollback()

    return [results[b.txn_id] for b in breaks]
