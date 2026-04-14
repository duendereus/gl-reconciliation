"""Tests for the Claude API client — prompt building, response parsing, fallback."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.services.claude_client import (
    BreakAnalysis,
    TraceStep,
    analyze_breaks,
    build_break_prompt,
    build_fallback_analysis,
    parse_claude_response,
)
from backend.services.reconciliation import Break


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _break(**overrides) -> Break:
    defaults = {
        "txn_id": "TXN-4821",
        "break_type": "FX_RATE",
        "description": "Applied rate 17.23 vs reference 17.19 (delta 0.04 MXN/USD)",
        "impact_mxn": 340.0,
        "severity": "Medium",
        "details": {"rate_applied": 17.23, "rate_reference": 17.19, "rate_delta": 0.04},
    }
    defaults.update(overrides)
    return Break(**defaults)


VALID_CLAUDE_RESPONSE = json.dumps({
    "title": "FX Rate Discrepancy · TXN-4821",
    "root_cause": "Applied rate (17.23) differs from BANXICO closing (17.19). Delta of 0.04 per unit across $8,500 USD.",
    "recommended_action": "Auto-generate accounting adjustment entry. Notify treasury team.",
    "confidence": 96,
    "priority": "Medium",
    "manual_time": "~45 min",
    "steps": [
        {"label": "Transaction classification", "value": "Type: <code>FX_TRANSFER</code>", "confidence": "", "rule": ""},
        {"label": "Rate extraction", "value": "Rate applied: <code>17.23 MXN/USD</code>", "confidence": "", "rule": ""},
        {"label": "Delta calculation", "value": "Delta: <code>0.04</code> · Impact: <code>$340 MXN</code>", "confidence": "96%", "rule": "FX-001"},
    ],
})


# ===== build_break_prompt =====================================================

class TestBuildBreakPrompt:
    def test_contains_txn_id(self):
        prompt = build_break_prompt(_break())
        assert "TXN-4821" in prompt

    def test_contains_break_type(self):
        prompt = build_break_prompt(_break())
        assert "FX_RATE" in prompt

    def test_contains_impact(self):
        prompt = build_break_prompt(_break())
        assert "340.00 MXN" in prompt

    def test_includes_row_data(self):
        row = {"type": "FX_TRANSFER", "amount_usd": 8500, "counterparty": "BBVA Mexico"}
        prompt = build_break_prompt(_break(), row_data=row)
        assert "FX_TRANSFER" in prompt
        assert "BBVA Mexico" in prompt

    def test_handles_empty_details(self):
        brk = _break(details={})
        prompt = build_break_prompt(brk)
        assert "TXN-4821" in prompt

    def test_handles_no_row_data(self):
        prompt = build_break_prompt(_break(), row_data=None)
        assert "TXN-4821" in prompt


# ===== parse_claude_response ==================================================

class TestParseClaudeResponse:
    def test_parses_valid_json(self):
        brk = _break()
        analysis = parse_claude_response(VALID_CLAUDE_RESPONSE, brk, elapsed_sec=2.5)
        assert isinstance(analysis, BreakAnalysis)
        assert analysis.txn_id == "TXN-4821"
        assert analysis.title == "FX Rate Discrepancy · TXN-4821"
        assert "Root cause:" in analysis.body
        assert "Recommended action:" in analysis.body
        assert analysis.conf == "96%"
        assert analysis.pri == "Medium"
        assert analysis.pri_color == "amber"
        assert analysis.ai == "3 sec"  # ceil(2.5)
        assert len(analysis.steps) == 3

    def test_strips_markdown_fences(self):
        wrapped = f"```json\n{VALID_CLAUDE_RESPONSE}\n```"
        analysis = parse_claude_response(wrapped, _break(), 1.0)
        assert analysis.title == "FX Rate Discrepancy · TXN-4821"

    def test_elapsed_time_formatted(self):
        analysis = parse_claude_response(VALID_CLAUDE_RESPONSE, _break(), 4.7)
        assert analysis.ai == "5 sec"

    def test_raises_on_invalid_json(self):
        with pytest.raises(json.JSONDecodeError):
            parse_claude_response("not json", _break(), 1.0)

    def test_priority_colors(self):
        for pri, color in [("Low", "green"), ("Medium", "amber"), ("High", "red"), ("Critical", "red")]:
            resp = json.dumps({
                "title": "Test",
                "root_cause": "test",
                "recommended_action": "test",
                "confidence": 80,
                "priority": pri,
                "manual_time": "~30 min",
                "steps": [],
            })
            analysis = parse_claude_response(resp, _break(), 1.0)
            assert analysis.pri_color == color

    def test_steps_parsing(self):
        analysis = parse_claude_response(VALID_CLAUDE_RESPONSE, _break(), 1.0)
        step = analysis.steps[2]
        assert step.label == "Delta calculation"
        assert "0.04" in step.val
        assert step.conf == "96%"
        assert step.rule == "FX-001"


# ===== build_fallback_analysis ================================================

class TestBuildFallbackAnalysis:
    def test_returns_analysis_with_break_info(self):
        brk = _break()
        fb = build_fallback_analysis(brk)
        assert fb.txn_id == "TXN-4821"
        assert "Root cause:" in fb.body
        assert fb.ai == "N/A"
        assert fb.conf == "N/A"
        assert fb.pri == "Medium"
        assert len(fb.steps) == 3

    def test_severity_mapped_to_priority(self):
        brk = _break(severity="Critical")
        fb = build_fallback_analysis(brk)
        assert fb.pri == "Critical"
        assert fb.pri_color == "red"


# ===== BreakAnalysis.to_dict ==================================================

class TestBreakAnalysisToDict:
    def test_pri_color_becomes_camel_case(self):
        analysis = parse_claude_response(VALID_CLAUDE_RESPONSE, _break(), 1.0)
        d = analysis.to_dict()
        assert "priColor" in d
        assert "pri_color" not in d

    def test_contains_all_fields(self):
        analysis = parse_claude_response(VALID_CLAUDE_RESPONSE, _break(), 1.0)
        d = analysis.to_dict()
        expected_keys = {"txn_id", "title", "body", "manual", "ai", "conf", "pri", "priColor", "steps"}
        assert expected_keys.issubset(d.keys())


# ===== analyze_breaks (integration) ===========================================

class TestAnalyzeBreaks:
    @pytest.mark.asyncio
    async def test_fallback_when_no_api_key(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            results = await analyze_breaks([_break()])
            assert len(results) == 1
            assert results[0].ai == "N/A"

    @pytest.mark.asyncio
    async def test_calls_claude_with_api_key(self):
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=VALID_CLAUDE_RESPONSE)]

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch("backend.services.claude_client.anthropic") as mock_anthropic:
                mock_anthropic.AsyncAnthropic.return_value = mock_client
                results = await analyze_breaks([_break()])

        assert len(results) == 1
        assert results[0].title == "FX Rate Discrepancy · TXN-4821"
        assert results[0].conf == "96%"
        mock_client.messages.create.assert_called_once()
        call_kwargs = mock_client.messages.create.call_args[1]
        assert "claude" in call_kwargs["model"]  # model is env-configurable
        assert call_kwargs["temperature"] == 0
        assert call_kwargs["max_tokens"] == 1000

    @pytest.mark.asyncio
    async def test_fallback_on_api_error(self):
        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(side_effect=Exception("API down"))

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch("backend.services.claude_client.anthropic") as mock_anthropic:
                mock_anthropic.AsyncAnthropic.return_value = mock_client
                results = await analyze_breaks([_break()])

        assert len(results) == 1
        assert results[0].ai == "N/A"  # fallback

    @pytest.mark.asyncio
    async def test_multiple_breaks(self):
        breaks = [
            _break(txn_id="TXN-001"),
            _break(txn_id="TXN-002"),
        ]
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            results = await analyze_breaks(breaks)
        assert len(results) == 2
        assert results[0].txn_id == "TXN-001"
        assert results[1].txn_id == "TXN-002"

    @pytest.mark.asyncio
    async def test_row_lookup_passed_to_prompt(self):
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=VALID_CLAUDE_RESPONSE)]

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        row_lookup = {
            "TXN-4821": {"type": "FX_TRANSFER", "counterparty": "BBVA Mexico", "amount_usd": 8500}
        }

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch("backend.services.claude_client.anthropic") as mock_anthropic:
                mock_anthropic.AsyncAnthropic.return_value = mock_client
                results = await analyze_breaks([_break()], row_lookup=row_lookup)

        call_kwargs = mock_client.messages.create.call_args[1]
        user_msg = call_kwargs["messages"][0]["content"]
        assert "BBVA Mexico" in user_msg
        assert "FX_TRANSFER" in user_msg
