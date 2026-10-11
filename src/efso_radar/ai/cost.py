"""Gemini API official pricing and token cost estimation.

Based on official Google AI Studio / Gemini API published pricing (2026):
- gemini-3.8-flash:
    - Input: $0.75 / 1,000,000 tokens
    - Output: $3.75 / 1,000,000 tokens (includes all output and thinking/reasoning tokens)
- gemini-3.5-flash-lite (Recommended for high-volume cost efficiency):
    - Input: $0.30 / 1,000,000 tokens
    - Output: $2.50 / 1,000,000 tokens (includes thinking/reasoning tokens)
- gemini-3.1-flash-lite:
    - Input: $0.25 / 1,000,000 tokens
    - Output: $1.50 / 1,000,000 tokens
- gemini-2.5-flash (Legacy, restricted access):
    - Input: $0.30 / 1,000,000 tokens
    - Output: $2.50 / 1,000,000 tokens
- Note: gemini-2.0-flash-lite was shut down on June 1, 2026.
- Google Search Grounding (optional):
    - $35.00 / 1,000 queries (after free tier)

Important Billing Rules:
- Output tokens encompass ALL generated tokens, including reasoning / thinking tokens.
  Thinking tokens are billed at the standard output rate.
"""
from __future__ import annotations

import logging
from typing import NamedTuple

logger = logging.getLogger("efso-radar.ai.cost")


class ModelPricing(NamedTuple):
    input_per_million: float
    output_per_million: float
    supports_search_grounding: bool = True
    is_deprecated: bool = False


OFFICIAL_PRICING: dict[str, ModelPricing] = {
    # Current generation models (2026)
    "gemini-3.8-flash": ModelPricing(
        input_per_million=0.75,
        output_per_million=3.75,
    ),
    "gemini-3.5-flash-lite": ModelPricing(
        input_per_million=0.30,
        output_per_million=2.50,
    ),
    "gemini-3.1-flash-lite": ModelPricing(
        input_per_million=0.25,
        output_per_million=1.50,
    ),
    # Legacy models
    "gemini-2.5-flash": ModelPricing(
        input_per_million=0.30,
        output_per_million=2.50,
        is_deprecated=True,
    ),
    "gemini-1.5-flash": ModelPricing(
        input_per_million=0.075,
        output_per_million=0.30,
        is_deprecated=True,
    ),
}

# gemini-2.0-flash-lite was decommissioned on June 1, 2026.
SHUTDOWN_MODELS: set[str] = {"gemini-2.0-flash-lite", "gemini-2.0-flash"}

DEFAULT_PRICING = ModelPricing(input_per_million=0.30, output_per_million=2.50)
SEARCH_GROUNDING_COST_PER_QUERY = 0.035  # $35 per 1,000 queries


def resolve_model_pricing(model_name: str) -> tuple[str, ModelPricing]:
    """
    Resolve model pricing. Rejects decommissioned or unsupported models with an actionable error.
    Does not silently switch model IDs.
    """
    clean_name = model_name.strip().lower()
    if clean_name in SHUTDOWN_MODELS:
        raise ValueError(
            f"Gemini model '{model_name}' was decommissioned on June 1, 2026 and is no longer available. "
            f"Please configure an active model such as 'gemini-3.5-flash-lite' or 'gemini-3.8-flash'."
        )

    if clean_name not in OFFICIAL_PRICING:
        raise ValueError(
            f"Gemini model '{model_name}' is unsupported or unrecognized. "
            f"Supported models: {sorted(list(OFFICIAL_PRICING.keys()))}."
        )

    pricing = OFFICIAL_PRICING[clean_name]
    if pricing.is_deprecated:
        logger.warning("Model '%s' is marked as legacy/deprecated with restricted access.", clean_name)
    return clean_name, pricing


def estimate_call_cost(
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    thinking_tokens: int = 0,
    used_search_grounding: bool = False,
    output_already_includes_thinking: bool = False,
) -> float:
    """
    Calculate the estimated USD cost of a Gemini API generation call.
    In Google GenAI SDK, `candidates_token_count` already counts all output tokens,
    including reasoning / thinking tokens. If output_already_includes_thinking is True,
    thinking_tokens is NOT added again to avoid double counting.
    """
    _, pricing = resolve_model_pricing(model_name)

    if output_already_includes_thinking:
        total_output_tokens = output_tokens
    else:
        total_output_tokens = output_tokens + thinking_tokens

    input_cost = (input_tokens / 1_000_000) * pricing.input_per_million
    output_cost = (total_output_tokens / 1_000_000) * pricing.output_per_million
    search_cost = SEARCH_GROUNDING_COST_PER_QUERY if used_search_grounding else 0.0

    return input_cost + output_cost + search_cost



def estimate_tokens_from_text(text: str) -> int:
    """Fast character-based heuristic for token count estimation (approx 4 chars/token)."""
    if not text:
        return 0
    return max(1, len(text) // 4)
