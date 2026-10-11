"""Gemini AI editorial client with Structured Outputs, token tracking, and business rule validation.

Treats Structured Outputs as schema enforcement, with an independent application-level
business rules validation layer to guarantee consistency and guard against hallucination.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional

from pydantic import ValidationError

from efso_radar.ai.cost import estimate_call_cost, estimate_tokens_from_text
from efso_radar.ai.prompts import build_editorial_prompt
from efso_radar.ai.schemas import EditorialDecisionBatch, EditorialPickSchema
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    EditorialAssessment,
    EditorialKind,
    Story,
    StoryCategory,
    SuggestedFormat,
    normalize_text,
)

logger = logging.getLogger("efso-radar.ai.client")


class AIEditorialEngine(ABC):
    """Abstract interface for AI editorial evaluation."""

    @abstractmethod
    def evaluate(
        self,
        candidates: list[Article],
        recent_stories: list[Story],
        now: datetime,
    ) -> list[EditorialAssessment]:
        pass


def is_headline_too_similar(headline: str, existing_headline: str, threshold: float = 0.80) -> bool:
    """Check if two headlines refer to the exact same phrase/event."""
    a, b = normalize_text(headline), normalize_text(existing_headline)
    if not a or not b:
        return False
    if a == b:
        return True
    return SequenceMatcher(None, a, b).ratio() >= threshold


class GeminiEditorialEngine(AIEditorialEngine):
    """Google Gemini AI client with Structured Outputs and application-level verification."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        if self.settings.gemini_api_key:
            from google import genai
            self.client = genai.Client(api_key=self.settings.gemini_api_key)

    def evaluate(
        self,
        candidates: list[Article],
        recent_stories: list[Story],
        now: datetime,
    ) -> list[EditorialAssessment]:
        if not candidates:
            return []

        prompt_text = build_editorial_prompt(candidates, recent_stories, now)

        # Budget & Token Pre-check
        est_input_tokens = estimate_tokens_from_text(prompt_text)
        est_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=est_input_tokens,
            output_tokens=self.settings.max_output_tokens // 2,
            used_search_grounding=self.settings.enable_search_grounding,
        )
        logger.info(
            "Estimated AI call: ~%d input tokens, est cost: $%.4f USD",
            est_input_tokens,
            est_cost,
        )

        if est_cost > self.settings.max_estimated_run_cost_usd:
            logger.error(
                "Estimated cost ($%.4f) exceeds configured per-run budget limit ($%.4f). Aborting AI triage.",
                est_cost,
                self.settings.max_estimated_run_cost_usd,
            )
            return []

        if not self.client:
            logger.warning("No GEMINI_API_KEY configured. Returning empty editorial picks.")
            return []

        # Call Gemini with Structured Outputs
        from google.genai import types

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=EditorialDecisionBatch,
            temperature=0.15,
            max_output_tokens=self.settings.max_output_tokens,
        )

        try:
            response = self.client.models.generate_content(
                model=self.settings.gemini_model,
                contents=prompt_text,
                config=config,
            )
        except Exception as exc:
            logger.error("Gemini API call failed: %s", exc)
            return []

        if not response or not response.text:
            logger.warning("Gemini returned empty response.")
            return []

        # Validate with Pydantic
        try:
            batch = EditorialDecisionBatch.model_validate_json(response.text)
        except (ValidationError, json.JSONDecodeError) as err:
            logger.error("Structured output parsing error: %s. Raw response: %s", err, response.text[:200])
            return []

        # Validate with Application-Level Business Rules
        return self._validate_and_transform_picks(batch.picks, candidates, recent_stories)

    def _validate_and_transform_picks(
        self,
        raw_picks: list[EditorialPickSchema],
        candidates: list[Article],
        recent_stories: list[Story],
    ) -> list[EditorialAssessment]:
        article_map = {a.id: a for a in candidates}
        story_map = {s.story_id: s for s in recent_stories}

        approved: list[EditorialAssessment] = []
        used_article_ids: set[str] = set()

        for pick in raw_picks:
            if len(approved) >= self.settings.max_per_run:
                break

            # 1. Score threshold check
            if pick.score < self.settings.min_score:
                logger.debug("Rejected pick '%s': score %d < %d", pick.headline_tr, pick.score, self.settings.min_score)
                continue

            # 2. Candidate reference validation (anti-hallucination)
            valid_ids = [aid for aid in pick.article_ids if aid in article_map]
            if not valid_ids:
                logger.warning("Rejected pick '%s': No valid candidate IDs referenced (%s)", pick.headline_tr, pick.article_ids)
                continue

            # Check if any ID was already consumed in this run
            if any(aid in used_article_ids for aid in valid_ids):
                logger.debug("Rejected pick '%s': Article IDs already used in another approved pick", pick.headline_tr)
                continue

            # 3. UPDATE consistency validation
            if pick.kind == "UPDATE":
                if not pick.parent_story_id or pick.parent_story_id not in story_map:
                    logger.warning("Rejected UPDATE pick '%s': parent_story_id '%s' not in history", pick.headline_tr, pick.parent_story_id)
                    continue
                if not pick.development_tr or len(pick.development_tr.strip()) < 10:
                    logger.warning("Rejected UPDATE pick '%s': Insufficient development explanation", pick.headline_tr)
                    continue

            # 4. NEW consistency validation (must not duplicate existing story)
            elif pick.kind == "NEW":
                is_duplicate = any(
                    is_headline_too_similar(pick.headline_tr, s.headline_tr)
                    for s in recent_stories
                )
                if is_duplicate:
                    logger.info("Rejected NEW pick '%s': Overly similar to previously alerted story", pick.headline_tr)
                    continue

            # Build validated domain model
            supporting_articles = [article_map[aid] for aid in valid_ids]
            assessment = EditorialAssessment(
                article_ids=valid_ids,
                score=pick.score,
                kind=EditorialKind(pick.kind),
                parent_story_id=pick.parent_story_id if pick.kind == "UPDATE" else None,
                development_tr=pick.development_tr.strip() if pick.kind == "UPDATE" else None,
                headline_tr=pick.headline_tr.strip(),
                cover_title_tr=pick.cover_title_tr.strip(),
                caption_idea_tr=pick.caption_idea_tr.strip(),
                why_efso_tr=pick.why_efso_tr.strip(),
                suggested_format=SuggestedFormat(pick.suggested_format),
                category=StoryCategory(pick.category),
                supporting_articles=supporting_articles,
            )

            approved.append(assessment)
            used_article_ids.update(valid_ids)

        return approved
