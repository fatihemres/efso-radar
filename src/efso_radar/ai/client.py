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
    VerificationStatus,
    normalize_text,
)

logger = logging.getLogger("efso-radar.ai.client")


class AIEvaluationError(Exception):
    """Raised when AI evaluation fails due to network outage, budget limit, or API error."""
    pass


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


TURKISH_STOPWORDS = {
    "ve", "ile", "bir", "bu", "o", "icin", "de", "da", "den", "dan", "te", "ta",
    "ne", "sonra", "once", "gibi", "kadar", "olan", "olarak", "yeni", "ilk", "kez",
    "cok", "en", "haber", "unlu", "hakkinda"
}

# Semantic topic groups that indicate different events even when the same celebrity is involved
DIVERGENT_EVENT_PAIRS = [
    # Breakup vs Relationship / Wedding / Reconciliation
    ({"ayrildi", "ayrilik", "bitti", "noktaladi", "yollarini ayirdi"}, {"baristi", "evlendi", "nikah", "ask", "iliski", "nisanlandi", "yuzuk"}),
    # Music / Concerts vs Acting / Movies / Television
    ({"album", "sarki", "single", "klip", "konser", "turne"}, {"dizi", "film", "sinema", "rol", "oyuncu", "karakter", "yonetmen"}),
    # Death / Mortality vs Birth / Children
    ({"vefat", "olum", "oldu", "hayatini kaybetti"}, {"dogum", "bebek", "anne oldu", "baba oldu"}),
    # Legal / Crime vs Romance / Projects
    ({"dava", "hapis", "gozalti", "tutuklama", "suc"}, {"dugun", "tatil", "konser", "album"}),
]


def _to_ascii_simple(text: str) -> str:
    """Strip common Turkish diacritics for robust semantic keyword comparison."""
    return (
        text.replace("ı", "i")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ş", "s")
        .replace("ö", "o")
        .replace("ç", "c")
    )


def is_headline_too_similar(headline: str, existing_headline: str, threshold: float = 0.78) -> bool:
    """Check if two headlines refer to the exact same phrase or event using token, ratio, and semantic divergence."""
    a, b = normalize_text(headline), normalize_text(existing_headline)
    if not a or not b:
        return False
    if a == b:
        return True

    words_a = set(a.split())
    words_b = set(b.split())

    ascii_a = _to_ascii_simple(a)
    ascii_b = _to_ascii_simple(b)

    # 1. Semantic divergence check: If headlines contain mutually incompatible event concepts, they are NOT the same
    for group1, group2 in DIVERGENT_EVENT_PAIRS:
        has_g1_in_a = any(_to_ascii_simple(token) in ascii_a for token in group1)
        has_g2_in_a = any(_to_ascii_simple(token) in ascii_a for token in group2)
        has_g1_in_b = any(_to_ascii_simple(token) in ascii_b for token in group1)
        has_g2_in_b = any(_to_ascii_simple(token) in ascii_b for token in group2)

        # If one headline has group1 and the other has group2, they refer to divergent events
        if (has_g1_in_a and has_g2_in_b and not has_g2_in_a) or (has_g2_in_a and has_g1_in_b and not has_g1_in_a):
            return False

    # 2. String ratio comparison
    ratio = SequenceMatcher(None, a, b).ratio()
    if ratio >= threshold:
        return True

    # 3. Content word overlap (ignoring common Turkish headline stopwords)
    content_a = words_a - TURKISH_STOPWORDS
    content_b = words_b - TURKISH_STOPWORDS
    if content_a and content_b:
        intersection = len(content_a & content_b)
        smaller = min(len(content_a), len(content_b))
        if smaller >= 3 and (intersection / smaller) >= 0.80:
            return True
    return False


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
            msg = f"Estimated AI cost (${est_cost:.4f}) exceeds configured per-run budget (${self.settings.max_estimated_run_cost_usd:.4f})."
            logger.error(msg)
            raise AIEvaluationError(msg)

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
            msg = f"Gemini API call failed: {exc}"
            logger.error(msg)
            raise AIEvaluationError(msg) from exc

        if not response or not response.text:
            logger.warning("Gemini returned empty response.")
            return []

        # Validate with Pydantic
        try:
            batch = EditorialDecisionBatch.model_validate_json(response.text)
        except (ValidationError, json.JSONDecodeError) as err:
            msg = f"Structured output parsing error: {err}. Raw response: {response.text[:200]}"
            logger.error(msg)
            raise AIEvaluationError(msg) from err

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

            # 1. Classification Gate: IGNORE and DUPLICATE are never alerted
            if pick.kind in ("IGNORE", "DUPLICATE"):
                logger.debug("Skipped non-alertable pick '%s' (kind=%s, score=%d)", pick.headline_tr, pick.kind, pick.score)
                continue

            # 2. Score threshold check
            if pick.score < self.settings.min_score:
                logger.debug("Rejected pick '%s': score %d < %d", pick.headline_tr, pick.score, self.settings.min_score)
                continue

            # 3. Candidate reference validation (Anti-Hallucination)
            valid_ids = [aid for aid in pick.article_ids if aid in article_map]
            if not valid_ids:
                logger.warning("Rejected pick '%s': No valid candidate IDs referenced (%s)", pick.headline_tr, pick.article_ids)
                continue

            # Check if any candidate article ID was already consumed in an approved pick
            if any(aid in used_article_ids for aid in valid_ids):
                logger.debug("Rejected pick '%s': Article IDs already used in another approved pick", pick.headline_tr)
                continue

            supporting_articles = [article_map[aid] for aid in valid_ids]
            source_urls = [article_map[aid].canonical_url or article_map[aid].url for aid in valid_ids]
            source_text = " ".join([f"{a.title} {a.snippet}" for a in supporting_articles]).lower()
            norm_headline = normalize_text(pick.headline_tr)

            # 4. Factual Grounding: Source vs Model Verification Cross-Check
            rumor_markers = ["iddia", "one suruldu", "öne sürüldü", "soylenti", "söylenti", "kulislere gore", "kulis", "blind item", "tabloid", "allegedly", "rumor", "claimed", "sources claim"]
            official_markers = ["resmi aciklama", "resmî açıklama", "resmen", "acikladi", "açıkladı", "duyurdu", "dogruladi", "doğruladı", "avukati", "avukatı", "menajeri", "confirmed", "announced", "official statement", "yalanladi", "yalanlandı"]

            has_source_rumor = any(m in source_text for m in rumor_markers)
            has_source_official = any(m in source_text for m in official_markers)

            grounded_verification_status = pick.verification_status
            uncertainties = pick.uncertainties_tr.strip() if pick.uncertainties_tr else ""

            # Guard against model hallucinating official status when source only states rumors
            if has_source_rumor and not has_source_official:
                if pick.verification_status in ("OFFICIAL_CONFIRMATION", "FIRST_PARTY_STATEMENT"):
                    logger.info("Downgrading ungrounded verification status '%s' to ATTRIBUTED_CLAIM for pick '%s'", pick.verification_status, pick.headline_tr)
                    grounded_verification_status = "ATTRIBUTED_CLAIM"
                    caution_msg = "Kaynak metninde resmi doğrulama bulunmuyor; haber iddia/söylenti niteliğindedir."
                    uncertainties = f"{caution_msg} {uncertainties}".strip()

            # 5. Definitive Assertion Guard: Allegations cannot become definitive factual headlines
            if grounded_verification_status in ("UNVERIFIED_RUMOR", "SPECULATION", "ATTRIBUTED_CLAIM") and not has_source_official:
                has_caution_marker = any(
                    marker in norm_headline
                    for marker in ["iddia", "one surulen", "konusulan", "soylenti", "iddiasi", "iddialari", "yalanlandi", "tartismasi"]
                )
                if not has_caution_marker:
                    logger.warning(
                        "Rejected unverified rumor pick '%s': Missing attribution/caution markers in headline",
                        pick.headline_tr,
                    )
                    continue

            # 6. Sensitive Allegations Conservative Handling (Death, crime, assault, infidelity)
            sensitive_topics = ["vefat", "olum", "ölüm", "intihar", "uyusturucu", "uyuşturucu", "taciz", "saldiri", "saldırı", "darp", "aldatma", "hirsizlik", "hırsızlık", "dolandiricilik", "dolandırıcılık", "tutuklandi", "tutuklandı", "gozalti", "gözaltı"]
            touches_sensitive_topic = any(topic in norm_headline for topic in sensitive_topics)
            if touches_sensitive_topic:
                if grounded_verification_status in ("UNVERIFIED_RUMOR", "SPECULATION"):
                    logger.warning("Rejected pick '%s': Sensitive topic on unverified rumor/speculation", pick.headline_tr)
                    continue
                if grounded_verification_status == "ATTRIBUTED_CLAIM":
                    sens_msg = "Hassas adli/yaşam iddiası: Yayınlamadan önce resmi kaynak ve adli kayıtlar teyit edilmelidir."
                    if sens_msg not in uncertainties:
                        uncertainties = f"{sens_msg} {uncertainties}".strip()

            # 7. Unsupported Quotations Guard
            import re
            for text_block in (pick.development_tr, pick.headline_tr):
                if text_block:
                    quotes = re.findall(r"['\"]([^'\"]{6,})['\"]", text_block)
                    for quote in quotes:
                        quote_words = set(normalize_text(quote).split()) - TURKISH_STOPWORDS
                        source_words = set(normalize_text(source_text).split())
                        if quote_words and not (quote_words & source_words):
                            quote_warn = f"Tırnak içi alıntı ('{quote[:30]}...') doğrudan kaynak metninde yer almıyor; teyit edilmelidir."
                            if quote_warn not in uncertainties:
                                uncertainties = f"{quote_warn} {uncertainties}".strip()

            # 8. UPDATE consistency validation
            if pick.kind == "UPDATE":
                if not pick.parent_story_id or pick.parent_story_id not in story_map:
                    logger.warning("Rejected UPDATE pick '%s': parent_story_id '%s' not found in history", pick.headline_tr, pick.parent_story_id)
                    continue
                parent_story = story_map[pick.parent_story_id]

                # Check candidate articles are not identical to parent story original articles
                if hasattr(parent_story, "article_ids") and any(aid in parent_story.article_ids for aid in valid_ids):
                    logger.warning("Rejected UPDATE pick '%s': Article was already part of parent story", pick.headline_tr)
                    continue

                if not pick.development_tr or len(pick.development_tr.strip()) < 15:
                    logger.warning("Rejected UPDATE pick '%s': Insufficient development explanation (<15 chars)", pick.headline_tr)
                    continue

                # Ensure the development is not an identical repeat of the parent headline
                if is_headline_too_similar(pick.development_tr, parent_story.headline_tr, threshold=0.75):
                    logger.warning("Rejected UPDATE pick '%s': Development repeats existing story headline", pick.headline_tr)
                    continue

            # 9. NEW consistency validation (Anti-Duplicate Gate)
            elif pick.kind == "NEW":
                is_duplicate = any(
                    is_headline_too_similar(pick.headline_tr, s.headline_tr)
                    for s in recent_stories
                )
                if is_duplicate:
                    logger.info("Rejected NEW pick '%s': Overly similar to previously alerted story", pick.headline_tr)
                    continue

            # Build validated domain model
            assessment = EditorialAssessment(
                article_ids=valid_ids,
                score=pick.score,
                kind=EditorialKind(pick.kind),
                parent_story_id=pick.parent_story_id if pick.kind == "UPDATE" else None,
                development_tr=pick.development_tr.strip() if pick.kind == "UPDATE" else None,
                headline_tr=pick.headline_tr.strip(),
                cover_title_tr=pick.cover_title_tr.strip(),
                caption_idea_tr=pick.caption_idea_tr.strip(),
                relevance_reason_tr=pick.relevance_reason_tr.strip(),
                why_efso_tr=pick.why_efso_tr.strip(),
                uncertainties_tr=uncertainties if uncertainties else None,
                suggested_format=SuggestedFormat(pick.suggested_format),
                category=StoryCategory(pick.category),
                verification_status=VerificationStatus(grounded_verification_status),
                source_urls=source_urls,
                supporting_articles=supporting_articles,
            )

            approved.append(assessment)
            used_article_ids.update(valid_ids)

        return approved
