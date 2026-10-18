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
import re
from typing import Optional

from pydantic import ValidationError

from efso_radar.ai.cost import (
    check_monthly_budget_guard,
    compute_candidate_batch_hash,
    compute_enforceable_batch_output_tokens,
    estimate_call_cost,
    estimate_tokens_from_text,
    extract_provider_token_usage,
)
from efso_radar.ai.prompts import build_diagnostic_editorial_prompt, build_editorial_prompt
from efso_radar.ai.schemas import (
    DiagnosticArticleDecision,
    DiagnosticEvaluationBatch,
    EditorialDecisionBatch,
    EditorialPickSchema,
)
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    Story,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
    normalize_text,
)
from efso_radar.repositories.base import StateRepository

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

    def __init__(self, settings: Settings, repository: Optional[StateRepository] = None):
        self.settings = settings
        self.repository = repository
        self.client = None
        if self.settings.gemini_api_key:
            from google import genai
            from google.genai import types

            http_opts = types.HttpOptions(retry_options=types.HttpRetryOptions(attempts=1))
            self.client = genai.Client(api_key=self.settings.gemini_api_key, http_options=http_opts)

    def evaluate(
        self,
        candidates: list[Article],
        recent_stories: list[Story],
        now: datetime,
    ) -> list[EditorialAssessment]:
        if not candidates:
            return []

        batch_hash = compute_candidate_batch_hash([c.id for c in candidates], self.settings.gemini_model)

        # 0. Check persistent batch cache first for idempotent resume / zero-cost replay
        if self.repository is not None:
            cached_raw = self.repository.get_cached_ai_batch_response(batch_hash, self.settings.gemini_model)
            if cached_raw:
                logger.info("Reusing cached Gemini response for batch_hash=%s (0 API tokens billed)", batch_hash)
                batch = EditorialDecisionBatch.model_validate_json(cached_raw)
                return self._validate_and_transform_picks(batch.picks, candidates, recent_stories)

        prompt_text = build_editorial_prompt(candidates, recent_stories, now)

        # 1. Per-Run & Monthly Budget Circuit-Breaker Pre-check
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

        if self.repository is not None:
            monthly_usage = self.repository.get_monthly_ai_usage(now=now)
            allowed, reason = check_monthly_budget_guard(
                est_run_cost_usd=est_cost,
                current_monthly_cost_usd=float(monthly_usage["total_cost_usd"]),
                current_monthly_calls=int(monthly_usage["call_count"]),
                max_run_cost_usd=self.settings.max_estimated_run_cost_usd,
                max_monthly_cost_usd=self.settings.max_monthly_ai_cost_usd,
                max_monthly_calls=self.settings.max_monthly_ai_calls,
            )
            if not allowed:
                logger.error("AI Budget Circuit-Breaker triggered: %s", reason)
                raise AIEvaluationError(reason)
        elif est_cost > self.settings.max_estimated_run_cost_usd:
            msg = f"Estimated AI cost (${est_cost:.4f}) exceeds configured per-run budget (${self.settings.max_estimated_run_cost_usd:.4f})."
            logger.error(msg)
            raise AIEvaluationError(msg)

        if not self.client:
            logger.warning("No GEMINI_API_KEY configured. Returning empty editorial picks.")
            return []

        import uuid
        usage_id = f"AI-{now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        batch_max_out = compute_enforceable_batch_output_tokens(
            len(candidates),
            self.settings.max_output_tokens,
        )
        expected_out_tokens = max(1, batch_max_out // 2)

        # Pre-call budget reservation: persist RESERVED_IN_FLIGHT entry before network dispatch
        # so concurrent invocations and ambiguous/failed requests are accounted for in the ledger.
        if self.repository is not None:
            self.repository.record_ai_usage(
                usage_id=usage_id,
                run_mode="RESERVED_IN_FLIGHT",
                model_name=self.settings.gemini_model,
                batch_hash=batch_hash,
                candidate_count=len(candidates),
                input_tokens=est_input_tokens,
                output_tokens=batch_max_out,
                estimated_cost_usd=est_cost,
                response_json=None,
                created_at=now,
            )

        # Call Gemini with Structured Outputs and enforceable batch output token ceiling
        from google.genai import types

        thinking_cfg = None
        if hasattr(types, "ThinkingConfig"):
            try:
                thinking_cfg = types.ThinkingConfig(thinking_level="minimal")
            except Exception:
                thinking_cfg = None

        http_opts = None
        if hasattr(types, "HttpOptions") and hasattr(types, "HttpRetryOptions"):
            http_opts = types.HttpOptions(retry_options=types.HttpRetryOptions(attempts=1))

        afc_cfg = None
        if hasattr(types, "AutomaticFunctionCallingConfig"):
            afc_cfg = types.AutomaticFunctionCallingConfig(disable=True, maximum_remote_calls=0)

        config_kwargs: dict[str, Any] = {
            "response_mime_type": "application/json",
            "response_schema": EditorialDecisionBatch,
            "temperature": 0.15,
            "max_output_tokens": batch_max_out,
        }
        if thinking_cfg is not None:
            config_kwargs["thinking_config"] = thinking_cfg
        if http_opts is not None:
            config_kwargs["http_options"] = http_opts
        if afc_cfg is not None:
            config_kwargs["automatic_function_calling"] = afc_cfg

        config = types.GenerateContentConfig(**config_kwargs)

        response = None
        try:
            response = self.client.models.generate_content(
                model=self.settings.gemini_model,
                contents=prompt_text,
                config=config,
            )
        except Exception as exc:
            if self.repository is not None:
                self.repository.record_ai_usage(
                    usage_id=usage_id,
                    run_mode="FAILED_AMBIGUOUS",
                    model_name=self.settings.gemini_model,
                    batch_hash=batch_hash,
                    candidate_count=len(candidates),
                    input_tokens=est_input_tokens,
                    output_tokens=expected_out_tokens,
                    estimated_cost_usd=est_cost,
                    response_json=None,
                    created_at=now,
                )
            msg = f"Gemini API call failed: {exc}"
            logger.error(msg)
            raise AIEvaluationError(msg) from exc

        if getattr(response, "candidates", None) and len(response.candidates) > 0:
            cand = response.candidates[0]
            finish_reason = getattr(cand, "finish_reason", None)
            if finish_reason and "MAX_TOKENS" in str(finish_reason).upper():
                if self.repository is not None:
                    actual_in, actual_out, _ = extract_provider_token_usage(response, est_input_tokens, batch_max_out)
                    trunc_cost = estimate_call_cost(
                        self.settings.gemini_model,
                        input_tokens=actual_in,
                        output_tokens=actual_out,
                        used_search_grounding=self.settings.enable_search_grounding,
                        output_already_includes_thinking=True,
                    )
                    self.repository.record_ai_usage(
                        usage_id=usage_id,
                        run_mode="FAILED_AMBIGUOUS",
                        model_name=self.settings.gemini_model,
                        batch_hash=batch_hash,
                        candidate_count=len(candidates),
                        input_tokens=actual_in,
                        output_tokens=actual_out,
                        estimated_cost_usd=trunc_cost,
                        response_json=None,
                        created_at=now,
                    )
                msg = f"Gemini response truncated at max token limit (finish_reason=MAX_TOKENS). Ceiling={batch_max_out}."
                logger.error(msg)
                raise AIEvaluationError(msg)

        if not response or not response.text:
            if self.repository is not None:
                actual_in, actual_out, _ = extract_provider_token_usage(response, est_input_tokens, 0)
                empty_cost = estimate_call_cost(
                    self.settings.gemini_model,
                    input_tokens=actual_in,
                    output_tokens=actual_out,
                    used_search_grounding=self.settings.enable_search_grounding,
                    output_already_includes_thinking=True,
                )
                self.repository.record_ai_usage(
                    usage_id=usage_id,
                    run_mode="FAILED_EMPTY",
                    model_name=self.settings.gemini_model,
                    batch_hash=batch_hash,
                    candidate_count=len(candidates),
                    input_tokens=actual_in,
                    output_tokens=actual_out,
                    estimated_cost_usd=empty_cost,
                    response_json=None,
                    created_at=now,
                )
            logger.warning("Gemini returned empty response.")
            return []

        actual_in, actual_out, _ = extract_provider_token_usage(
            response,
            fallback_input_tokens=est_input_tokens,
            fallback_output_tokens=estimate_tokens_from_text(response.text),
        )
        actual_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=actual_in,
            output_tokens=actual_out,
            used_search_grounding=self.settings.enable_search_grounding,
            output_already_includes_thinking=True,
        )

        # Validate with Pydantic
        try:
            batch = EditorialDecisionBatch.model_validate_json(response.text)
        except (ValidationError, json.JSONDecodeError) as err:
            if self.repository is not None:
                self.repository.record_ai_usage(
                    usage_id=usage_id,
                    run_mode="FAILED_VALIDATION",
                    model_name=self.settings.gemini_model,
                    batch_hash=batch_hash,
                    candidate_count=len(candidates),
                    input_tokens=actual_in,
                    output_tokens=actual_out,
                    estimated_cost_usd=actual_cost,
                    response_json=None,
                    created_at=now,
                )
            msg = f"Structured output parsing error: {err}. Raw response: {response.text[:200]}"
            logger.error(msg)
            raise AIEvaluationError(msg) from err

        # Reconcile actual/estimated token usage and cached JSON in repository ledger
        if self.repository is not None:
            self.repository.record_ai_usage(
                usage_id=usage_id,
                run_mode="PRODUCTION",
                model_name=self.settings.gemini_model,
                batch_hash=batch_hash,
                candidate_count=len(candidates),
                input_tokens=actual_in,
                output_tokens=actual_out,
                estimated_cost_usd=actual_cost,
                response_json=response.text,
                created_at=now,
            )

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


def sanitize_sensitive_headline(headline: str) -> str:
    """Suppress intrusive, exploitative, or sensationalized clickbait phrases from sensitive headlines."""
    cleaned = headline.strip()
    patterns_to_strip = [
        r"(?i)^\s*(bomba\s*iddia|şok\s*iddia|kahreden\s*detay|şok\s*gelişme|flaş\s*gelişme|şok\s*vefat|şok\s*ölüm|şok\s*haber)[!:\s-]*",
        r"(?i)\s*bomba\s*iddia[!:]*",
        r"(?i)\s*şok\s*(gelişme|iddia|ölüm|detay|vefat|haber)[!:]*",
        r"(?i)\s*kahreden\s*(detay|iddia|vefat|ölüm|haber)[!:]*",
        r"(?i)\s*görenler\s*tanıyamadı[!:]*",
        r"(?i)\s*feci\s*(kaza|ölüm|şekilde)[!:]*",
    ]
    changed = True
    while changed:
        prev = cleaned
        for pat in patterns_to_strip:
            cleaned = re.sub(pat, "", cleaned).strip()
        changed = (cleaned != prev)
    return cleaned or headline.strip()


TR_SENSITIVE_PATTERNS = [
    r"\bvefat[a-zıiüöçşğ]*\b",
    r"\bölüm[üi]?(?:ne|nün|nü|nde)?\b",
    r"\böl(?:dü|düğü|müş|en|dürüldü)[a-zıiüöçşğ]*\b",
    r"\b(?:hayatını|yaşamını)\s+(?:kayb[eü][td]|yitir)[a-zıiüöçşğ]*\b",
    r"\bcan\s+verdi\b",
    r"\bson\s+yolculuğuna\b",
    r"\bcenaze[a-zıiüöçşğ]*\b",
    r"\btoprağa\s+verild[a-zıiüöçşğ]*\b",
    r"\bdefnedil[a-zıiüöçşğ]*\b",
    r"\bnaaşı\b",
    r"\bhelallik\b",
    r"\btaziye[a-zıiüöçşğ]*\b",
    r"\bbaşsağlığı\b",
    r"\bacılı\s+(?:anne|baba|aile|eş|çocuk)\b",
    r"\byasa\s+boğ[a-zıiüöçşğ]*\b",
    r"\bmatem[a-zıiüöçşğ]*\b",
    r"\bintihar[a-zıiüöçşğ]*\b",
    r"\bfeci\s+kaza[a-zıiüöçşğ]*\b",
    r"\btrajik\s+(?:vefat|ölüm|kaza|kayıp|olay)[a-zıiüöçşğ]*\b",
    r"\bcinayet[a-zıiüöçşğ]*\b",
]

EN_SENSITIVE_PATTERNS = [
    r"\bdied\b",
    r"\bpasses?\s+away\b",
    r"\bpassed\s+away\b",
    r"\bpassing\s+away\b",
    r"\b(?:fatal|fatally)\b",
    r"\b(?:killed\s+in|mauled\s+(?:to\s+death|by))\b",
    r"\b(?:funeral|memorial\s+service|obituary|suicide)\b",
    r"\b(?:cause\s+of\s+death|tragic\s+death|found\s+dead|pronounced\s+dead|mourns?\s+(?:the\s+)?death|death\s+of)\b",
    r"\b(?:grieving|mourning|bereavement|bereaved)\b",
]

COMPILED_SENSITIVE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in (TR_SENSITIVE_PATTERNS + EN_SENSITIVE_PATTERNS)
]


def detect_sensitive_tragedy_content(*texts: Optional[str]) -> bool:
    """Independent defense-in-depth safety detector identifying significant death, funeral,
    suicide, fatal accident, bereavement, or tragedy-related entertainment news."""
    combined = " ".join(t.strip() for t in texts if t and t.strip())
    if not combined:
        return False
    return any(pat.search(combined) is not None for pat in COMPILED_SENSITIVE_PATTERNS)


def project_diagnostic_decisions_to_assessments(
    decisions: list[DiagnosticArticleDecision],
    candidates: list[Article],
    recent_stories: Optional[list[Story]] = None,
    min_score: int = 75,
    include_manual_holds: bool = True,
    include_sensitive_reviews: bool = True,
) -> list[EditorialAssessment]:
    """
    Production projection converting per-article diagnostic decisions into selective
    EditorialAssessment objects suitable for Telegram delivery planning.

    Strict Precedence & Invariants:
    1. Urgency NEVER overrides verification, duplication, or editorial disposition.
    2. IGNORE: Never enters publication queue under any circumstances (even if BREAKING).
    3. DUPLICATE: Never becomes a new publishable story under any circumstances.
    4. RESERVE: Retained for backlog buffer, never projected into immediate delivery (even if BREAKING).
    5. INDEPENDENT SENSITIVITY VETO (Death, funeral, suicide, fatal accidents, bereavement):
       Independent defense-in-depth safety mechanism intercepts sensitive stories even if the AI
       model assigned DIGEST or BREAKING urgency.
       - State B (Verified sensitive story): is_publishable=False, disposition=SENSITIVE_REVIEW,
         uncertainties_tr='[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED] ...'.
       - State C (Sensitive + unverified story): is_publishable=False, disposition=SENSITIVE_REVIEW,
         uncertainties_tr='[NOT_PUBLISHABLE - DUAL LOCK: SENSITIVE EDITORIAL REVIEW + FACTUAL VERIFICATION REQUIRED] ...'.
         Factual verification alone does NOT authorize publication; both locks are required.
       Intrusive, exploitative, graphic, speculative, or sensationalized details are suppressed.
    6. UNVERIFIED NON-SENSITIVE CLAIMS (UNVERIFIED_RUMOR, SPECULATION):
       - State A (Unverified non-sensitive claim): is_publishable=False, disposition=HOLD,
         uncertainties_tr='[NOT_PUBLISHABLE - MANUAL VERIFICATION REQUIRED] ...'.
    7. HOLD: Never enters the publishable digest or breaking-notification queue.
    8. BREAKING: Applies ONLY to a verified, eligible, editorially approved (DIGEST) non-sensitive story.
    9. UPDATE: Legitimate UPDATE events with meaningful developments remain eligible when approved.
    """
    candidate_map = {a.id: a for a in candidates}
    assessments: list[EditorialAssessment] = []

    for d in decisions:
        if d.article_id not in candidate_map:
            continue
        art = candidate_map[d.article_id]

        # 1. Absolute rejection gates (Urgency can NEVER override these)
        if d.disposition == "IGNORE" or d.kind == "DUPLICATE":
            continue

        # 2. Backlog buffer gate (Urgency can NEVER override RESERVE)
        if d.disposition == "RESERVE":
            continue

        cat = StoryCategory.YERLI if art.market == Market.TR else StoryCategory.GLOBAL
        is_unverified = d.verification_status in ("UNVERIFIED_RUMOR", "SPECULATION")
        is_hold = (d.disposition == "HOLD" or is_unverified)

        # 3. Independent Defense-in-Depth Sensitivity Veto:
        # Detect death, funeral, suicide, fatal accidents, bereavement, or grief news
        # independently of model's disposition, urgency, or score.
        is_sensitive = (
            d.disposition == "SENSITIVE_REVIEW"
            or detect_sensitive_tragedy_content(
                art.title,
                art.snippet,
                d.headline_tr,
                d.rationale_tr,
            )
        )

        if is_sensitive:
            if not include_sensitive_reviews:
                continue
            raw_headline = d.headline_tr.strip() if d.headline_tr and d.headline_tr.strip() else art.title
            cleaned_headline = sanitize_sensitive_headline(raw_headline)

            if is_unverified:
                # State C: Sensitive AND unverified story (Dual Lock: Sensitive Review + Factual Verification)
                uncertainties_msg = (
                    "[NOT_PUBLISHABLE - DUAL LOCK: SENSITIVE EDITORIAL REVIEW + FACTUAL VERIFICATION REQUIRED] "
                    "Çift kilit güvenlik bekletmesinde: Hem teyitsiz spekülasyon doğrulaması hem de Product Owner "
                    "hassas içerik editoryal onayı zorunludur. Doğrulama tek başına yayına izin vermez."
                )
            else:
                # State B: Verified sensitive story (Sensitive Editorial Review)
                uncertainties_msg = (
                    "[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED] "
                    "Hassas içerik özel editoryal inceleme bekletmesinde: "
                    "Vefat/cenaze/taziye haberi Product Owner onayı olmadan yayınlanamaz."
                )

            assessment = EditorialAssessment(
                article_ids=[art.id],
                score=d.score,
                kind=EditorialKind(d.kind),
                parent_story_id=None,
                development_tr=None,
                headline_tr=cleaned_headline[:200],
                cover_title_tr=cleaned_headline[:80],
                caption_idea_tr=d.rationale_tr[:800],
                relevance_reason_tr=d.rationale_tr[:500],
                why_efso_tr=d.rationale_tr[:500],
                uncertainties_tr=uncertainties_msg,
                suggested_format=SuggestedFormat.POST,
                category=cat,
                verification_status=VerificationStatus(d.verification_status),
                source_urls=[art.url],
                supporting_articles=[art],
                is_publishable=False,
                disposition=EditorialDisposition.SENSITIVE_REVIEW,
            )
            assessments.append(assessment)
            continue

        # 4. State A: Unverified non-sensitive claim / manual verification hold
        if is_hold:
            if not include_manual_holds:
                continue
            headline = d.headline_tr.strip() if d.headline_tr and d.headline_tr.strip() else art.title
            assessment = EditorialAssessment(
                article_ids=[art.id],
                score=d.score,
                kind=EditorialKind(d.kind),
                parent_story_id=None,
                development_tr=None,
                headline_tr=headline[:200],
                cover_title_tr=headline[:80],
                caption_idea_tr=d.rationale_tr[:800],
                relevance_reason_tr=d.rationale_tr[:500],
                why_efso_tr=d.rationale_tr[:500],
                uncertainties_tr="[NOT_PUBLISHABLE - MANUAL VERIFICATION REQUIRED] Doğrulama bekletmesinde: Teyit gerektiren iddia/söylenti.",
                suggested_format=SuggestedFormat.POST,
                category=cat,
                verification_status=VerificationStatus(d.verification_status),
                source_urls=[art.url],
                supporting_articles=[art],
                is_publishable=False,
                disposition=EditorialDisposition.HOLD,
            )
            assessments.append(assessment)
            continue

        # 5. Standard publishable evaluation (Only verified non-sensitive DIGEST stories)
        if d.disposition != "DIGEST":
            continue

        # Unified product rule: DIGEST determines eligibility for the periodic editorial digest.
        # Score ranks eligible items by editorial priority; no independent numeric threshold
        # silently drops a verified DIGEST decision.

        # Validate UPDATE development content if applicable
        dev_tr = None
        if d.kind == "UPDATE":
            dev_tr = d.rationale_tr.strip() if d.rationale_tr else None

        headline = d.headline_tr.strip() if d.headline_tr and d.headline_tr.strip() else art.title
        cat = StoryCategory.YERLI if art.market == Market.TR else StoryCategory.GLOBAL

        assessment = EditorialAssessment(
            article_ids=[art.id],
            score=d.score,
            kind=EditorialKind(d.kind),
            parent_story_id=None,
            development_tr=dev_tr,
            headline_tr=headline[:200],
            cover_title_tr=headline[:80],
            caption_idea_tr=d.rationale_tr[:800],
            relevance_reason_tr=d.rationale_tr[:500],
            why_efso_tr=d.rationale_tr[:500],
            uncertainties_tr=None,
            suggested_format=SuggestedFormat.POST,
            category=cat,
            verification_status=VerificationStatus(d.verification_status),
            source_urls=[art.url],
            supporting_articles=[art],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )
        assessments.append(assessment)

    return assessments
