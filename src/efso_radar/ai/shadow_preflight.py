"""Phase 5B.3 / Issue #61: Bounded, fail-closed one-shot Gemini editorial shadow preflight harness.

Strictly offline-by-default and fail-closed:
1. Reuses existing editorial prompt (build_editorial_prompt) and strict structured
   validation (GeminiEditorialEngine._validate_and_transform_picks); builds NO second classifier.
2. Uses a fixed, immutable, public-only historical news sample (from Issue #49) stripped of
   all private editor labels, owner notes, and credentials.
3. Enforces fixed model `gemini-3.5-flash-lite`, zero search grounding, zero tools,
   and single-shot execution with zero SDK retries.
4. Preserves `require_verified_free_tier()` unconditional block; produces structured
   preview with `AI_RESULT_NOT_AVAILABLE` and `PROVIDER_CALL_NOT_AUTHORIZED` until real
   model inference has been separately authorized and genuinely occurred.
5. Injected mock client support for hermetic tests with fail-closed behavior on timeout/429/5xx,
   malformed JSON, truncation, and hallucinated candidates.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError

from efso_radar.ai.client import AIEvaluationError, GeminiEditorialEngine
from efso_radar.ai.cost import (
    compute_candidate_batch_hash,
    compute_enforceable_batch_output_tokens,
    estimate_call_cost,
    estimate_tokens_from_text,
    require_verified_free_tier,
)
from efso_radar.ai.prompts import build_editorial_prompt
from efso_radar.ai.schemas import EditorialDecisionBatch
from efso_radar.config import Settings
from efso_radar.domain import Article, Market, Story

logger = logging.getLogger("efso-radar.ai.shadow_preflight")

# Canonical historical sample metadata (derived from Issue #49 public news records)
SHADOW_SAMPLE_ID = "EFSO-SHADOW-20261030-HISTORICAL-PUBLIC-V1"
SHADOW_SAMPLE_AS_OF_UTC = datetime(2026, 10, 30, 7, 0, tzinfo=timezone.utc)
ALLOWED_SHADOW_MODEL = "gemini-3.5-flash-lite"
NOT_REAL_GEMINI_DISCLAIMER = (
    "NOT REAL GEMINI. Real Gemini editorial evaluation has NOT occurred. "
    "Any decision fields shown in test output were produced exclusively via an offline "
    "injected mock client for hermetic validation, not real AI inference."
)


def get_historical_public_shadow_sample() -> list[Article]:
    """Return the fixed, dated, public-only historical news metadata sample.

    Derived from Issue #49 public news records (Oct 26–29, 2026).
    Contains genuine source URLs, headlines, and published timestamps.
    Explicitly stripped of all owner ratings, calibration labels, and private notes.
    Includes:
    - Same-event cross-publisher pair (Hande Erçel & Devin Booker: Habertürk vs Reflex Haber).
    - Contrasting sensitive legal/health pair (Sansar Salvo: Habertürk vs Sözcü).
    - Entertainment statement (Jülide Kural / Kadir İnanır: Habertürk).
    - Cross-market international entertainment reporting (Chrishell Stause / G Flip: People).
    """
    return [
        Article.create(
            title="Kendall Jenner'ın eski sevgilisi, Hande Erçel'i beğendi",
            url="https://www.haberturk.com/magazin/foto/kendall-jenner-in-eski-sevgilisi-hande-ercel-i-begendi-3923531",
            source="Habertürk",
            published_at=datetime(2026, 10, 29, 14, 7, tzinfo=timezone.utc),
            snippet="Devin Booker, Hande Erçel'in son Instagram paylaşımını beğendi.",
            market=Market.TR,
            source_category="MAGAZIN",
        ),
        Article.create(
            title="Devin Booker’dan Hande Erçel’in Paylaşımına Dikkat Çeken Beğeni",
            url="https://reflexhaber.com/devin-bookerdan-hande-ercelin-paylasimina-dikkat-ceken-begeni",
            source="Reflex Haber",
            published_at=datetime(2026, 10, 29, 14, 10, tzinfo=timezone.utc),
            snippet="NBA yıldızı Devin Booker, oyuncu Hande Erçel'in fotoğrafını beğenerek sosyal medyada gündem oldu.",
            market=Market.TR,
            source_category="MAGAZIN",
        ),
        Article.create(
            title="Rapçi Sansar Salvo için zorunlu yatış kararı",
            url="https://www.haberturk.com/magazin/rapci-sansar-salvo-icin-zorunlu-yatis-karari-3923376",
            source="Habertürk",
            published_at=datetime(2026, 10, 29, 7, 26, tzinfo=timezone.utc),
            snippet="Rap müziğin tanınan isimlerinden Sansar Salvo hakkında hastaneye zorunlu yatış kararı verildiği açıklandı.",
            market=Market.TR,
            source_category="MAGAZIN",
        ),
        Article.create(
            title="Milyonlarca dinlenen rapçi için hastaneye zorunlu yatış kararı",
            url="https://www.sozcu.com.tr/milyonlarca-dinlenen-rapci-icin-hastaneye-zorunlu-yatis-karari-p373850",
            source="Sözcü",
            published_at=datetime(2026, 10, 29, 6, 33, tzinfo=timezone.utc),
            snippet="Müzik dünyasında Sansar Salvo olarak bilinen rapçi hakkında sağlık tedbiri kararı alındı.",
            market=Market.TR,
            source_category="MAGAZIN",
        ),
        Article.create(
            title="Jülide Kural'dan iddialara yanıt",
            url="https://www.haberturk.com/magazin/foto/kadir-inanirin-hayat-arkadasi-julide-kural-dan-iddialara-yanit-3923246",
            source="Habertürk",
            published_at=datetime(2026, 10, 28, 19, 16, tzinfo=timezone.utc),
            snippet="Oyuncu Jülide Kural, Kadir İnanır'ın sağlık durumu ve mal varlığı iddialarına ilişkin açıklamalarda bulundu.",
            market=Market.TR,
            source_category="MAGAZIN",
        ),
        Article.create(
            title="Chrishell Stause Reveals She and G Flip Have Split After 4 Years Together",
            url="https://people.com/chrishell-stause-g-flip-split-after-four-years-12190605",
            source="People",
            published_at=datetime(2026, 10, 26, 19, 10, tzinfo=timezone.utc),
            snippet="Selling Sunset star Chrishell Stause announced her separation from partner G Flip after four years together.",
            market=Market.EN,
            source_category="ENTERTAINMENT",
        ),
        Article.create(
            title="G Flip 'Feeling Very Heavy' After Chrishell Stause Split",
            url="https://people.com/g-flip-feeling-very-heavy-after-chrishell-stause-split-12195426",
            source="People",
            published_at=datetime(2026, 10, 26, 20, 12, tzinfo=timezone.utc),
            snippet="Australian musician G Flip shared emotional reflections following the public split announcement.",
            market=Market.EN,
            source_category="ENTERTAINMENT",
        ),
    ]


class ShadowPreflightResult(BaseModel):
    """Structured, sanitized preflight result and readiness assessment."""

    sample_id: str = Field(description="Deterministic sample cohort identifier")
    sample_date_utc: str = Field(description="Reference as-of UTC timestamp for the cohort")
    status: str = Field(description="Execution outcome status (e.g. PREFLIGHT_READY_OFFLINE_ONLY)")
    ai_result: str = Field(description="AI recommendation status: AI_RESULT_NOT_AVAILABLE unless mock tested")
    provider_status: str = Field(description="Provider connection status: PROVIDER_CALL_NOT_AUTHORIZED")
    free_tier_gate_status: str = Field(description="require_verified_free_tier() enforcement status")
    model_name: str = Field(description="Fixed Gemini model name (gemini-3.5-flash-lite)")
    candidate_count: int = Field(description="Number of public candidate articles in sample")
    batch_hash: str = Field(description="Deterministic SHA256 hash of candidate IDs and model")
    estimated_input_tokens: int = Field(description="Estimated prompt input tokens")
    enforceable_max_output_tokens: int = Field(description="Enforceable batch output token ceiling")
    estimated_cost_usd: float = Field(description="Estimated USD cost (0.00 for Free Tier target)")
    call_count: int = Field(description="Actual inference call count executed in this runner (max 1)")
    max_allowed_calls: int = Field(default=1, description="Strict maximum calls allowed per session")
    zero_retry_enforced: bool = Field(default=True, description="Whether HttpRetryOptions(attempts=1) was set")
    grounding_forbidden: bool = Field(default=True, description="Whether search grounding and tools are disabled")
    disclaimer: str = Field(description="Explicit human-readable safety and non-production disclaimer")
    candidates_preview: list[dict[str, Any]] = Field(default_factory=list, description="Sanitized candidates")
    decisions: list[dict[str, Any]] = Field(default_factory=list, description="Approved decisions if mock tested")
    error_message: Optional[str] = Field(default=None, description="Error reason if blocked or failed")

    def to_markdown_report(self) -> str:
        """Render a clean, redacted, human-readable markdown report safe for public PR and CI."""
        lines = [
            f"# Efso Radar — Gemini Editorial Shadow Preflight Report",
            f"",
            f"- **Sample ID:** `{self.sample_id}`",
            f"- **Reference As-Of:** `{self.sample_date_utc}`",
            f"- **Model:** `{self.model_name}`",
            f"- **Preflight Status:** `{self.status}`",
            f"- **AI Result Status:** `{self.ai_result}`",
            f"- **Provider Call Status:** `{self.provider_status}`",
            f"- **Free-Tier Gate:** `{self.free_tier_gate_status}`",
            f"- **Calls Executed:** `{self.call_count} / {self.max_allowed_calls}` (Zero retries enforced: `{self.zero_retry_enforced}`)",
            f"- **Search Grounding:** `{'Disabled' if self.grounding_forbidden else 'Enabled'}`",
            f"- **Candidate Count:** `{self.candidate_count}`",
            f"- **Batch Hash:** `{self.batch_hash}`",
            f"- **Estimated Input Tokens:** `~{self.estimated_input_tokens}`",
            f"- **Output Token Ceiling:** `{self.enforceable_max_output_tokens}`",
            f"- **Estimated Cost:** `${self.estimated_cost_usd:.4f}` USD",
            f"",
            f"### Safety Disclaimer",
            f"> {self.disclaimer}",
            f"",
            f"### Public Historical Sample Candidates ({self.candidate_count} items)",
            f"| # | Source | Market | Title | Published (UTC) | URL |",
            f"| :--- | :--- | :---: | :--- | :--- | :--- |",
        ]
        for idx, c in enumerate(self.candidates_preview, start=1):
            lines.append(
                f"| {idx} | {c.get('source')} | {c.get('market')} | {c.get('title')} | "
                f"{c.get('published_at')} | {c.get('url')} |"
            )

        if self.decisions:
            lines.extend([
                f"",
                f"### Mock Evaluation Decisions ({len(self.decisions)} approved)",
                f"> [!WARNING] These decisions were generated by an INJECTED MOCK CLIENT in hermetic test. NOT REAL GEMINI.",
                f"| Article IDs | Headline (TR) | Score | Kind | Category | Format | Verification |",
                f"| :--- | :--- | :---: | :---: | :---: | :---: | :--- |",
            ])
            for d in self.decisions:
                lines.append(
                    f"| {', '.join(d.get('article_ids', []))} | {d.get('headline_tr')} | {d.get('score')} | "
                    f"{d.get('kind')} | {d.get('category')} | {d.get('format')} | {d.get('verification_status')} |"
                )
        else:
            lines.extend([
                f"",
                f"### Editorial Decision Status",
                f"**`AI_RESULT_NOT_AVAILABLE`** (No model inference has taken place; fail-closed offline preview only).",
            ])

        if self.error_message:
            lines.extend([
                f"",
                f"### Block / Error Detail",
                f"```text",
                f"{self.error_message}",
                f"```",
            ])

        return "\n".join(lines)


class OneShotShadowPreflightRunner:
    """Bounded, fail-closed one-shot Gemini editorial shadow preflight harness.

    Enforces all requirements of Issue #61:
    - Fixed model `gemini-3.5-flash-lite`.
    - Zero search grounding / tools.
    - Zero automatic SDK retries (attempts=1).
    - Single-shot execution guard: max 1 call per runner session; repeated execution blocked.
    - Default offline mode respects require_verified_free_tier() and produces safe preview.
    - Injected mock client mode for hermetic unit testing.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._call_count = 0

    @property
    def call_count(self) -> int:
        return self._call_count

    def run_preflight(
        self,
        candidates: Optional[list[Article]] = None,
        recent_stories: Optional[list[Story]] = None,
        as_of_time: Optional[datetime] = None,
        mock_client: Optional[Any] = None,
    ) -> ShadowPreflightResult:
        """Execute the fail-closed shadow preflight or mock-injected evaluation.

        Args:
            candidates: Optional custom candidates list (defaults to historical public sample).
            recent_stories: Optional recent stories context for deduplication/update check.
            as_of_time: Optional reference time (defaults to 2026-10-30T07:00:00Z).
            mock_client: Optional injected mock genai client for hermetic offline testing.
        """
        # 1. Enforce single-invocation guard
        if self._call_count >= 1:
            raise AIEvaluationError(
                "Repeated execution blocked: shadow harness enforces single-shot execution (max 1 call)"
            )

        # 2. Resolve candidates and timestamps
        active_candidates = candidates if candidates is not None else get_historical_public_shadow_sample()
        if not active_candidates:
            raise AIEvaluationError("Candidate list is empty: shadow preflight requires at least one article")

        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC
        active_stories = recent_stories or []

        # 3. Enforce model restriction
        if self.settings.gemini_model != ALLOWED_SHADOW_MODEL:
            raise AIEvaluationError(
                f"Shadow preflight only permits model '{ALLOWED_SHADOW_MODEL}', got '{self.settings.gemini_model}'"
            )

        # 4. Enforce zero grounding / tools
        if self.settings.enable_search_grounding:
            raise AIEvaluationError("Search grounding is strictly forbidden in shadow preflight")

        # 5. Compute deterministic parameters and prompt
        prompt_text = build_editorial_prompt(active_candidates, active_stories, ref_now)
        batch_hash = compute_candidate_batch_hash(
            [c.id for c in active_candidates], self.settings.gemini_model
        )
        est_input_tokens = estimate_tokens_from_text(prompt_text)
        batch_max_out = compute_enforceable_batch_output_tokens(
            len(active_candidates), max_tokens_per_story=180
        )
        est_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=est_input_tokens,
            output_tokens=batch_max_out // 2,
            used_search_grounding=False,
        )

        candidates_preview = [
            {
                "id": c.id,
                "title": c.title,
                "source": c.source,
                "published_at": c.published_at.isoformat(),
                "url": c.canonical_url or c.url,
                "market": c.market.value,
                "snippet": c.snippet,
            }
            for c in active_candidates
        ]

        # 6. Default Offline Mode (No mock client injected)
        if mock_client is None:
            # Enforce verified free-tier guard; must fail closed before any network attempt
            try:
                require_verified_free_tier()
            except RuntimeError as exc:
                logger.info(
                    "Shadow preflight: real provider call blocked as expected by require_verified_free_tier()"
                )
                return ShadowPreflightResult(
                    sample_id=SHADOW_SAMPLE_ID,
                    sample_date_utc=ref_now.isoformat(),
                    status="PREFLIGHT_READY_OFFLINE_ONLY",
                    ai_result="AI_RESULT_NOT_AVAILABLE",
                    provider_status="PROVIDER_CALL_NOT_AUTHORIZED",
                    free_tier_gate_status="BLOCKED_BY_REQUIRE_VERIFIED_FREE_TIER",
                    model_name=self.settings.gemini_model,
                    candidate_count=len(active_candidates),
                    batch_hash=batch_hash,
                    estimated_input_tokens=est_input_tokens,
                    enforceable_max_output_tokens=batch_max_out,
                    estimated_cost_usd=est_cost,
                    call_count=0,
                    max_allowed_calls=1,
                    zero_retry_enforced=True,
                    grounding_forbidden=True,
                    disclaimer=NOT_REAL_GEMINI_DISCLAIMER,
                    candidates_preview=candidates_preview,
                    decisions=[],
                    error_message=str(exc),
                )
            # Stage A safety invariant: never permit real inference in this PR
            raise AIEvaluationError("Real API execution is strictly not authorized in Stage A")

        # 7. Injected Mock Client Mode (Hermetic test execution only)
        self._call_count += 1
        from google.genai import types

        thinking_cfg = None
        if hasattr(types, "ThinkingConfig"):
            try:
                thinking_cfg = types.ThinkingConfig(thinking_level="minimal")
            except Exception:
                thinking_cfg = None

        http_opts = None
        if hasattr(types, "HttpOptions") and hasattr(types, "HttpRetryOptions"):
            # Enforce attempts=1 (zero SDK retries)
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

        try:
            response = mock_client.models.generate_content(
                model=self.settings.gemini_model,
                contents=prompt_text,
                config=config,
            )
        except Exception as exc:
            msg = f"Injected mock call failed: {exc}"
            logger.error(msg)
            raise AIEvaluationError(msg) from exc

        # Check truncation fail-closed
        if getattr(response, "candidates", None) and len(response.candidates) > 0:
            cand = response.candidates[0]
            finish_reason = getattr(cand, "finish_reason", None)
            if finish_reason and "MAX_TOKENS" in str(finish_reason).upper():
                raise AIEvaluationError("Gemini response was truncated due to MAX_TOKENS ceiling")

        raw_text = getattr(response, "text", "")
        if not raw_text or not raw_text.strip():
            raise AIEvaluationError("Gemini response returned empty text payload")

        try:
            batch = EditorialDecisionBatch.model_validate_json(raw_text)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise AIEvaluationError(f"Malformed Gemini structured output: {exc}") from exc

        # Re-use authoritative application-level business rules validation
        engine = GeminiEditorialEngine(self.settings)
        approved_assessments = engine._validate_and_transform_picks(
            batch.picks, active_candidates, active_stories
        )

        decisions = [
            {
                "article_ids": asm.article_ids,
                "headline_tr": asm.headline_tr,
                "score": asm.score,
                "kind": asm.kind.value,
                "category": asm.category.value,
                "format": asm.suggested_format.value,
                "verification_status": asm.verification_status.value,
                "why_efso_tr": asm.why_efso_tr,
                "uncertainties_tr": asm.uncertainties_tr,
            }
            for asm in approved_assessments
        ]

        return ShadowPreflightResult(
            sample_id=SHADOW_SAMPLE_ID,
            sample_date_utc=ref_now.isoformat(),
            status="MOCK_EVALUATION_COMPLETED",
            ai_result="MOCK_AI_RESULT_NOT_REAL_GEMINI",
            provider_status="MOCK_TRANSPORT_TEST_ONLY",
            free_tier_gate_status="MOCK_CLIENT_INJECTED_FOR_TEST",
            model_name=self.settings.gemini_model,
            candidate_count=len(active_candidates),
            batch_hash=batch_hash,
            estimated_input_tokens=est_input_tokens,
            enforceable_max_output_tokens=batch_max_out,
            estimated_cost_usd=est_cost,
            call_count=self._call_count,
            max_allowed_calls=1,
            zero_retry_enforced=True,
            grounding_forbidden=True,
            disclaimer=NOT_REAL_GEMINI_DISCLAIMER,
            candidates_preview=candidates_preview,
            decisions=decisions,
            error_message=None,
        )


if __name__ == "__main__":
    runner = OneShotShadowPreflightRunner()
    result = runner.run_preflight()
    print(result.to_markdown_report())
