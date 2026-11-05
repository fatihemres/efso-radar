"""Phase 5B.3 / Issue #61: Bounded, fail-closed one-shot Gemini editorial shadow preflight harness.

Stage A Implementation (Strictly Offline & Zero-Network):
1. Reuses existing editorial prompt (build_editorial_prompt) and strict structured
   validation (GeminiEditorialEngine._validate_and_transform_picks); builds NO second classifier.
2. Strictly restricted to the fixed historical public news sample (SHADOW_SAMPLE_ID from Issue #49);
   rejects arbitrary/custom candidate ingestion.
3. Completely network-incapable: contains NO SDK client invocation or generate_content() call.
   All mock/synthetic testing operates strictly on pre-computed structured JSON payloads.
4. Preserves require_verified_free_tier() unconditional block. Default execution produces
   a blocked offline preview with AI_RESULT_NOT_AVAILABLE and PROVIDER_CALL_NOT_AUTHORIZED.
5. All token/pricing calculations explicitly distinguish defensive theoretical estimates
   from actual provider charges ($0.00 / 0 TRY).
6. Comprehensive report sanitization against API keys, tokens, and credentials.
"""
from __future__ import annotations

import json
import logging
import re
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
    "Stage A code contains zero network-capable client execution paths. "
    "Any decision fields shown in test output were produced exclusively via offline "
    "synthetic JSON payloads through the application validation layer."
)


def sanitize_report_text(text: str) -> str:
    """Sanitize strings against accidental API key, token, or credential leakage."""
    if not text:
        return ""
    # Mask potential Google API keys: AIza...
    sanitized = re.sub(r"AIza[0-9A-Za-z-_]{35}", "[REDACTED_API_KEY]", text)
    # Mask bearer tokens
    sanitized = re.sub(r"(?i)bearer\s+[A-Za-z0-9-_.]+", "Bearer [REDACTED_TOKEN]", sanitized)
    # Mask common key/secret patterns
    sanitized = re.sub(
        r"(?i)(api[_-]?key|secret|token|password)\s*[:=]\s*(['\"][^'\"]*['\"]|[^\s'\"]+)",
        r"\1=[REDACTED]",
        sanitized,
    )
    return sanitized


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
    status: str = Field(description="Execution outcome status (e.g. PREFLIGHT_BLOCKED_OFFLINE_ONLY)")
    ai_result: str = Field(description="AI recommendation status: AI_RESULT_NOT_AVAILABLE unless synthetic test")
    provider_status: str = Field(description="Provider connection status: PROVIDER_CALL_NOT_AUTHORIZED")
    free_tier_gate_status: str = Field(description="require_verified_free_tier() enforcement status")
    model_name: str = Field(description="Fixed Gemini model name (gemini-3.5-flash-lite)")
    candidate_count: int = Field(description="Number of public candidate articles in sample")
    batch_hash: str = Field(description="Deterministic SHA256 hash of candidate IDs and model")
    estimated_input_tokens: int = Field(description="Estimated prompt input tokens")
    enforceable_max_output_tokens: int = Field(description="Enforceable batch output token ceiling")
    actual_cost_usd: float = Field(default=0.0, description="Actual provider charge incurred ($0.00 / 0 TRY)")
    theoretical_paid_equivalent_usd: float = Field(
        description="Defensive theoretical estimate based on public list pricing; NOT a verified charge"
    )
    call_count: int = Field(default=0, description="Actual provider inference calls executed (strictly 0 in Stage A)")
    synthetic_evaluation_count: int = Field(
        default=0, description="Synthetic structured payload evaluations executed (hermetic test only)"
    )
    sdk_network_executed: bool = Field(default=False, description="Whether any network SDK call was made")
    zero_retry_design_contract: str = Field(
        default="HttpRetryOptions(attempts=1) designed; NO live SDK session exercised",
        description="Designed retry contract",
    )
    grounding_forbidden: bool = Field(default=True, description="Whether search grounding and tools are disabled")
    cost_disclaimer: str = Field(
        default="Actual cost is $0.00 / 0 TRY (provider call blocked). Theoretical paid-equivalent estimate is based on standard list pricing, NOT an active billing charge or provider guarantee.",
        description="Cost calculation disclaimer",
    )
    disclaimer: str = Field(description="Explicit human-readable safety and non-production disclaimer")
    candidates_preview: list[dict[str, Any]] = Field(default_factory=list, description="Sanitized candidates")
    decisions: list[dict[str, Any]] = Field(
        default_factory=list, description="Approved decisions if synthetic response was evaluated"
    )
    error_message: Optional[str] = Field(default=None, description="Sanitized error reason if blocked or failed")

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
            f"- **Actual Cost Incurred:** `${self.actual_cost_usd:.4f}` USD ($0.00 / 0 TRY)",
            f"- **Theoretical Paid-Equivalent:** `~${self.theoretical_paid_equivalent_usd:.4f}` USD (Defensive estimate only)",
            f"- **Provider API Calls Executed:** `{self.call_count}` (Stage A contains zero network-capable calls; strictly 0)",
            f"- **Synthetic Evaluations Executed:** `{self.synthetic_evaluation_count}` (Hermetic offline test only)",
            f"- **Retry Contract (Designed):** `{self.zero_retry_design_contract}`",
            f"- **Search Grounding:** `{'Disabled' if self.grounding_forbidden else 'Enabled'}`",
            f"- **Candidate Count:** `{self.candidate_count}` (Public historical sample only)",
            f"- **Batch Hash:** `{self.batch_hash}`",
            f"- **Estimated Input Tokens:** `~{self.estimated_input_tokens}`",
            f"- **Output Token Ceiling:** `{self.enforceable_max_output_tokens}`",
            f"",
            f"### Cost & Billing Disclaimer",
            f"> {self.cost_disclaimer}",
            f"",
            f"### Safety Disclaimer",
            f"> {self.disclaimer}",
            f"",
            f"### Public Historical Sample Candidates ({self.candidate_count} items)",
            f"| # | Source | Market | Title | Published (UTC) | URL |",
            f"| :--- | :--- | :---: | :--- | :--- | :--- |",
        ]
        for idx, c in enumerate(self.candidates_preview, start=1):
            sanitized_title = sanitize_report_text(str(c.get("title", "")))
            sanitized_source = sanitize_report_text(str(c.get("source", "")))
            sanitized_url = sanitize_report_text(str(c.get("url", "")))
            lines.append(
                f"| {idx} | {sanitized_source} | {c.get('market')} | {sanitized_title} | "
                f"{c.get('published_at')} | {sanitized_url} |"
            )

        if self.decisions:
            lines.extend([
                f"",
                f"### Synthetic Evaluation Decisions ({len(self.decisions)} approved)",
                f"> [!WARNING] These decisions were generated from an OFFLINE SYNTHETIC JSON PAYLOAD in hermetic test. NOT REAL GEMINI.",
                f"| Article IDs | Headline (TR) | Score | Kind | Category | Format | Verification |",
                f"| :--- | :--- | :---: | :---: | :---: | :---: | :--- |",
            ])
            for d in self.decisions:
                sanitized_hl = sanitize_report_text(str(d.get("headline_tr", "")))
                art_ids_str = ", ".join(d.get("article_ids", []))
                lines.append(
                    f"| {art_ids_str} | {sanitized_hl} | {d.get('score')} | "
                    f"{d.get('kind')} | {d.get('category')} | {d.get('format')} | {d.get('verification_status')} |"
                )
        else:
            lines.extend([
                f"",
                f"### Editorial Decision Status",
                f"**`AI_RESULT_NOT_AVAILABLE`** (No model inference has taken place; fail-closed offline preview only).",
            ])

        if self.error_message:
            sanitized_err = sanitize_report_text(self.error_message)
            lines.extend([
                f"",
                f"### Block / Gate Detail",
                f"```text",
                f"{sanitized_err}",
                f"```",
            ])

        return "\n".join(lines)


class OneShotShadowPreflightRunner:
    """Bounded, fail-closed one-shot Gemini editorial shadow preflight harness.

    Stage A Guarantees:
    - Purely offline and fail-closed: NO network-capable client execution path exists.
    - Strictly bound to authorized historical public news sample (SHADOW_SAMPLE_ID).
    - Preserves require_verified_free_tier() unconditional block.
    - Model restriction: gemini-3.5-flash-lite only.
    - Grounding forbidden: enable_search_grounding=False only.
    - Zero retries designed: HttpRetryOptions(attempts=1).
    - Synthetic testing operates strictly on pre-computed string JSON payloads.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._provider_call_count = 0
        self._synthetic_evaluation_count = 0

    @property
    def call_count(self) -> int:
        """Provider inference calls executed (strictly 0 in Stage A)."""
        return self._provider_call_count

    @property
    def provider_call_count(self) -> int:
        """Provider inference calls executed (strictly 0 in Stage A)."""
        return self._provider_call_count

    @property
    def synthetic_evaluation_count(self) -> int:
        """Synthetic structured payload evaluations executed (test-only)."""
        return self._synthetic_evaluation_count

    def _resolve_and_validate_candidates(
        self, candidates: Optional[list[Article]] = None
    ) -> list[Article]:
        """Enforce that only the authentic, unaltered historical public sample can enter Stage A.

        Stage A is strictly bound to the authorized historical public news sample (SHADOW_SAMPLE_ID).
        Any custom candidates, candidate lists with different IDs, or candidates with altered/tampered
        title, snippet, URL, source, or metadata are strictly rejected fail-closed.
        """
        authorized_sample = get_historical_public_shadow_sample()

        if candidates is None:
            return authorized_sample

        if not candidates:
            raise AIEvaluationError("Candidate list is empty: shadow preflight requires at least one article")

        auth_by_id = {a.id: a for a in authorized_sample}
        input_ids = {c.id for c in candidates}
        if input_ids != set(auth_by_id.keys()):
            raise AIEvaluationError(
                f"Arbitrary candidates rejected: Stage A is strictly restricted to the authorized "
                f"historical public metadata sample ({SHADOW_SAMPLE_ID}). Custom candidate ingestion is forbidden."
            )

        # Rigorous content integrity: reject candidates with identical IDs but modified content
        for c in candidates:
            auth_art = auth_by_id[c.id]
            if (
                c.title != auth_art.title
                or c.url != auth_art.url
                or c.snippet != auth_art.snippet
                or c.source != auth_art.source
                or c.market != auth_art.market
                or c.published_at != auth_art.published_at
                or c.canonical_url != auth_art.canonical_url
            ):
                raise AIEvaluationError(
                    f"Candidate content modification rejected: candidate '{c.id}' has altered title, "
                    f"snippet, URL, or metadata. Stage A strictly forbids tampered candidate content."
                )

        return authorized_sample

    def run_preflight(
        self,
        candidates: Optional[list[Article]] = None,
        as_of_time: Optional[datetime] = None,
        **kwargs: Any,
    ) -> ShadowPreflightResult:
        """Execute the fail-closed offline shadow preflight.

        Guarantees:
        - NEVER makes a network call.
        - Rejects any attempt to inject a network-capable client.
        - Strictly evaluates require_verified_free_tier() and produces an offline preview.
        """
        # P1 Security Guard: explicitly reject any client injection attempts
        if "client" in kwargs or "mock_client" in kwargs or "engine" in kwargs:
            raise AIEvaluationError(
                "Stage A preflight strictly forbids network-capable client injection. "
                "No SDK client can be executed through this API."
            )

        active_candidates = self._resolve_and_validate_candidates(candidates)
        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC

        # Enforce model restriction
        if self.settings.gemini_model != ALLOWED_SHADOW_MODEL:
            raise AIEvaluationError(
                f"Shadow preflight only permits model '{ALLOWED_SHADOW_MODEL}', got '{self.settings.gemini_model}'"
            )

        # Enforce zero grounding / tools
        if self.settings.enable_search_grounding:
            raise AIEvaluationError("Search grounding is strictly forbidden in shadow preflight")

        # Compute deterministic parameters and prompt
        prompt_text = build_editorial_prompt(active_candidates, [], ref_now)
        batch_hash = compute_candidate_batch_hash(
            [c.id for c in active_candidates], self.settings.gemini_model
        )
        est_input_tokens = estimate_tokens_from_text(prompt_text)
        batch_max_out = compute_enforceable_batch_output_tokens(
            len(active_candidates), max_tokens_per_story=180
        )
        est_paid_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=est_input_tokens,
            output_tokens=batch_max_out // 2,
            used_search_grounding=False,
        )

        candidates_preview = [
            {
                "id": c.id,
                "title": sanitize_report_text(c.title),
                "source": sanitize_report_text(c.source),
                "published_at": c.published_at.isoformat(),
                "url": sanitize_report_text(c.canonical_url or c.url),
                "market": c.market.value,
                "snippet": sanitize_report_text(c.snippet),
            }
            for c in active_candidates
        ]

        # Enforce verified free-tier guard; must fail closed before any network attempt
        try:
            require_verified_free_tier()
        except RuntimeError as exc:
            logger.info(
                "Shadow preflight: provider call safely blocked as expected by require_verified_free_tier()"
            )
            return ShadowPreflightResult(
                sample_id=SHADOW_SAMPLE_ID,
                sample_date_utc=ref_now.isoformat(),
                status="PREFLIGHT_BLOCKED_OFFLINE_ONLY",
                ai_result="AI_RESULT_NOT_AVAILABLE",
                provider_status="PROVIDER_CALL_NOT_AUTHORIZED",
                free_tier_gate_status="BLOCKED_BY_REQUIRE_VERIFIED_FREE_TIER",
                model_name=self.settings.gemini_model,
                candidate_count=len(active_candidates),
                batch_hash=batch_hash,
                estimated_input_tokens=est_input_tokens,
                enforceable_max_output_tokens=batch_max_out,
                actual_cost_usd=0.0,
                theoretical_paid_equivalent_usd=est_paid_cost,
                call_count=0,
                synthetic_evaluation_count=0,
                sdk_network_executed=False,
                zero_retry_design_contract="HttpRetryOptions(attempts=1) designed; NO live SDK session exercised",
                grounding_forbidden=True,
                disclaimer=NOT_REAL_GEMINI_DISCLAIMER,
                candidates_preview=candidates_preview,
                decisions=[],
                error_message=sanitize_report_text(str(exc)),
            )

        # Stage A safety invariant: if require_verified_free_tier() did not raise, fail closed anyway
        raise AIEvaluationError("Real API execution is strictly not authorized in Stage A")

    def evaluate_synthetic_response(
        self,
        synthetic_response_json: str,
        candidates: Optional[list[Article]] = None,
        as_of_time: Optional[datetime] = None,
        **kwargs: Any,
    ) -> ShadowPreflightResult:
        """Hermetic test-only evaluation of a pre-computed synthetic JSON structured response.

        Guarantees:
        - NEVER calls an SDK client or makes a network request (accepts raw string JSON only).
        - Enforces single-shot execution guard on synthetic evaluations (max 1 call).
        - Rejects external recent_stories; Stage A synthetic evaluation evaluates against an empty history.
        - Instantiates validator with keyless settings (gemini_api_key="") to guarantee that
          no genai.Client SDK instance is ever created, even if runner settings contain an API key.
        - Runs raw payload through Pydantic validation (EditorialDecisionBatch) and
          authoritative application business rules (GeminiEditorialEngine._validate_and_transform_picks).
        - Distinguishes synthetic evaluation count from provider call count (strictly 0).
        - Produces decisions explicitly marked SYNTHETIC_AI_RESULT_NOT_REAL_GEMINI.
        """
        if "recent_stories" in kwargs:
            raise AIEvaluationError(
                "evaluate_synthetic_response does not accept external recent_stories; "
                "Stage A synthetic evaluations operate hermetically against an empty story history."
            )

        # Enforce single-invocation guard on synthetic evaluations
        if self._synthetic_evaluation_count >= 1:
            raise AIEvaluationError(
                "Repeated execution blocked: shadow harness enforces single-shot evaluation (max 1 call)"
            )
        self._synthetic_evaluation_count += 1

        active_candidates = self._resolve_and_validate_candidates(candidates)
        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC
        active_stories: list[Story] = []

        if not synthetic_response_json or not synthetic_response_json.strip():
            raise AIEvaluationError("Synthetic response payload is empty")

        try:
            batch = EditorialDecisionBatch.model_validate_json(synthetic_response_json)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise AIEvaluationError(f"Malformed synthetic structured output: {exc}") from exc

        # Stage A Safety Invariant: validator must use keyless settings to guarantee no SDK client is created
        keyless_settings = self.settings.model_copy(update={"gemini_api_key": ""})
        engine = GeminiEditorialEngine(keyless_settings)
        if engine.client is not None:
            raise AIEvaluationError(
                "Safety invariant violated: SDK client must never be instantiated during shadow evaluation"
            )

        approved_assessments = engine._validate_and_transform_picks(
            batch.picks, active_candidates, active_stories
        )

        decisions = [
            {
                "article_ids": asm.article_ids,
                "headline_tr": sanitize_report_text(asm.headline_tr),
                "score": asm.score,
                "kind": asm.kind.value,
                "category": asm.category.value,
                "format": asm.suggested_format.value,
                "verification_status": asm.verification_status.value,
                "why_efso_tr": sanitize_report_text(asm.why_efso_tr),
                "uncertainties_tr": sanitize_report_text(asm.uncertainties_tr),
            }
            for asm in approved_assessments
        ]

        batch_hash = compute_candidate_batch_hash(
            [c.id for c in active_candidates], self.settings.gemini_model
        )
        batch_max_out = compute_enforceable_batch_output_tokens(
            len(active_candidates), max_tokens_per_story=180
        )
        prompt_text = build_editorial_prompt(active_candidates, active_stories, ref_now)
        est_input_tokens = estimate_tokens_from_text(prompt_text)
        est_paid_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=est_input_tokens,
            output_tokens=batch_max_out // 2,
            used_search_grounding=False,
        )

        candidates_preview = [
            {
                "id": c.id,
                "title": sanitize_report_text(c.title),
                "source": sanitize_report_text(c.source),
                "published_at": c.published_at.isoformat(),
                "url": sanitize_report_text(c.canonical_url or c.url),
                "market": c.market.value,
                "snippet": sanitize_report_text(c.snippet),
            }
            for c in active_candidates
        ]

        return ShadowPreflightResult(
            sample_id=SHADOW_SAMPLE_ID,
            sample_date_utc=ref_now.isoformat(),
            status="SYNTHETIC_EVALUATION_COMPLETED_TEST_ONLY",
            ai_result="SYNTHETIC_AI_RESULT_NOT_REAL_GEMINI",
            provider_status="PROVIDER_CALL_NEVER_ATTEMPTED",
            free_tier_gate_status="SYNTHETIC_OFFLINE_PAYLOAD_EVALUATED",
            model_name=self.settings.gemini_model,
            candidate_count=len(active_candidates),
            batch_hash=batch_hash,
            estimated_input_tokens=est_input_tokens,
            enforceable_max_output_tokens=batch_max_out,
            actual_cost_usd=0.0,
            theoretical_paid_equivalent_usd=est_paid_cost,
            call_count=0,
            synthetic_evaluation_count=self._synthetic_evaluation_count,
            sdk_network_executed=False,
            zero_retry_design_contract="Offline synthetic payload; NO live SDK session exercised",
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
