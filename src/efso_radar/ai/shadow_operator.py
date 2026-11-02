"""Phase 5B.3 / Issue #63: Stage B manual, one-shot Gemini editorial shadow evaluation operator.

Guarantees & Safety Constraints:
1. Isolated Operator: Completely separated from production AI evaluation and Stage A preflight.
   Does NOT modify, bypass, or monkeypatch require_verified_free_tier().
2. Data Boundary: Strictly bound to Stage A's authorized 7-item historical public news sample
   (EFSO-SHADOW-20261030-HISTORICAL-PUBLIC-V1). Rejects custom, altered, or empty candidate batches.
3. Raw Model Dispositions vs Post-Validation Filtered Suggestions:
   Captures and preserves all structured model decisions (NEW, UPDATE, DUPLICATE, IGNORE)
   separately from the post-validation alertable suggestions approved by business rules.
4. Execution Contract:
   - Exactly one Standard generate_content request only.
   - Zero automatic SDK retries: HttpRetryOptions(attempts=1).
   - Zero transport retries: single dispatch.
   - Fixed model: gemini-3.5-flash-lite only.
   - Search grounding / tools: strictly forbidden (tools=None, enable_search_grounding=False).
   - Output token ceiling: compute_enforceable_batch_output_tokens(7, 180).
   - Timeout bounded: 30.0s.
   - Single-shot guard: Max 1 execution attempt per runner instance.
   - No re-execution on ambiguous outcomes or failure: runner instance remains consumed.
   - Explicit confirmation required: confirm_single_shot_free_tier=True.
5. Privacy & Sanitization:
   - Reports and outputs are strictly sanitized against API keys, tokens, and credentials.
   - Never exposes private operational notes or unredacted secrets.
6. Offline Hermetic Testing:
   - All unit tests use mocked clients or pre-computed payloads; zero real network or API calls.
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
    extract_provider_token_usage,
)
from efso_radar.ai.prompts import build_editorial_prompt
from efso_radar.ai.schemas import EditorialDecisionBatch
from efso_radar.ai.shadow_preflight import (
    ALLOWED_SHADOW_MODEL,
    SHADOW_SAMPLE_AS_OF_UTC,
    SHADOW_SAMPLE_ID,
    get_historical_public_shadow_sample,
    sanitize_report_text,
)
from efso_radar.config import Settings
from efso_radar.domain import Article

logger = logging.getLogger("efso-radar.ai.shadow_operator")

STAGE_B_DISCLAIMER = (
    "STAGE B SHADOW EVALUATION REPORT. Historical news replay only "
    f"(Sample ID: {SHADOW_SAMPLE_ID}, As-Of: {SHADOW_SAMPLE_AS_OF_UTC.isoformat()}). "
    "NOT CURRENT NEWS FRESHNESS. "
    "Small sample qualitative smoke test (7 public articles); NOT a measurement of population "
    "editorial precision, recall, or end-to-end product acceptance. "
    "All decisions and token counts reflect a single-shot execution on Free Tier ($0.00 / 0 TRY)."
)


class ShadowOperatorResult(BaseModel):
    """Structured, sanitized outcome of the Stage B one-shot shadow operator."""

    sample_id: str = Field(description="Deterministic sample cohort identifier")
    sample_date_utc: str = Field(description="Reference as-of UTC timestamp for the cohort")
    status: str = Field(description="Outcome status (e.g. OPERATOR_EXECUTION_SUCCESS, DRY_RUN_COMPLETED_OFFLINE)")
    model_name: str = Field(description="Fixed Gemini model name (gemini-3.5-flash-lite)")
    is_live_call: bool = Field(default=False, description="Whether a live/mock provider SDK call was dispatched")
    provider_call_count: int = Field(default=0, description="Provider inference calls executed (0 or 1)")
    batch_hash: str = Field(description="Deterministic SHA256 hash of candidate IDs and model")
    input_tokens: int = Field(description="Input token count (from provider usage_metadata or fallback estimate)")
    output_tokens: int = Field(description="Output token count (from provider usage_metadata or fallback estimate)")
    actual_cost_usd: float = Field(default=0.0, description="Actual provider charge incurred ($0.00 / 0 TRY on Free Tier)")
    theoretical_paid_equivalent_usd: float = Field(description="Defensive theoretical estimate for accounting only")
    finish_reason: Optional[str] = Field(default=None, description="SDK candidate finish reason (e.g. STOP)")
    zero_retry_design_contract: str = Field(
        default="HttpRetryOptions(attempts=1) enforced; zero retry at SDK and transport",
        description="Enforced retry contract",
    )
    grounding_forbidden: bool = Field(default=True, description="Whether search grounding and tools are disabled")
    raw_summary_tr: Optional[str] = Field(default=None, description="Model overall editorial summary in Turkish")
    raw_picks: list[dict[str, Any]] = Field(
        default_factory=list,
        description="ALL raw structured model decisions directly from JSON (NEW, UPDATE, DUPLICATE, IGNORE)",
    )
    approved_suggestions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Suggestions approved after business rules post-validation",
    )
    filtered_out_dispositions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Raw model picks filtered out during post-validation, with specific suppression reason",
    )
    summary_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Counts breakdown: total raw picks, approved suggestions, suppressed picks",
    )
    disclaimer: str = Field(default=STAGE_B_DISCLAIMER, description="Safety and non-production disclaimer")
    candidates_preview: list[dict[str, Any]] = Field(default_factory=list, description="Sanitized candidate metadata")
    error_message: Optional[str] = Field(default=None, description="Sanitized error reason if failed")

    def to_markdown_report(self) -> str:
        """Render a clean, redacted, human-readable markdown report safe for public review."""
        lines = [
            "# Efso Radar — Stage B Gemini Editorial Shadow Evaluation Report",
            "",
            "## 1. Execution & Metadata Overview",
            f"- **Sample ID:** `{self.sample_id}`",
            f"- **Reference As-Of:** `{self.sample_date_utc}`",
            f"- **Model:** `{self.model_name}`",
            f"- **Execution Status:** `{self.status}`",
            f"- **Live Provider Call Dispatched:** `{'Yes' if self.is_live_call else 'No (Dry Run / Mock)'}`",
            f"- **Provider API Calls Executed:** `{self.provider_call_count}`",
            f"- **Actual Cost Incurred:** `${self.actual_cost_usd:.4f}` USD ($0.00 / 0 TRY)",
            f"- **Theoretical Paid-Equivalent:** `~${self.theoretical_paid_equivalent_usd:.4f}` USD (Defensive estimate only)",
            f"- **Input Tokens:** `{self.input_tokens}`",
            f"- **Output Tokens:** `{self.output_tokens}`",
            f"- **Finish Reason:** `{self.finish_reason or 'N/A'}`",
            f"- **Retry Contract:** `{self.zero_retry_design_contract}`",
            f"- **Search Grounding:** `{'Disabled' if self.grounding_forbidden else 'Enabled'}`",
            f"- **Candidate Batch Hash:** `{self.batch_hash}`",
            "",
            "### Safety & Scope Disclaimers",
            f"> [!IMPORTANT]",
            f"> **Historical Replay:** {self.disclaimer}",
            "",
            "> [!NOTE]",
            "> **Cost & Billing:** Actual provider cost is strictly $0.00 / 0 TRY on eligible Free Tier. "
            "Theoretical paid-equivalent is calculated defensively based on standard list pricing ($0.10/M input, $0.40/M output).",
            "",
        ]

        if self.raw_summary_tr:
            lines.extend([
                "### Model Run Editorial Summary",
                f"> {sanitize_report_text(self.raw_summary_tr)}",
                "",
            ])

        # Section 2: Raw Model Dispositions
        lines.extend([
            "## 2. Raw Model Dispositions (Direct from Model Structured Output)",
            "> [!NOTE] This section contains all structured decisions produced by the model prior to application post-validation.",
            "",
            f"**Total Raw Decisions:** `{len(self.raw_picks)}` (NEW: {self.summary_counts.get('raw_new', 0)}, "
            f"UPDATE: {self.summary_counts.get('raw_update', 0)}, "
            f"DUPLICATE: {self.summary_counts.get('raw_duplicate', 0)}, "
            f"IGNORE: {self.summary_counts.get('raw_ignore', 0)})",
            "",
        ])

        if self.raw_picks:
            lines.extend([
                "| # | Article IDs | Headline (TR) | Score | Kind | Format | Category | Verification |",
                "| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :--- |",
            ])
            for idx, p in enumerate(self.raw_picks, start=1):
                hl = sanitize_report_text(str(p.get("headline_tr", "")))
                art_ids = ", ".join(p.get("article_ids", []))
                lines.append(
                    f"| {idx} | `{art_ids}` | {hl} | {p.get('score')} | "
                    f"`{p.get('kind')}` | `{p.get('suggested_format')}` | "
                    f"`{p.get('category')}` | `{p.get('verification_status')}` |"
                )
        else:
            lines.append("_No raw model picks recorded (dry-run or empty output)._")

        # Section 3: Approved Alertable Suggestions
        lines.extend([
            "",
            "## 3. Post-Validation Alertable Suggestions (Application Business Rules)",
            "> [!NOTE] These picks passed application validation (anti-hallucination, score threshold, "
            "classification gate, factual verification, and sensitive topic handling).",
            "",
            f"**Total Approved Suggestions:** `{len(self.approved_suggestions)}`",
            "",
        ])

        if self.approved_suggestions:
            lines.extend([
                "| # | Article IDs | Headline (TR) | Score | Kind | Format | Category | Verification | Why Efso |",
                "| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :--- | :--- |",
            ])
            for idx, a in enumerate(self.approved_suggestions, start=1):
                hl = sanitize_report_text(str(a.get("headline_tr", "")))
                why = sanitize_report_text(str(a.get("why_efso_tr", "")))
                art_ids = ", ".join(a.get("article_ids", []))
                lines.append(
                    f"| {idx} | `{art_ids}` | {hl} | {a.get('score')} | "
                    f"`{a.get('kind')}` | `{a.get('format')}` | "
                    f"`{a.get('category')}` | `{a.get('verification_status')}` | {why} |"
                )
        else:
            lines.append("_No alertable suggestions approved by business rules._")

        # Section 4: Filtered Out Dispositions Breakdown
        lines.extend([
            "",
            "## 4. Filtered Out Dispositions Breakdown (Suppressed by Business Rules)",
            "> [!NOTE] Shows raw model picks that were suppressed during post-validation, with exact reasons.",
            "",
            f"**Total Suppressed Items:** `{len(self.filtered_out_dispositions)}`",
            "",
        ])

        if self.filtered_out_dispositions:
            lines.extend([
                "| Article IDs | Headline (TR) | Kind | Score | Suppression Reason |",
                "| :--- | :--- | :---: | :---: | :--- |",
            ])
            for f in self.filtered_out_dispositions:
                hl = sanitize_report_text(str(f.get("headline_tr", "")))
                art_ids = ", ".join(f.get("article_ids", []))
                lines.append(
                    f"| `{art_ids}` | {hl} | `{f.get('kind')}` | {f.get('score')} | {f.get('filter_reason')} |"
                )
        else:
            lines.append("_No items were suppressed by business rules._")

        # Section 5: Candidates Provenance
        lines.extend([
            "",
            "## 5. Public Historical Sample Candidates (7 items)",
            "| # | Source | Market | Title | Published (UTC) | URL |",
            "| :--- | :--- | :---: | :--- | :--- | :--- |",
        ])
        for idx, c in enumerate(self.candidates_preview, start=1):
            sanitized_title = sanitize_report_text(str(c.get("title", "")))
            sanitized_source = sanitize_report_text(str(c.get("source", "")))
            sanitized_url = sanitize_report_text(str(c.get("url", "")))
            lines.append(
                f"| {idx} | {sanitized_source} | {c.get('market')} | {sanitized_title} | "
                f"{c.get('published_at')} | {sanitized_url} |"
            )

        if self.error_message:
            lines.extend([
                "",
                "## 6. Execution Error Detail",
                "```text",
                f"{sanitize_report_text(self.error_message)}",
                "```",
            ])

        return "\n".join(lines)


class OneShotShadowOperatorRunner:
    """Manual, bounded one-shot Gemini editorial shadow evaluation operator harness.

    Enforces:
    - Exactly 1 provider execution attempt per session (runner marked consumed on attempt).
    - Ambiguous outcome or failure cannot be re-executed in the same session.
    - Explicit confirmation gate: confirm_single_shot_free_tier=True.
    - Pinned model: gemini-3.5-flash-lite only.
    - Search grounding forbidden: enable_search_grounding=False only.
    - Retry options: HttpRetryOptions(attempts=1) at SDK and transport.
    - Candidate sample integrity: rejects altered or non-authentic candidate batches.
    - Keyless validator: GeminiEditorialEngine initialized with keyless settings for post-validation.
    - Separation of raw model picks from post-validation alertable suggestions.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._execution_attempted: bool = False
        self._call_count: int = 0

    @property
    def execution_attempted(self) -> bool:
        """Whether an execution attempt was dispatched on this runner instance."""
        return self._execution_attempted

    @property
    def call_count(self) -> int:
        """Actual provider inference calls executed (0 or 1)."""
        return self._call_count

    def _resolve_and_validate_candidates(
        self, candidates: Optional[list[Article]] = None
    ) -> list[Article]:
        """Enforce that only the authentic, unaltered historical public sample can enter Stage B."""
        authorized_sample = get_historical_public_shadow_sample()

        if candidates is None:
            return authorized_sample

        if not candidates:
            raise AIEvaluationError("Candidate list is empty: shadow evaluation requires at least one article")

        auth_by_id = {a.id: a for a in authorized_sample}
        input_ids = {c.id for c in candidates}
        if input_ids != set(auth_by_id.keys()):
            raise AIEvaluationError(
                f"Arbitrary candidates rejected: Stage B is strictly restricted to the authorized "
                f"historical public metadata sample ({SHADOW_SAMPLE_ID}). Custom candidate ingestion is forbidden."
            )

        # Rigorous content integrity: reject candidates with identical IDs but altered content
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
                    f"snippet, URL, or metadata. Stage B strictly forbids tampered candidate content."
                )

        return authorized_sample

    def _build_candidates_preview(self, candidates: list[Article]) -> list[dict[str, Any]]:
        return [
            {
                "id": c.id,
                "title": sanitize_report_text(c.title),
                "source": sanitize_report_text(c.source),
                "published_at": c.published_at.isoformat(),
                "url": sanitize_report_text(c.canonical_url or c.url),
                "market": c.market.value,
                "snippet": sanitize_report_text(c.snippet),
            }
            for c in candidates
        ]

    def dry_run(
        self,
        candidates: Optional[list[Article]] = None,
        as_of_time: Optional[datetime] = None,
    ) -> ShadowOperatorResult:
        """Execute a completely offline dry run validating prompt, token bounds, and sample integrity."""
        active_candidates = self._resolve_and_validate_candidates(candidates)
        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC

        if self.settings.gemini_model != ALLOWED_SHADOW_MODEL:
            raise AIEvaluationError(
                f"Shadow operator only permits model '{ALLOWED_SHADOW_MODEL}', got '{self.settings.gemini_model}'"
            )

        if self.settings.enable_search_grounding:
            raise AIEvaluationError("Search grounding is strictly forbidden in shadow operator")

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

        return ShadowOperatorResult(
            sample_id=SHADOW_SAMPLE_ID,
            sample_date_utc=ref_now.isoformat(),
            status="DRY_RUN_COMPLETED_OFFLINE",
            model_name=self.settings.gemini_model,
            is_live_call=False,
            provider_call_count=0,
            batch_hash=batch_hash,
            input_tokens=est_input_tokens,
            output_tokens=batch_max_out,
            actual_cost_usd=0.0,
            theoretical_paid_equivalent_usd=est_paid_cost,
            finish_reason=None,
            grounding_forbidden=True,
            raw_summary_tr=None,
            raw_picks=[],
            approved_suggestions=[],
            filtered_out_dispositions=[],
            summary_counts={"raw_total": 0, "approved_total": 0, "suppressed_total": 0},
            candidates_preview=self._build_candidates_preview(active_candidates),
            error_message=None,
        )

    def execute_one_shot(
        self,
        confirm_single_shot_free_tier: bool = False,
        client: Optional[Any] = None,
        candidates: Optional[list[Article]] = None,
        as_of_time: Optional[datetime] = None,
    ) -> ShadowOperatorResult:
        """Execute the bounded, single-shot Gemini editorial shadow evaluation.

        Enforces:
        - Exactly one execution attempt per runner instance (no re-execution on error or ambiguous outcome).
        - Explicit confirm_single_shot_free_tier=True flag required.
        - Pinned gemini-3.5-flash-lite, no grounding, zero SDK retries.
        - Distinguishes raw model picks from post-validation alertable suggestions.
        """
        # 1. Single-shot session guard: runner marked consumed immediately on attempt
        if self._execution_attempted:
            raise AIEvaluationError(
                "Repeated execution blocked: Stage B shadow operator enforces strict single-shot execution "
                "(max 1 call per session). Ambiguous outcome or previous attempt cannot be re-executed."
            )
        self._execution_attempted = True

        # 2. Safety confirmation gate
        if not confirm_single_shot_free_tier:
            raise AIEvaluationError(
                "Safety gate: Stage B operator execution requires explicit confirm_single_shot_free_tier=True "
                "confirming that Free Tier status, active rate limits (15 RPM / 250K TPM / 500 RPD), and "
                "explicit PO authorization have been verified."
            )

        # 3. Model enforcement
        if self.settings.gemini_model != ALLOWED_SHADOW_MODEL:
            raise AIEvaluationError(
                f"Shadow operator only permits model '{ALLOWED_SHADOW_MODEL}', got '{self.settings.gemini_model}'"
            )

        # 4. Search grounding forbidden
        if self.settings.enable_search_grounding:
            raise AIEvaluationError("Search grounding is strictly forbidden in shadow operator")

        # 5. Candidate integrity
        active_candidates = self._resolve_and_validate_candidates(candidates)
        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC
        candidates_preview = self._build_candidates_preview(active_candidates)

        # 6. Prompt, hash, and token ceiling calculations
        prompt_text = build_editorial_prompt(active_candidates, [], ref_now)
        batch_hash = compute_candidate_batch_hash(
            [c.id for c in active_candidates], self.settings.gemini_model
        )
        est_input_tokens = estimate_tokens_from_text(prompt_text)
        batch_max_out = compute_enforceable_batch_output_tokens(
            len(active_candidates), max_tokens_per_story=180
        )

        # 7. Resolve SDK Client
        active_client = client
        if active_client is None:
            if not self.settings.gemini_api_key:
                raise AIEvaluationError(
                    "GEMINI_API_KEY is not configured or empty; Stage B operator cannot execute without an API key."
                )
            from google import genai
            from google.genai import types

            http_opts = types.HttpOptions(
                timeout=30.0,
                retry_options=types.HttpRetryOptions(attempts=1),
            )
            active_client = genai.Client(
                api_key=self.settings.gemini_api_key,
                http_options=http_opts,
            )

        # 8. Build request configuration
        from google.genai import types

        thinking_cfg = None
        if hasattr(types, "ThinkingConfig"):
            try:
                thinking_cfg = types.ThinkingConfig(thinking_level="minimal")
            except Exception:
                thinking_cfg = None

        http_opts = None
        if hasattr(types, "HttpOptions") and hasattr(types, "HttpRetryOptions"):
            http_opts = types.HttpOptions(
                timeout=30.0,
                retry_options=types.HttpRetryOptions(attempts=1),
            )

        afc_cfg = None
        if hasattr(types, "AutomaticFunctionCallingConfig"):
            afc_cfg = types.AutomaticFunctionCallingConfig(disable=True)

        config = types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=batch_max_out,
            response_mime_type="application/json",
            response_schema=EditorialDecisionBatch,
            tools=None,
            thinking_config=thinking_cfg,
            http_options=http_opts,
            automatic_function_calling=afc_cfg,
        )

        # 9. Single-shot dispatch (fail-closed, zero retry)
        try:
            response = active_client.models.generate_content(
                model=self.settings.gemini_model,
                contents=prompt_text,
                config=config,
            )
            self._call_count += 1
        except Exception as exc:
            err_sanitized = sanitize_report_text(str(exc))
            logger.error("Stage B shadow operator call failed: %s", err_sanitized)
            raise AIEvaluationError(f"Gemini operator call failed: {err_sanitized}") from exc

        # 10. Truncation and structure validation
        finish_reason = None
        if hasattr(response, "candidates") and response.candidates:
            cand = response.candidates[0]
            finish_reason = str(getattr(cand, "finish_reason", "UNKNOWN"))
            if "MAX_TOKENS" in finish_reason.upper():
                raise AIEvaluationError(
                    f"Gemini response truncated at max token limit (finish_reason=MAX_TOKENS). Ceiling={batch_max_out}."
                )

        if not response or not getattr(response, "text", None):
            raise AIEvaluationError("Gemini returned empty or null response.")

        raw_json = response.text
        try:
            batch = EditorialDecisionBatch.model_validate_json(raw_json)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise AIEvaluationError(f"Malformed structured response from model: {exc}") from exc

        # 11. Re-use application-level business rules post-validation with keyless settings
        keyless_settings = self.settings.model_copy(update={"gemini_api_key": ""})
        engine = GeminiEditorialEngine(keyless_settings)
        if engine.client is not None:
            raise AIEvaluationError(
                "Safety invariant violated: SDK client must never be instantiated during post-validation"
            )

        approved_assessments = engine._validate_and_transform_picks(
            batch.picks, active_candidates, []
        )

        # 12. Structure raw picks
        raw_picks = []
        for p in batch.picks:
            raw_picks.append({
                "article_ids": p.article_ids,
                "kind": p.kind,
                "score": p.score,
                "headline_tr": sanitize_report_text(p.headline_tr),
                "cover_title_tr": sanitize_report_text(p.cover_title_tr),
                "caption_idea_tr": sanitize_report_text(p.caption_idea_tr),
                "why_efso_tr": sanitize_report_text(p.why_efso_tr),
                "uncertainties_tr": sanitize_report_text(p.uncertainties_tr) if p.uncertainties_tr else None,
                "suggested_format": p.suggested_format,
                "category": p.category,
                "verification_status": p.verification_status,
            })

        # 13. Structure approved suggestions
        approved_suggestions = []
        for asm in approved_assessments:
            approved_suggestions.append({
                "article_ids": asm.article_ids,
                "headline_tr": sanitize_report_text(asm.headline_tr),
                "score": asm.score,
                "kind": asm.kind.value,
                "category": asm.category.value,
                "format": asm.suggested_format.value,
                "verification_status": asm.verification_status.value,
                "why_efso_tr": sanitize_report_text(asm.why_efso_tr),
                "uncertainties_tr": sanitize_report_text(asm.uncertainties_tr) if asm.uncertainties_tr else None,
            })

        # 14. Compute filtered-out dispositions breakdown
        candidate_id_set = {c.id for c in active_candidates}
        filtered_out_dispositions = []
        for p in batch.picks:
            is_approved = any(set(p.article_ids) == set(asm.article_ids) for asm in approved_assessments)
            if not is_approved:
                reason = "SUPPRESSED_BY_EDITORIAL_SAFETY_RULES"
                if p.kind == "IGNORE":
                    reason = "SUPPRESSED_KIND_IGNORE: Classification gate suppresses IGNORE items from user notifications"
                elif p.kind == "DUPLICATE":
                    reason = "SUPPRESSED_KIND_DUPLICATE: Classification gate suppresses DUPLICATE items from user notifications"
                elif p.score < self.settings.min_score:
                    reason = f"BELOW_SCORE_THRESHOLD: Score {p.score} < min_score {self.settings.min_score}"
                elif not any(aid in candidate_id_set for aid in p.article_ids):
                    reason = "HALLUCINATED_ARTICLE_ID: Referenced article IDs not found in candidate batch"

                filtered_out_dispositions.append({
                    "article_ids": p.article_ids,
                    "headline_tr": sanitize_report_text(p.headline_tr),
                    "kind": p.kind,
                    "score": p.score,
                    "filter_reason": reason,
                })

        # 15. Summary statistics
        raw_kinds = [p.kind for p in batch.picks]
        summary_counts = {
            "raw_total": len(raw_picks),
            "raw_new": raw_kinds.count("NEW"),
            "raw_update": raw_kinds.count("UPDATE"),
            "raw_duplicate": raw_kinds.count("DUPLICATE"),
            "raw_ignore": raw_kinds.count("IGNORE"),
            "approved_total": len(approved_suggestions),
            "suppressed_total": len(filtered_out_dispositions),
        }

        # 16. Actual token extraction & bookkeeping cost calculation
        actual_in, actual_out, _ = extract_provider_token_usage(
            response,
            fallback_input_tokens=est_input_tokens,
            fallback_output_tokens=estimate_tokens_from_text(raw_json),
        )
        est_paid_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=actual_in,
            output_tokens=actual_out,
            used_search_grounding=False,
        )

        return ShadowOperatorResult(
            sample_id=SHADOW_SAMPLE_ID,
            sample_date_utc=ref_now.isoformat(),
            status="OPERATOR_EXECUTION_SUCCESS",
            model_name=self.settings.gemini_model,
            is_live_call=True,
            provider_call_count=self._call_count,
            batch_hash=batch_hash,
            input_tokens=actual_in,
            output_tokens=actual_out,
            actual_cost_usd=0.0,
            theoretical_paid_equivalent_usd=est_paid_cost,
            finish_reason=finish_reason,
            grounding_forbidden=True,
            raw_summary_tr=batch.run_editorial_summary_tr,
            raw_picks=raw_picks,
            approved_suggestions=approved_suggestions,
            filtered_out_dispositions=filtered_out_dispositions,
            summary_counts=summary_counts,
            candidates_preview=candidates_preview,
            error_message=None,
        )
