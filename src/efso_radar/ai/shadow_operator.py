"""Phase 5B.3 / Issue #63: Stage B manual, one-shot Gemini editorial shadow evaluation operator.

Stage B Software Preparation & Safety Rules:
1. Isolated Operator: Completely separated from production AI evaluation and Stage A preflight.
   Does NOT modify, bypass, or monkeypatch require_verified_free_tier() or AI_EVALUATION_ENABLED.
2. Network-Incapable Merged Code: Stage B merged code is strictly offline and fail-closed.
   Contains NO live SDK client invocation or generate_content() network dispatch.
   Live inference remains strictly unapproved and requires an explicit, separate PO operational gate.
3. Candidate Data Boundary: Strictly bound to Stage A's authorized 7-item historical public news sample
   (EFSO-SHADOW-20261030-HISTORICAL-PUBLIC-V1). Rejects custom, altered, or empty candidate batches.
4. Raw Model Dispositions vs Filtered Suggestions vs Unreturned Candidates:
   - Captures all structured model decisions (NEW, UPDATE, DUPLICATE, IGNORE) in raw_picks.
   - Evaluates business rules post-validation via keyless GeminiEditorialEngine (client=None).
   - Identifies suppressed raw picks with specific reasons (SUPPRESSED_KIND_IGNORE, etc.).
   - Explicitly marks unreferenced cohort candidates as NOT_RETURNED_BY_MODEL (prompt bounds choices to <= 3).
5. Cross-Process Session Lock Status:
   Documented as PENDING LIVE AUTHORIZATION; process-level single-shot is enforced, but cross-process
   persistence is not claimed without a persistent atomic lock mechanism.
6. Timeout & Options Standard:
   SDK HttpOptions timeout is strictly defined in milliseconds: 30_000 ms (30.0 seconds).
7. Honest Provider Charges:
   Actual provider charge is reported as NOT_VERIFIED (no external invoice or provider billing reconciliation).
   Theoretical paid-equivalent is explicitly marked as a bookkeeping estimate only ($0.10/M in, $0.40/M out).
8. Privacy & Sanitization:
   All report strings, titles, summaries, and errors are sanitized via sanitize_report_text().
"""
from __future__ import annotations

from enum import Enum
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError

from efso_radar.ai.client import AIEvaluationError, GeminiEditorialEngine
from efso_radar.ai.cost import (
    compute_candidate_batch_hash,
    compute_enforceable_batch_output_tokens,
    estimate_call_cost,
    estimate_tokens_from_text,
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
from efso_radar.ai.shadow_session import (
    ShadowSessionClaimRecord,
    claim_shadow_session,
)
from efso_radar.config import Settings
from efso_radar.domain import Article

logger = logging.getLogger("efso-radar.ai.shadow_operator")

# SDK HttpOptions.timeout is in MILLISECONDS: 30,000 ms = 30 seconds
OPERATOR_TIMEOUT_MS: int = 30_000

STAGE_B_OFFLINE_DISCLAIMER = (
    "STAGE B SHADOW EVALUATION REPORT (OFFLINE PREVIEW). Historical news replay only "
    f"(Sample ID: {SHADOW_SAMPLE_ID}, As-Of: {SHADOW_SAMPLE_AS_OF_UTC.isoformat()}). "
    "NOT REAL GEMINI. Real Gemini editorial evaluation has NOT occurred. "
    "Stage B merged software contains zero network-capable client execution paths. "
    "NOT CURRENT NEWS FRESHNESS. "
    "Small sample qualitative smoke test (7 public articles); NOT a measurement of population "
    "editorial precision, recall, or end-to-end product acceptance. "
    "Provider charge is NOT_VERIFIED; no live inference or invoice reconciliation has occurred."
)


class SimulatedDispatchOutcome(str, Enum):
    """Closed, inert outcome enumeration for hermetic test simulation seam.

    Callers cannot execute arbitrary callbacks or network handlers.
    """

    SUCCESS = "SUCCESS"
    SIMULATED_UNKNOWN = "SIMULATED_UNKNOWN"


class ShadowOperatorResult(BaseModel):
    """Structured, sanitized outcome of the Stage B shadow operator."""

    sample_id: str = Field(description="Deterministic sample cohort identifier")
    sample_date_utc: str = Field(description="Reference as-of UTC timestamp for the cohort")
    status: str = Field(description="Outcome status (e.g. OPERATOR_OFFLINE_EVALUATION_SUCCESS, DRY_RUN_COMPLETED_OFFLINE)")
    model_name: str = Field(description="Fixed Gemini model name (gemini-3.5-flash-lite)")
    is_live_call: bool = Field(default=False, description="Strictly False in Stage B merged code; no live calls")
    provider_call_count: int = Field(default=0, description="Provider inference calls executed (strictly 0)")
    batch_hash: str = Field(description="Deterministic SHA256 hash of candidate IDs and model")
    input_tokens: int = Field(description="Input token count (estimated)")
    output_tokens: int = Field(description="Output token ceiling or estimated count")
    actual_provider_charge: str = Field(
        default="NOT_VERIFIED",
        description="Provider-reconciled billing charge; strictly NOT_VERIFIED until live reconciliation",
    )
    theoretical_paid_equivalent_usd: float = Field(
        description="Defensive theoretical estimate for accounting only; NOT an actual charge"
    )
    finish_reason: Optional[str] = Field(default=None, description="SDK candidate finish reason")
    timeout_ms: int = Field(default=OPERATOR_TIMEOUT_MS, description="SDK timeout in milliseconds (30,000 ms)")
    zero_retry_design_contract: str = Field(
        default="HttpRetryOptions(attempts=1) designed; zero retry at SDK and transport",
        description="Enforced retry contract",
    )
    grounding_forbidden: bool = Field(default=True, description="Whether search grounding and tools are disabled")
    session_lock_status: str = Field(
        default="CROSS_PROCESS_SESSION_LOCK_PENDING_LIVE_AUTHORIZATION",
        description="Durable cross-process session lock status",
    )
    raw_summary_tr: Optional[str] = Field(default=None, description="Model overall editorial summary in Turkish")
    raw_picks: list[dict[str, Any]] = Field(
        default_factory=list,
        description="ALL raw structured model decisions from JSON (NEW, UPDATE, DUPLICATE, IGNORE)",
    )
    approved_suggestions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Suggestions approved after business rules post-validation",
    )
    filtered_out_dispositions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Raw model picks filtered out during post-validation with specific suppression reasons",
    )
    unreturned_candidates: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Candidate articles from the 7-item cohort not referenced in any model pick (NOT_RETURNED_BY_MODEL)",
    )
    summary_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Counts breakdown: total raw picks, approved suggestions, suppressed picks, unreturned candidates",
    )
    disclaimer: str = Field(default=STAGE_B_OFFLINE_DISCLAIMER, description="Safety and non-production disclaimer")
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
            f"- **Live Provider Call Dispatched:** `No (Offline Preparation)`",
            f"- **Provider API Calls Executed:** `{self.provider_call_count}` (Strictly 0; live calls not authorized)",
            f"- **Provider Invoiced Charge:** `{self.actual_provider_charge}` (No live invoice or charge reconciled)",
            f"- **Theoretical Paid-Equivalent:** `~${self.theoretical_paid_equivalent_usd:.4f}` USD (Defensive estimate only)",
            f"- **Input Tokens:** `{self.input_tokens}`",
            f"- **Output Tokens:** `{self.output_tokens}`",
            f"- **Finish Reason:** `{self.finish_reason or 'N/A'}`",
            f"- **SDK Timeout Setting:** `{self.timeout_ms} ms` (30 seconds)",
            f"- **Retry Contract:** `{self.zero_retry_design_contract}`",
            f"- **Search Grounding:** `{'Disabled' if self.grounding_forbidden else 'Enabled'}`",
            f"- **Candidate Batch Hash:** `{self.batch_hash}`",
            f"- **Cross-Process Session Lock:** `{self.session_lock_status}`",
            "",
            "### Safety & Scope Disclaimers",
            "> [!IMPORTANT]",
            f"> **Historical Replay & Non-Production:** {self.disclaimer}",
            "",
            "> [!NOTE]",
            "> **Billing Integrity:** Provider charge is NOT_VERIFIED. Eligible Free Tier allows $0.00 / 0 TRY, "
            "but no live invoice is collected. Theoretical paid-equivalent is a standard list pricing estimate "
            "($0.10/M input, $0.40/M output), NOT an active billing charge.",
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

        # Section 5: Unreturned Candidate Articles (NOT_RETURNED_BY_MODEL)
        lines.extend([
            "",
            "## 5. Unreturned Candidate Articles (NOT_RETURNED_BY_MODEL)",
            "> [!NOTE] These articles from the 7-item historical sample were NOT referenced or selected by the model. "
            "The prompt bounds model output to at most 3 choices.",
            "",
            f"**Total Unreturned Articles:** `{len(self.unreturned_candidates)}`",
            "",
        ])

        if self.unreturned_candidates:
            lines.extend([
                "| Article ID | Source | Market | Title | Status |",
                "| :--- | :--- | :---: | :--- | :--- |",
            ])
            for u in self.unreturned_candidates:
                t = sanitize_report_text(str(u.get("title", "")))
                s = sanitize_report_text(str(u.get("source", "")))
                lines.append(
                    f"| `{u.get('id')}` | {s} | {u.get('market')} | {t} | `NOT_RETURNED_BY_MODEL` |"
                )
        else:
            lines.append("_All candidates were referenced by the model._")

        # Section 6: Candidates Provenance
        lines.extend([
            "",
            "## 6. Public Historical Sample Candidates (7 items)",
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
                "## 7. Execution Error Detail",
                "```text",
                f"{sanitize_report_text(self.error_message)}",
                "```",
            ])

        return "\n".join(lines)


class OneShotShadowOperatorRunner:
    """Manual, bounded one-shot Gemini editorial shadow evaluation operator harness.

    Enforces:
    - Strictly offline execution in merged code: NO live SDK client invocation or generate_content() dispatch.
    - Single-shot execution guard within the running process instance.
    - Explicitly reports cross-process lock as pending live authorization.
    - Pinned model: gemini-3.5-flash-lite only.
    - Search grounding forbidden: enable_search_grounding=False only.
    - Timeout standard: 30_000 ms.
    - Candidate sample integrity: rejects altered or non-authentic candidate batches.
    - Keyless validator: GeminiEditorialEngine initialized with keyless settings for post-validation.
    - Full segregation of raw model picks, approved suggestions, suppressed picks, and unreturned candidates.
    """

    def __init__(self, settings: Optional[Settings] = None):
        # Never call Settings.from_env() in offline operator; instantiate clean Settings
        self.settings = settings or Settings()
        self._execution_attempted: bool = False
        self._call_count: int = 0

    @property
    def execution_attempted(self) -> bool:
        """Whether an execution attempt was dispatched on this runner instance."""
        return self._execution_attempted

    @property
    def call_count(self) -> int:
        """Provider inference calls executed (strictly 0 in Stage B)."""
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
            actual_provider_charge="NOT_VERIFIED",
            theoretical_paid_equivalent_usd=est_paid_cost,
            finish_reason=None,
            timeout_ms=OPERATOR_TIMEOUT_MS,
            grounding_forbidden=True,
            session_lock_status="CROSS_PROCESS_SESSION_LOCK_PENDING_LIVE_AUTHORIZATION",
            raw_summary_tr=None,
            raw_picks=[],
            approved_suggestions=[],
            filtered_out_dispositions=[],
            unreturned_candidates=[],
            summary_counts={"raw_total": 0, "approved_total": 0, "suppressed_total": 0, "unreturned_total": 0},
            candidates_preview=self._build_candidates_preview(active_candidates),
            error_message=None,
        )

    def evaluate_precomputed_response_json(
        self,
        response_json: str,
        candidates: Optional[list[Article]] = None,
        as_of_time: Optional[datetime] = None,
        finish_reason: Optional[str] = "STOP",
    ) -> ShadowOperatorResult:
        """Hermetic, offline evaluation of a pre-computed or synthetic structured response.

        Guarantees:
        - NEVER calls an SDK client or makes a network request.
        - Enforces single-shot session execution within the runner instance.
        - Segregates raw model picks vs approved suggestions vs filtered dispositions vs unreturned candidates.
        """
        if self._execution_attempted:
            raise AIEvaluationError(
                "Repeated execution blocked: Stage B shadow operator enforces strict single-shot execution "
                "(max 1 call per session). Ambiguous outcome or previous attempt cannot be re-executed."
            )
        self._execution_attempted = True

        if self.settings.gemini_model != ALLOWED_SHADOW_MODEL:
            raise AIEvaluationError(
                f"Shadow operator only permits model '{ALLOWED_SHADOW_MODEL}', got '{self.settings.gemini_model}'"
            )

        if self.settings.enable_search_grounding:
            raise AIEvaluationError("Search grounding is strictly forbidden in shadow operator")

        active_candidates = self._resolve_and_validate_candidates(candidates)
        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC
        candidates_preview = self._build_candidates_preview(active_candidates)

        prompt_text = build_editorial_prompt(active_candidates, [], ref_now)
        batch_hash = compute_candidate_batch_hash(
            [c.id for c in active_candidates], self.settings.gemini_model
        )
        est_input_tokens = estimate_tokens_from_text(prompt_text)
        batch_max_out = compute_enforceable_batch_output_tokens(
            len(active_candidates), max_tokens_per_story=180
        )

        if not response_json or not response_json.strip():
            raise AIEvaluationError("Structured response JSON is empty or null.")

        if finish_reason and "MAX_TOKENS" in str(finish_reason).upper():
            raise AIEvaluationError(
                f"Gemini response truncated at max token limit (finish_reason=MAX_TOKENS). Ceiling={batch_max_out}."
            )

        try:
            batch = EditorialDecisionBatch.model_validate_json(response_json)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise AIEvaluationError(
                "Malformed structured response: payload does not conform to EditorialDecisionBatch schema."
            ) from exc

        # Re-use application-level business rules post-validation with keyless settings
        keyless_settings = self.settings.model_copy(update={"gemini_api_key": ""})
        engine = GeminiEditorialEngine(keyless_settings)
        if engine.client is not None:
            raise AIEvaluationError(
                "Safety invariant violated: SDK client must never be instantiated during post-validation"
            )

        approved_assessments = engine._validate_and_transform_picks(
            batch.picks, active_candidates, []
        )

        # Structure raw picks
        raw_picks = []
        referenced_article_ids: set[str] = set()
        for p in batch.picks:
            referenced_article_ids.update(p.article_ids)
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

        # Structure approved suggestions
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
                "is_publishable": asm.is_publishable,
                "disposition": asm.disposition.value if asm.disposition else None,
                "why_efso_tr": sanitize_report_text(asm.why_efso_tr),
                "uncertainties_tr": sanitize_report_text(asm.uncertainties_tr) if asm.uncertainties_tr else None,
            })

        # Compute filtered-out dispositions breakdown
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

        # Compute unreturned cohort candidates (NOT_RETURNED_BY_MODEL)
        unreturned_candidates = []
        for c in active_candidates:
            if c.id not in referenced_article_ids:
                unreturned_candidates.append({
                    "id": c.id,
                    "title": sanitize_report_text(c.title),
                    "source": sanitize_report_text(c.source),
                    "market": c.market.value,
                    "status": "NOT_RETURNED_BY_MODEL",
                })

        # Summary statistics
        raw_kinds = [p.kind for p in batch.picks]
        summary_counts = {
            "raw_total": len(raw_picks),
            "raw_new": raw_kinds.count("NEW"),
            "raw_update": raw_kinds.count("UPDATE"),
            "raw_duplicate": raw_kinds.count("DUPLICATE"),
            "raw_ignore": raw_kinds.count("IGNORE"),
            "approved_total": len(approved_suggestions),
            "suppressed_total": len(filtered_out_dispositions),
            "unreturned_total": len(unreturned_candidates),
        }

        est_output_tokens = estimate_tokens_from_text(response_json)
        est_paid_cost = estimate_call_cost(
            self.settings.gemini_model,
            input_tokens=est_input_tokens,
            output_tokens=est_output_tokens,
            used_search_grounding=False,
        )

        return ShadowOperatorResult(
            sample_id=SHADOW_SAMPLE_ID,
            sample_date_utc=ref_now.isoformat(),
            status="OPERATOR_OFFLINE_EVALUATION_SUCCESS",
            model_name=self.settings.gemini_model,
            is_live_call=False,
            provider_call_count=0,
            batch_hash=batch_hash,
            input_tokens=est_input_tokens,
            output_tokens=est_output_tokens,
            actual_provider_charge="NOT_VERIFIED",
            theoretical_paid_equivalent_usd=est_paid_cost,
            finish_reason=finish_reason,
            timeout_ms=OPERATOR_TIMEOUT_MS,
            grounding_forbidden=True,
            session_lock_status="CROSS_PROCESS_SESSION_LOCK_PENDING_LIVE_AUTHORIZATION",
            raw_summary_tr=batch.run_editorial_summary_tr,
            raw_picks=raw_picks,
            approved_suggestions=approved_suggestions,
            filtered_out_dispositions=filtered_out_dispositions,
            unreturned_candidates=unreturned_candidates,
            summary_counts=summary_counts,
            candidates_preview=candidates_preview,
            error_message=None,
        )

    def execute_one_shot(
        self,
        confirm_single_shot_free_tier: bool = False,
        **kwargs: Any,
    ) -> ShadowOperatorResult:
        """P1 Security Gate: Live execution is strictly denied in merged Stage B code.

        Fails closed without ANY filesystem I/O, session claim mutation, or network dispatch.
        Real inference requires separate, explicit PO operational authorization.
        """
        # Mark attempt as consumed in instance
        self._execution_attempted = True
        raise AIEvaluationError(
            "Live Gemini API execution is strictly not authorized in Stage B operator preparation. "
            "Real inference requires an explicit, separate Product Owner operational authorization "
            "and a dedicated live execution gate."
        )

    def simulate_hermetic_dispatched_session(
        self,
        session_fixture_root: Path | str,
        simulated_outcome: SimulatedDispatchOutcome = SimulatedDispatchOutcome.SUCCESS,
        candidates: Optional[list[Article]] = None,
        as_of_time: Optional[datetime] = None,
        purpose_version: str = "STAGE-B-SHADOW-V1",
    ) -> ShadowOperatorResult:
        """Dedicated hermetic simulation seam for Issue #65 atomic pre-dispatch session claim.

        Rules:
        1. Validates candidates, model, and grounding BEFORE touching the filesystem.
           If validation fails, HALTS with zero claim I/O.
        2. Enforces single-shot execution on this runner instance.
        3. Performs atomic pre-dispatch session claim in the attested test fixture root.
        4. Simulates dispatch outcome using closed SimulatedDispatchOutcome enum only.
           Zero live SDK client calls, network connections, or caller-provided callbacks.
        5. If simulated_outcome is SIMULATED_UNKNOWN, raises AIEvaluationError while
           the claim record remains CONSUMED on disk to prove replay prevention.
        6. If simulated_outcome is SUCCESS, returns the resulting ShadowOperatorResult
           with session_lock_status updated to SAME_HOST_FIXTURE_CLAIM_CONSUMED:<canonical_id>.
        """
        if self._execution_attempted:
            raise AIEvaluationError(
                "Repeated execution blocked: Stage B shadow operator enforces strict single-shot execution "
                "(max 1 call per session). Ambiguous outcome or previous attempt cannot be re-executed."
            )

        if self.settings.gemini_model != ALLOWED_SHADOW_MODEL:
            raise AIEvaluationError(
                f"Shadow operator only permits model '{ALLOWED_SHADOW_MODEL}', got '{self.settings.gemini_model}'"
            )

        if self.settings.enable_search_grounding:
            raise AIEvaluationError("Search grounding is strictly forbidden in shadow operator")

        # Validate simulated outcome parameter
        if isinstance(simulated_outcome, str):
            try:
                simulated_outcome = SimulatedDispatchOutcome(simulated_outcome)
            except ValueError:
                raise ValueError(f"Invalid simulated dispatch outcome: {simulated_outcome}")
        elif not isinstance(simulated_outcome, SimulatedDispatchOutcome):
            raise ValueError(f"Invalid simulated dispatch outcome: {simulated_outcome}")

        # Validate candidate data BEFORE any filesystem I/O
        active_candidates = self._resolve_and_validate_candidates(candidates)
        ref_now = as_of_time or SHADOW_SAMPLE_AS_OF_UTC
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

        # Pre-dispatch session claim in attested test fixture root: consumed BEFORE dispatch simulation
        claim_record = claim_shadow_session(
            fixture_root=session_fixture_root,
            cohort_id=SHADOW_SAMPLE_ID,
            model_name=self.settings.gemini_model,
            purpose_version=purpose_version,
        )
        self._execution_attempted = True

        # Closed simulated dispatch outcome: no caller execution, no network
        if simulated_outcome == SimulatedDispatchOutcome.SIMULATED_UNKNOWN:
            raise AIEvaluationError(
                "Simulated hermetic dispatch interrupted with unknown outcome. "
                "Session claim retained on disk to deny replay."
            )

        return ShadowOperatorResult(
            sample_id=SHADOW_SAMPLE_ID,
            sample_date_utc=ref_now.isoformat(),
            status="HERMETIC_SIMULATED_DISPATCH_SUCCESS",
            model_name=self.settings.gemini_model,
            is_live_call=False,
            provider_call_count=0,
            batch_hash=batch_hash,
            input_tokens=est_input_tokens,
            output_tokens=batch_max_out,
            actual_provider_charge="NOT_VERIFIED",
            theoretical_paid_equivalent_usd=est_paid_cost,
            finish_reason=None,
            timeout_ms=OPERATOR_TIMEOUT_MS,
            grounding_forbidden=True,
            session_lock_status=f"SAME_HOST_FIXTURE_CLAIM_CONSUMED:{claim_record.session_identity}",
            raw_summary_tr=None,
            raw_picks=[],
            approved_suggestions=[],
            filtered_out_dispositions=[],
            unreturned_candidates=[],
            summary_counts={"raw_total": 0, "approved_total": 0, "suppressed_total": 0, "unreturned_total": 0},
            candidates_preview=self._build_candidates_preview(active_candidates),
            error_message=None,
        )
