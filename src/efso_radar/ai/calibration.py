"""Phase 3C-2A: Human Editorial Calibration Loader, Feedback Importer,
Offline Calibration Analyzer, Hybrid Notification Planner, and Shadow Evaluation Designer.

Treats `data/Efso_Radar_Kalibrasyon_Doldurulmus.csv` as the authoritative source of
the Product Owner's 50 human editorial decisions and comments, preserving all human
labels verbatim without reinterpreting or overwriting them.
"""
from __future__ import annotations

import csv
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from efso_radar.ai.client import is_headline_too_similar
from efso_radar.ai.cost import estimate_call_cost, estimate_tokens_from_text
from efso_radar.ai.prompts import build_editorial_prompt
from efso_radar.config import Settings
from efso_radar.deduplication.engine import calculate_token_overlap, extract_key_tokens
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationUrgencyTier,
    OwnerFeedback,
    OwnerFeedbackRating,
    Story,
    VerificationStatus,
    compute_article_id,
)
from efso_radar.notifications.formatters import format_telegram_alert, format_telegram_digest
from efso_radar.repositories.base import StateRepository

logger = logging.getLogger("efso-radar.ai.calibration")

DEFAULT_FILLED_CALIBRATION_CSV = Path("data/Efso_Radar_Kalibrasyon_Doldurulmus.csv")
DEFAULT_CALIBRATION_ANALYSIS_MD = Path("data/Efso_Radar_Editoryal_Kalibrasyon_Analizi.md")

REQUIRED_CSV_COLUMNS: tuple[str, ...] = (
    "review_id",
    "headline",
    "publisher",
    "source_url",
    "published_at",
    "discovered_at",
    "category",
    "factual_summary",
    "supporting_sources",
    "verification_uncertainty_notes",
    "discovery_priority",
    "enters_ai_cutoff_yn",
    "owner_decision",
    "owner_notes",
)


@dataclass(frozen=True)
class CalibrationItem:
    """Authoritative human-labeled calibration record from the 50-item real-world dataset."""
    review_id: str
    headline: str
    publisher: str
    source_url: str
    published_at: datetime
    discovered_at: datetime
    category: str
    factual_summary: str
    supporting_sources: str
    verification_uncertainty_notes: str
    discovery_priority: float
    enters_ai_cutoff_yn: str
    owner_decision: OwnerFeedbackRating
    owner_notes: str

    @property
    def article_id(self) -> str:
        """Deterministic 24-character article hash matching Efso Radar's discovery pipeline."""
        return compute_article_id(self.source_url, self.headline, self.publisher)

    @property
    def enters_ai_cutoff(self) -> bool:
        return self.enters_ai_cutoff_yn.strip().upper() == "Y"

    @property
    def is_editorially_positive(self) -> bool:
        """True if the owner marked the item APPROVE or IMPORTANT_UPDATE."""
        return self.owner_decision in (
            OwnerFeedbackRating.APPROVE,
            OwnerFeedbackRating.IMPORTANT_UPDATE,
        )

    @property
    def has_owner_note(self) -> bool:
        return bool(self.owner_notes and self.owner_notes.strip())

    def to_article(self) -> Article:
        """Convert calibration record into a domain Article for pipeline evaluation."""
        is_tr = (
            self.category.endswith("_TR")
            or any(ch in self.headline for ch in "şığüöçŞİĞÜÖÇ")
            or self.publisher.lower() in {
                "habertürk magazin", "onedio magazin", "hürriyet kelebek",
                "posta", "ntv yaşam & sanat", "trhaber", "gazete kritik",
                "sabah", "internet haber",
            }
        )
        return Article(
            id=self.article_id,
            title=self.headline,
            url=self.source_url,
            canonical_url=self.source_url,
            source=self.publisher,
            market=Market.TR if is_tr else Market.EN,
            published_at=self.published_at,
            discovered_at=self.discovered_at,
            source_category=self.category,
            snippet=self.factual_summary,
            raw_metadata={
                "review_id": self.review_id,
                "priority_score": self.discovery_priority,
                "enters_ai_cutoff_yn": self.enters_ai_cutoff_yn,
                "supporting_sources": self.supporting_sources,
                "verification_uncertainty_notes": self.verification_uncertainty_notes,
            },
        )

    def to_blind_article(self) -> Article:
        """
        Convert calibration record into a strictly blind production Article for shadow evaluation.
        Excludes all human decisions, comments, review IDs, and calibration metadata.
        """
        base = self.to_article()
        via_google_news = "news.google.com" in self.source_url
        return Article(
            id=base.id,
            title=base.title,
            url=base.url,
            canonical_url=base.canonical_url,
            source=base.source,
            market=base.market,
            published_at=base.published_at,
            discovered_at=base.discovered_at,
            source_category="" if via_google_news else base.source_category,
            snippet=base.snippet,
            raw_metadata={},
        )

    def to_owner_feedback(self) -> OwnerFeedback:
        """Convert to domain OwnerFeedback preserving owner_decision and owner_notes verbatim."""
        return OwnerFeedback(
            feedback_id=self.review_id,
            story_id=None,
            article_id=self.article_id,
            rating=self.owner_decision,
            notes=self.owner_notes,
            created_at=self.discovered_at,
        )


@dataclass
class CalibrationImportSummary:
    """Result of validating and idempotently importing owner feedback into StateRepository."""
    csv_path: str
    secondary_md_exists: bool
    total_verified_rows: int
    imported_count: int
    repository_stored_count: int
    written_notes_count: int
    decision_counts: dict[str, int]
    cutoff_matrix: dict[str, dict[str, int]]


@dataclass
class BacklogPositiveDiagnostic:
    """Diagnostic record for an owner-approved story that fell outside the top-55 cutoff."""
    review_id: str
    headline: str
    publisher: str
    category: str
    discovery_priority: float
    owner_decision: str
    owner_notes: str
    via_google_news_url: bool
    root_causes: list[str]


@dataclass
class PairConsistencyAudit:
    """Audit record for potentially duplicate or inconsistent story pairs without mutating labels."""
    pair_id: str
    first_id: str
    second_id: str
    first_headline: str
    second_headline: str
    first_publisher: str
    second_publisher: str
    first_decision: str
    second_decision: str
    token_overlap: float
    headline_too_similar: bool
    same_underlying_event: bool
    diagnostic_summary_tr: str
    engineering_handling_tr: str


@dataclass
class FactualSafetyAuditItem:
    """Verification safety check on owner-approved stories containing unverified claims."""
    review_id: str
    headline: str
    publisher: str
    category: str
    owner_decision: str
    owner_notes: str
    verification_status: VerificationStatus
    requires_verification_hold: bool
    can_trigger_immediate_alert: bool
    safety_rationale_tr: str


@dataclass
class HybridDispatchPlan:
    """Result of applying the configurable Hybrid Telegram Notification Policy to assessments."""
    immediate_breaking_alerts: list[EditorialAssessment] = field(default_factory=list)
    digest_batch: list[EditorialAssessment] = field(default_factory=list)
    deferred_for_next_digest: list[EditorialAssessment] = field(default_factory=list)
    held_for_verification: list[EditorialAssessment] = field(default_factory=list)
    held_quiet_hours: list[EditorialAssessment] = field(default_factory=list)
    suppressed: list[EditorialAssessment] = field(default_factory=list)
    formatted_breaking_messages: list[str] = field(default_factory=list)
    formatted_digest_message: Optional[str] = None
    telegram_messages_to_send_now: int = 0


@dataclass
class ShadowEvaluationDesign:
    """Pre-execution design and cost calculation for controlled live-Gemini shadow evaluation."""
    model_name: str
    total_candidates: int
    estimated_input_tokens: int
    estimated_output_tokens: int
    estimated_single_pass_cost_usd: float
    configured_budget_limit_usd: float
    within_budget: bool
    validation_criteria: list[str]
    requires_explicit_owner_approval: bool = True


def _parse_iso_datetime(val: str) -> datetime:
    cleaned = val.strip()
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_and_verify_calibration_csv(
    csv_path: Path = DEFAULT_FILLED_CALIBRATION_CSV,
    expected_count: int = 50,
) -> list[CalibrationItem]:
    """Load and strictly verify all 50 human editorial decisions and comments from the CSV.

    Raises FileNotFoundError or ValueError if the file is missing, unreadable, malformed,
    or missing any required review IDs or decisions. Never modifies human labels.
    """
    path = Path(csv_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Authoritative calibration CSV not found at: {path}")

    items: list[CalibrationItem] = []
    seen_ids: set[str] = set()

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"Calibration CSV {path} has no header row.")

        missing_cols = [c for c in REQUIRED_CSV_COLUMNS if c not in reader.fieldnames]
        if missing_cols:
            raise ValueError(f"Calibration CSV missing required columns: {missing_cols}")

        for idx, row in enumerate(reader, 1):
            review_id = (row.get("review_id") or "").strip()
            if not review_id:
                raise ValueError(f"Row {idx} is missing review_id.")
            if review_id in seen_ids:
                raise ValueError(f"Duplicate review_id detected: {review_id}")
            seen_ids.add(review_id)

            headline = (row.get("headline") or "").strip()
            publisher = (row.get("publisher") or "").strip()
            source_url = (row.get("source_url") or "").strip()
            if not headline or not publisher or not source_url:
                raise ValueError(f"Row {review_id} missing headline, publisher, or source_url.")

            decision_raw = (row.get("owner_decision") or "").strip()
            if not decision_raw:
                raise ValueError(f"Row {review_id} is missing owner_decision.")
            try:
                decision = OwnerFeedbackRating(decision_raw)
            except ValueError as exc:
                raise ValueError(f"Row {review_id} has invalid owner_decision '{decision_raw}'.") from exc

            cutoff_yn = (row.get("enters_ai_cutoff_yn") or "").strip().upper()
            if cutoff_yn not in ("Y", "N"):
                raise ValueError(f"Row {review_id} has invalid enters_ai_cutoff_yn '{cutoff_yn}'.")

            # Preserve owner_notes exactly as provided in the CSV without stripping or altering
            raw_notes = row.get("owner_notes") if row.get("owner_notes") is not None else ""

            item = CalibrationItem(
                review_id=review_id,
                headline=headline,
                publisher=publisher,
                source_url=source_url,
                published_at=_parse_iso_datetime(row["published_at"]),
                discovered_at=_parse_iso_datetime(row["discovered_at"]),
                category=(row.get("category") or "").strip(),
                factual_summary=(row.get("factual_summary") or "").strip(),
                supporting_sources=(row.get("supporting_sources") or "").strip(),
                verification_uncertainty_notes=(row.get("verification_uncertainty_notes") or "").strip(),
                discovery_priority=float(row["discovery_priority"]),
                enters_ai_cutoff_yn=cutoff_yn,
                owner_decision=decision,
                owner_notes=raw_notes,
            )
            items.append(item)

    if len(items) != expected_count:
        raise ValueError(f"Expected {expected_count} calibration items, found {len(items)}.")

    expected_ids = {f"CALIB-{i:02d}" for i in range(1, expected_count + 1)}
    if seen_ids != expected_ids:
        missing = sorted(expected_ids - seen_ids)
        extra = sorted(seen_ids - expected_ids)
        raise ValueError(f"Review ID mismatch: missing={missing}, unexpected={extra}")

    return items


def import_calibration_feedback(
    repository: StateRepository,
    csv_path: Path = DEFAULT_FILLED_CALIBRATION_CSV,
    analysis_md_path: Path = DEFAULT_CALIBRATION_ANALYSIS_MD,
) -> CalibrationImportSummary:
    """Validate and idempotently import all 50 human editorial decisions into SQLite/Turso."""
    items = load_and_verify_calibration_csv(csv_path)
    md_exists = Path(analysis_md_path).exists() and Path(analysis_md_path).is_file()

    feedbacks = [item.to_owner_feedback() for item in items]
    imported_count = repository.import_owner_feedback_batch(feedbacks)
    stored = repository.get_owner_feedback(limit=200)

    decision_counts: Counter[str] = Counter()
    cutoff_matrix: dict[str, dict[str, int]] = {
        r.value: {"total": 0, "Y": 0, "N": 0} for r in OwnerFeedbackRating
    }
    notes_count = 0

    for item in items:
        key = item.owner_decision.value
        decision_counts[key] += 1
        cutoff_matrix[key]["total"] += 1
        cutoff_matrix[key][item.enters_ai_cutoff_yn] += 1
        if item.has_owner_note:
            notes_count += 1

    return CalibrationImportSummary(
        csv_path=str(csv_path),
        secondary_md_exists=md_exists,
        total_verified_rows=len(items),
        imported_count=imported_count,
        repository_stored_count=len(stored),
        written_notes_count=notes_count,
        decision_counts=dict(decision_counts),
        cutoff_matrix=cutoff_matrix,
    )


class CalibrationAnalyzer:
    """Offline calibration analyzer for Phase 3C-2A."""

    def __init__(self, items: list[CalibrationItem]):
        self.items = items
        self.by_id: dict[str, CalibrationItem] = {item.review_id: item for item in items}

    def analyze_backlog_positives(self) -> list[BacklogPositiveDiagnostic]:
        """Analyze the 11 owner-approved or important-update stories outside the top-55 cutoff."""
        diagnostics: list[BacklogPositiveDiagnostic] = []
        for item in self.items:
            if not item.enters_ai_cutoff and item.is_editorially_positive:
                via_gn = "news.google.com" in item.source_url
                causes: list[str] = []
                if via_gn:
                    causes.append("google_news_missing_category_and_image_bonus(-3.5_pts)")
                if item.discovery_priority <= 4.5:
                    causes.append("older_publication_recency_decay")
                if item.category in ("VIRAL_CELEBRITY_GLOBAL", "MUSIC_GLOBAL", "FILM_TV_GLOBAL", "VIRAL_INTERNET_GLOBAL"):
                    causes.append("missing_english_pop_culture_event_keywords(cancels,birth,baby,viral_dance, samurai_project)")
                if item.category == "OUT_OF_SCOPE_LIFESTYLE" or "haircut" in item.headline.lower():
                    causes.append("missing_fashion_beauty_trend_signal(trend,haircut,moda,stil)")
                if item.publisher in ("TMZ", "Habertürk Magazin", "Variety", "TRHaber"):
                    causes.append("high_volume_publisher_frequency_dampening")
                if not causes:
                    causes.append("single_source_without_boost_keyword_match")

                diagnostics.append(
                    BacklogPositiveDiagnostic(
                        review_id=item.review_id,
                        headline=item.headline,
                        publisher=item.publisher,
                        category=item.category,
                        discovery_priority=item.discovery_priority,
                        owner_decision=item.owner_decision.value,
                        owner_notes=item.owner_notes,
                        via_google_news_url=via_gn,
                        root_causes=causes,
                    )
                )
        return diagnostics

    def compare_owner_preferences_with_priority(self) -> dict[str, Any]:
        """Compare the owner's 50 decisions against the deterministic candidate-priority algorithm."""
        top_55 = [i for i in self.items if i.enters_ai_cutoff]
        backlog = [i for i in self.items if not i.enters_ai_cutoff]

        top_55_pos = [i for i in top_55 if i.is_editorially_positive]
        top_55_neg = [i for i in top_55 if not i.is_editorially_positive]
        backlog_pos = [i for i in backlog if i.is_editorially_positive]
        backlog_neg = [i for i in backlog if not i.is_editorially_positive]

        pos_scores = [i.discovery_priority for i in self.items if i.is_editorially_positive]
        neg_scores = [i.discovery_priority for i in self.items if not i.is_editorially_positive]

        return {
            "total_items": len(self.items),
            "total_positive_owner_labels": len(pos_scores),
            "total_negative_owner_labels": len(neg_scores),
            "top_55_total": len(top_55),
            "top_55_positive": len(top_55_pos),
            "top_55_negative_false_positives": len(top_55_neg),
            "top_55_negative_ids": [i.review_id for i in top_55_neg],
            "backlog_total": len(backlog),
            "backlog_positive_missed": len(backlog_pos),
            "backlog_positive_ids": [i.review_id for i in backlog_pos],
            "backlog_negative_true_negatives": len(backlog_neg),
            "mean_priority_positive": round(sum(pos_scores) / len(pos_scores), 2) if pos_scores else 0.0,
            "mean_priority_negative": round(sum(neg_scores) / len(neg_scores), 2) if neg_scores else 0.0,
            "sample_cutoff_capture_rate": round(len(top_55_pos) / len(pos_scores), 3) if pos_scores else 0.0,
        }

    def audit_duplicate_and_inconsistent_pairs(self) -> list[PairConsistencyAudit]:
        """Review the 4 identified duplicate or inconsistent news pairs without overwriting human labels."""
        pair_specs = [
            (
                "PAIR-01",
                "CALIB-01",
                "CALIB-31",
                True,
                "Her iki haber de Çağatay Ulusoy'un Bulgaristan'daki köy ziyaretini konu alıyor (Habertürk vs Sabah). "
                "Kullanıcı ilkine APPROVE, ikincisine IMPORTANT_UPDATE vermiştir.",
                "İnsan etiketleri olduğu gibi korunur. Sistem düzeyinde ise çapraz yayıncı kopyası (cross-publisher duplicate) "
                "somut yeni bir olgu içermediği sürece otomatik UPDATE sayılmaz; tek olay kümesinde (cluster) birleştirilir.",
            ),
            (
                "PAIR-02",
                "CALIB-30",
                "CALIB-32",
                True,
                "Her iki haber de rapçi Lvbel C5'in tutuklama talebinin reddi ve edebiyat/kitap okuma şartını konu alıyor "
                "(Gazete Kritik vs TRHaber). Kullanıcı ilkine APPROVE, ikincisine IMPORTANT_UPDATE vermiştir.",
                "İnsan etiketleri değiştirilmez. Üretim hattında aynı adli kararın farklı yayıncı başlığı DUPLICATE olarak "
                "sönümlenir; yalnızca yeni bir mahkeme kararı veya tahliye/itiraz gelişmesi varsa UPDATE bildirimi üretilir.",
            ),
            (
                "PAIR-03",
                "CALIB-07",
                "CALIB-40",
                True,
                "Her iki haber de Olivia Culpo ve Christian McCaffrey çiftinin ikinci bebeklerinin doğumunu konu alıyor "
                "(Habertürk 'İkinci çocuğunu dünyaya getirdi' -> TOO_WEAK vs TMZ 'Olivia Culpo Gives Birth...' -> APPROVE). "
                "Türkçe başlıkta ünlü isminin gizlenmiş olması ('clickbait/belirsiz başlık') editörün ilk haberi zayıf bulmasına yol açmıştır.",
                "İnsan etiketleri korunur. Çok önemli bulgu: Başlıkta özneyi gizleyen Türk magazin manşetleri ('İkinci çocuğunu dünyaya getirdi') "
                "yerine özneyi açıkça belirten küresel/doğrudan başlıklar veya snippet'ten özne çıkarımı tercih edilmelidir.",
            ),
            (
                "PAIR-04",
                "CALIB-03",
                "CALIB-44",
                True,
                "Sezen Taş'ın 14. kattan düşerek vefatı (CALIB-03: REJECT) ve eski eşinin cenaze sonrası ilk açıklaması "
                "(CALIB-44: REJECT). Kullanıcı her iki haberi de 'olumsuz ve can sıkıcı adli/vefat haberi' olduğu için tutarlı biçimde reddetmiştir.",
                "Tutarlı negatif çift. Trajik yerel adli vefat vakaları ve bunların aile/cenaze takip haberleri Efso Magazin'in "
                "eğlence/pop-kültür tonuna uygun değildir; hem ana olay hem takip haberi elenmelidir.",
            ),
        ]

        audits: list[PairConsistencyAudit] = []
        for pair_id, id_a, id_b, same_event, summary_tr, handling_tr in pair_specs:
            a = self.by_id[id_a]
            b = self.by_id[id_b]
            tok_a = extract_key_tokens(f"{a.headline} {a.factual_summary}")
            tok_b = extract_key_tokens(f"{b.headline} {b.factual_summary}")
            overlap = round(calculate_token_overlap(tok_a, tok_b), 3)
            sim = is_headline_too_similar(a.headline, b.headline)
            audits.append(
                PairConsistencyAudit(
                    pair_id=pair_id,
                    first_id=id_a,
                    second_id=id_b,
                    first_headline=a.headline,
                    second_headline=b.headline,
                    first_publisher=a.publisher,
                    second_publisher=b.publisher,
                    first_decision=a.owner_decision.value,
                    second_decision=b.owner_decision.value,
                    token_overlap=overlap,
                    headline_too_similar=sim,
                    same_underlying_event=same_event,
                    diagnostic_summary_tr=summary_tr,
                    engineering_handling_tr=handling_tr,
                )
            )
        return audits

    def evaluate_factual_safety_on_calibration(self) -> list[FactualSafetyAuditItem]:
        """Verify that factual-safety and verification gates are preserved even on APPROVE rumors."""
        results: list[FactualSafetyAuditItem] = []
        for item in self.items:
            notes_lower = item.verification_uncertainty_notes.lower()
            cat_upper = item.category.upper()

            if cat_upper == "RUMOR_UNVERIFIED" or "kara büyü" in notes_lower or "teyitsiz" in notes_lower:
                v_status = VerificationStatus.UNVERIFIED_RUMOR
                hold = True
                can_immediate = False
                rationale = (
                    "Editör konuyu ilginç bulup APPROVE vermiş olsa dahi, kaynak teyitsiz/spekülatif iddia "
                    "içerdiğinden güvenlik kapısı (Factual Grounding Gate) baypas edilmez. Kesin olgu gibi "
                    "sunulamaz ve acil bildirim tetiklemez; HOLD_FOR_VERIFICATION statüsünde tutulur."
                )
            elif "kulislere dayalı iddia" in notes_lower or "insider claims" in notes_lower:
                v_status = VerificationStatus.ATTRIBUTED_CLAIM
                hold = True
                can_immediate = False
                rationale = (
                    "Kulis/insider iddiası ('Claims Insider'). Editör 'Dedikodu insanların ilgisini çeker' "
                    "notuyla APPROVE vermiştir; ancak kesinleşmiş olgu değildir. Başlıkta açık iddia atfı "
                    "zorunludur ve acil kırılım (immediate alert) yerine doğrulama/özet katmanında değerlendirilir."
                )
            elif "hassas" in notes_lower or "vefat" in notes_lower:
                v_status = VerificationStatus.REPUTABLE_REPORTING
                hold = False
                can_immediate = False
                rationale = (
                    "Hassas vefat/taziye içeriği. Doğrulanmış olsa bile sansasyonel dil kullanılamaz ve "
                    "otomatik acil bildirim yerine editoryal özet/insan kontrolü ile sunulmalıdır."
                )
            else:
                v_status = VerificationStatus.REPUTABLE_REPORTING
                hold = False
                can_immediate = item.is_editorially_positive
                rationale = "Doğrulanmış birinci ağız beyan veya saygın yayıncı haberi."

            if item.review_id in ("CALIB-27", "CALIB-28", "CALIB-47", "CALIB-03", "CALIB-11", "CALIB-44"):
                results.append(
                    FactualSafetyAuditItem(
                        review_id=item.review_id,
                        headline=item.headline,
                        publisher=item.publisher,
                        category=item.category,
                        owner_decision=item.owner_decision.value,
                        owner_notes=item.owner_notes,
                        verification_status=v_status,
                        requires_verification_hold=hold,
                        can_trigger_immediate_alert=can_immediate,
                        safety_rationale_tr=rationale,
                    )
                )
        return results


class HybridNotificationPlanner:
    """Implements the Product Owner's configurable Hybrid Telegram Notification Strategy.

    Separates:
    1. Editorial Relevance (`score >= min_score` and `kind in (NEW, UPDATE)`)
    2. Verification Confidence (`OFFICIAL_CONFIRMATION`, `FIRST_PARTY_STATEMENT`, `REPUTABLE_REPORTING`
       vs `ATTRIBUTED_CLAIM`, `UNVERIFIED_RUMOR`, `SPECULATION`)
    3. Notification Urgency (`BREAKING_IMMEDIATE`, `PERIODIC_DIGEST`, `HOLD_FOR_VERIFICATION`, `SUPPRESSED`)
    4. Daily & Per-Digest Capacity Governance (never silently discards qualifying stories)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def classify_urgency_tier(self, assessment: EditorialAssessment) -> NotificationUrgencyTier:
        """Determine the notification urgency tier for an AI editorial assessment."""
        if assessment.kind in (EditorialKind.IGNORE, EditorialKind.DUPLICATE):
            return NotificationUrgencyTier.SUPPRESSED

        if assessment.score < self.settings.min_score:
            return NotificationUrgencyTier.SUPPRESSED

        # Factual-safety & verification gate: unverified rumors or sensitive claims never trigger immediate breaking alerts
        if assessment.verification_status in (
            VerificationStatus.UNVERIFIED_RUMOR,
            VerificationStatus.SPECULATION,
            VerificationStatus.ATTRIBUTED_CLAIM,
        ):
            return NotificationUrgencyTier.HOLD_FOR_VERIFICATION

        if assessment.uncertainties_tr and (
            "hassas" in assessment.uncertainties_tr.lower()
            or "resmi doğrulama bulunmuyor" in assessment.uncertainties_tr.lower()
        ):
            return NotificationUrgencyTier.HOLD_FOR_VERIFICATION

        if self.settings.notification_strategy == "DIGEST_ONLY":
            return NotificationUrgencyTier.PERIODIC_DIGEST

        if self.settings.notification_strategy == "IMMEDIATE":
            return NotificationUrgencyTier.BREAKING_IMMEDIATE

        # Default HYBRID strategy:
        # Only exceptionally important, sufficiently verified stories (>= breaking_alert_min_score) trigger immediate alerts
        if assessment.score >= self.settings.breaking_alert_min_score:
            return NotificationUrgencyTier.BREAKING_IMMEDIATE

        return NotificationUrgencyTier.PERIODIC_DIGEST

    def plan_dispatch(
        self,
        assessments: list[EditorialAssessment],
        messages_sent_today: int = 0,
        is_digest_due: bool = True,
        now: Optional[datetime] = None,
    ) -> HybridDispatchPlan:
        """Plan notification delivery across immediate breaking alerts and periodic 2-hour digests.

        Guarantees that qualifying stories are NEVER silently discarded when daily or per-digest
        caps are reached; excess qualifying stories are deferred to `deferred_for_next_digest`.
        """
        plan = HybridDispatchPlan()
        in_quiet_hours = self.settings.is_in_quiet_hours(now)
        remaining_daily_budget = max(0, self.settings.target_daily_telegram_messages - messages_sent_today)

        # Sort qualifying candidates by score descending so highest priority stories get budget priority
        sorted_assessments = sorted(assessments, key=lambda a: a.score, reverse=True)

        for item in sorted_assessments:
            tier = self.classify_urgency_tier(item)
            if tier == NotificationUrgencyTier.SUPPRESSED:
                plan.suppressed.append(item)
                continue

            if tier == NotificationUrgencyTier.HOLD_FOR_VERIFICATION:
                plan.held_for_verification.append(item)
                continue

            if in_quiet_hours:
                if (
                    self.settings.quiet_hours_behavior == "ALLOW_BREAKING_95"
                    and item.score >= 95
                    and tier == NotificationUrgencyTier.BREAKING_IMMEDIATE
                ):
                    pass  # Allowed to proceed as breaking alert
                else:
                    plan.held_quiet_hours.append(item)
                    continue

            if tier == NotificationUrgencyTier.BREAKING_IMMEDIATE:
                if remaining_daily_budget > 0:
                    plan.immediate_breaking_alerts.append(item)
                    plan.formatted_breaking_messages.append(format_telegram_alert(item))
                    remaining_daily_budget -= 1
                else:
                    # Budget reached: never silently discard! Downgrade to periodic digest queue
                    plan.deferred_for_next_digest.append(item)
            elif tier == NotificationUrgencyTier.PERIODIC_DIGEST:
                if is_digest_due and remaining_daily_budget > 0:
                    if len(plan.digest_batch) < self.settings.max_items_per_digest:
                        plan.digest_batch.append(item)
                    else:
                        plan.deferred_for_next_digest.append(item)
                else:
                    plan.deferred_for_next_digest.append(item)

        if plan.digest_batch and is_digest_due and remaining_daily_budget > 0:
            hours_label = max(1, self.settings.digest_interval_minutes // 60)
            plan.formatted_digest_message = format_telegram_digest(
                plan.digest_batch,
                window_label=f"{hours_label} Saatlik Editoryal Özet",
            )
            remaining_daily_budget -= 1

        plan.telegram_messages_to_send_now = len(plan.formatted_breaking_messages) + (
            1 if plan.formatted_digest_message else 0
        )
        return plan


def design_shadow_gemini_evaluation(
    items: list[CalibrationItem],
    settings: Optional[Settings] = None,
    recent_stories: Optional[list[Story]] = None,
) -> ShadowEvaluationDesign:
    """Design the controlled real-Gemini shadow evaluation with token and cost estimates.

    Does NOT make any live network or API calls.
    """
    cfg = settings or Settings()
    articles = [item.to_blind_article() for item in items]
    stories = recent_stories or []
    ref_now = max((a.published_at for a in articles), default=datetime.now(timezone.utc))

    prompt_text = build_editorial_prompt(articles, stories, ref_now)
    est_in_tokens = estimate_tokens_from_text(prompt_text)
    est_out_tokens = cfg.max_output_tokens
    est_cost = estimate_call_cost(
        cfg.gemini_model,
        input_tokens=est_in_tokens,
        output_tokens=est_out_tokens,
        used_search_grounding=False,
    )

    criteria = [
        "1. Şema Doğrulaması (Structured Outputs): Tüm model yanıtları `EditorialDecisionBatch` Pydantic şemasından %100 hatasız geçmelidir.",
        "2. Anti-Halüsinasyon Kaynak Kontrolü: Modelin seçtiği tüm `article_ids` değerleri yalnızca girdi listesindeki 50 gerçek adaydan oluşmalıdır.",
        "3. Negatif Filtre (False-Positive Suppression): İnsan editörün `REJECT` (10 haber) ve `TOO_WEAK` (6 haber) verdiği 16 haberde acil bildirim (breaking alert) oranı %0-10 aralığında tutulmalıdır.",
        "4. Olgusal Güvenlik Kapısı (Factual-Safety Gate): `CALIB-28` (Taylor Swift kulis iddiası) ve `CALIB-47` (Tanushree Dutta kara büyü iddiası) insan tarafından `APPROVE` edilmiş olsa dahi, kesin olgu gibi başlıklandırılmamalı ve `HOLD_FOR_VERIFICATION` / `ATTRIBUTED_CLAIM` kapısından geçmelidir.",
        "5. Çift / Mükerrer Ayrımı (Pair Consistency): `CALIB-01/31`, `CALIB-30/32` ve `CALIB-07/40` çiftlerinde aynı olay iki kez bağımsız `NEW` bildirimine dönüşmemelidir.",
        "6. Hibrit Bildirim Katmanlaması: Onaylanan hikayeler acil kırılım (`BREAKING_IMMEDIATE`, skor >= 92 ve teyitli) ile 6 saatlik periyodik özet (`PERIODIC_DIGEST`, skor 78-91) arasında dengeli dağıtılmalıdır.",
    ]

    return ShadowEvaluationDesign(
        model_name=cfg.gemini_model,
        total_candidates=len(articles),
        estimated_input_tokens=est_in_tokens,
        estimated_output_tokens=est_out_tokens,
        estimated_single_pass_cost_usd=est_cost,
        configured_budget_limit_usd=cfg.max_shadow_eval_budget_usd,
        within_budget=est_cost <= cfg.max_shadow_eval_budget_usd,
        validation_criteria=criteria,
        requires_explicit_owner_approval=True,
    )


from efso_radar.ai.phase3c2b_eval import (  # noqa: E402, F401
    BlindShadowComparisonMetrics,
    BlindShadowEvaluationRunner,
    CONSTRUCTED_REGRESSION_SCENARIOS,
    ConstructedRegressionSample,
    DiscoveryRankingComparisonReport,
    HoldoutSample,
    INDEPENDENT_HOLDOUT_SET,
    MonthlyCostProjection,
    NotificationThresholdScenarioResult,
    PairClusteringComparison,
    RankingSnapshotMetrics,
    ScheduleOptionSimulation,
    ShadowPredictionRecord,
    evaluate_constructed_regression_set,
    evaluate_discovery_ranking_comparison,
    evaluate_independent_holdout_set,
    evaluate_notification_quality,
    model_monthly_operating_costs,
    run_phase3c_preflight_dry_run,
    simulate_monitoring_schedules,
)

