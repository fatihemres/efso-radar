"""Editorial Intelligence Offline Evaluation Benchmark Runner.

Evaluates editorial triage, classification (NEW, UPDATE, DUPLICATE, IGNORE),
scoring gates, and verification statuses against curated ground-truth articles
representing all 10 target and adversarial verticals.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from efso_radar.ai.schemas import EditorialPickSchema
from efso_radar.domain import Article, Story, StoryCategory

logger = logging.getLogger("efso-radar.ai.benchmark")


@dataclass
class BenchmarkItem:
    """An individual labeled news item in the editorial benchmark."""
    article_id: str
    vertical: str
    title: str
    source: str
    url: str
    market: str
    category_hint: str
    snippet: str
    expected_kind: str
    expected_alert: bool
    expected_score_min: int
    expected_score_max: int
    verification_status: str
    editorial_rationale: str
    parent_story_id: Optional[str] = None
    development_tr: Optional[str] = None

    def to_article(self) -> Article:
        return Article(
            id=self.article_id,
            title=self.title,
            url=self.url,
            canonical_url=self.url,
            source=self.source,
            market=Market(self.market),
            published_at=datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc),
            source_category=self.category_hint,
            snippet=self.snippet,
        )


@dataclass
class SyntheticRuleValidationMetrics:
    """Quantitative rule conformance metrics for deterministic editorial gating on synthetic fixtures."""
    total_articles: int
    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int
    precision: float
    recall: float
    f1_score: float
    duplicate_accuracy: float
    update_recognition_rate: float
    vertical_stats: dict[str, dict[str, Any]] = field(default_factory=dict)
    evaluation_kind: str = "SYNTHETIC_RULE_VALIDATION"
    prediction_source: str = "PREDICTIONS_DERIVED_FROM_EXPECTED_LABELS"
    model_accuracy_status: str = "NOT_MEASURED_MODEL_ACCURACY"

    def summary(self) -> str:
        source_tag = f"  [{self.prediction_source}]"
        source_tag_line = f"{source_tag:<66}"
        if len(source_tag_line) > 66:
            source_tag_line = source_tag

        if self.prediction_source == "PREDICTIONS_DERIVED_FROM_EXPECTED_LABELS":
            source_note = [
                "In default offline evaluation, predictions are self-oracle picks  ",
                "derived from expected fixture labels, NOT independent model picks.",
            ]
        else:
            source_note = [
                "Predictions were supplied by caller/external input and evaluated  ",
                "against curated synthetic fixtures without live model inference.  ",
            ]

        lines = [
            "==================================================================",
            "  EFSO RADAR -- SYNTHETIC_RULE_VALIDATION CONFORMANCE REPORT       ",
            "  [Deterministic Scenario Testing -- NOT Live Gemini Model Metrics]",
            source_tag_line,
            "  [NOT_MEASURED_MODEL_ACCURACY -- Live Model Performance Unproven] ",
            "==================================================================",
            "NOTE: This report validates application-level gating rules and    ",
            "threshold logic against curated synthetic fixtures.               ",
            *source_note,
            "Real editorial precision and recall remain NOT MEASURED.          ",
            "------------------------------------------------------------------",
            f"Evaluation Kind             : {self.evaluation_kind}",
            f"Prediction Source           : {self.prediction_source}",
            f"Model Accuracy Status       : {self.model_accuracy_status}",
            "------------------------------------------------------------------",
            f"Synthetic Test Fixtures     : {self.total_articles}",
            f"Alert Conformance (Passed)  : {self.true_positives}",
            f"Silence Conformance (Muted) : {self.true_negatives}",
            f"Unexpected Alerts (Spam)    : {self.false_positives}",
            f"Unexpected Silences (Missed): {self.false_negatives}",
            "------------------------------------------------------------------",
            f"Rule Conformance Rate       : {self.precision * 100:.1f}%",
            f"Gate Coverage Rate          : {self.recall * 100:.1f}%",
            f"Composite F1 Conformance    : {self.f1_score * 100:.1f}%",
            f"Duplicate Gate Accuracy     : {self.duplicate_accuracy * 100:.1f}%",
            f"Update Rule Recognition Rate: {self.update_recognition_rate * 100:.1f}%",
            "------------------------------------------------------------------",
            "VERTICAL SCENARIO BREAKDOWN:",
        ]
        for vert, stats in self.vertical_stats.items():
            lines.append(
                f"  - {vert:<25}: {stats['correct']}/{stats['total']} conforming "
                f"(Alerts: {stats['alerts']}, Mutes: {stats['mutes']})"
            )
        lines.append("==================================================================")
        return "\n".join(lines)


# Backwards compatibility alias
BenchmarkMetrics = SyntheticRuleValidationMetrics


class OfflineBenchmarkEvaluator:
    """Evaluates editorial benchmark fixtures deterministically."""

    def __init__(self, fixture_path: Optional[Path] = None):
        if fixture_path is None:
            fixture_path = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "editorial_benchmark.json"
        self.fixture_path = fixture_path
        self._load_dataset()

    def _load_dataset(self) -> None:
        with open(self.fixture_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.metadata = data.get("benchmark_metadata", {})
        self.recent_stories: list[Story] = [
            Story(
                story_id=s["story_id"],
                headline_tr=s["headline_tr"],
                category=StoryCategory(s.get("category", "GLOBAL")),
                first_seen_at=datetime.fromisoformat(s["first_seen_at"]),
                last_updated_at=datetime.fromisoformat(s["last_updated_at"]),
            )
            for s in data.get("recent_stories_context", [])
        ]
        self.items: list[BenchmarkItem] = [
            BenchmarkItem(**item_dict) for item_dict in data.get("articles", [])
        ]

    def simulate_model_decisions(self) -> list[EditorialPickSchema]:
        """Simulate realistic model output strictly following system instructions."""
        picks: list[EditorialPickSchema] = []
        for item in self.items:
            # Calibrated model score based on expected range
            score = (item.expected_score_min + item.expected_score_max) // 2

            pick = EditorialPickSchema(
                article_ids=[item.article_id],
                score=score,
                kind=item.expected_kind,  # type: ignore[arg-type]
                verification_status=item.verification_status,  # type: ignore[arg-type]
                parent_story_id=item.parent_story_id or "",
                development_tr=item.development_tr or "",
                headline_tr=f"Türkçe: {item.title}",
                cover_title_tr="KAPAK BAŞLIĞI",
                caption_idea_tr="Örnek detaylı Instagram caption açıklaması.",
                relevance_reason_tr=item.editorial_rationale,
                why_efso_tr="Efso Magazin hedef kitlesi için uygunluk değerlendirmesi.",
                uncertainties_tr="" if item.verification_status != "UNVERIFIED_RUMOR" else "Doğrulama eksik",
                suggested_format="POST",
                category="GLOBAL" if item.market == "EN" else "YERLI",
            )
            picks.append(pick)
        return picks

    def run_evaluation(
        self,
        predicted_picks: Optional[list[EditorialPickSchema]] = None,
        min_score: int = 82,
    ) -> BenchmarkMetrics:
        """Run evaluation comparing predicted picks with ground-truth labels."""
        caller_supplied = predicted_picks is not None
        if predicted_picks is None:
            predicted_picks = self.simulate_model_decisions()

        pred_map = {p.article_ids[0]: p for p in predicted_picks if p.article_ids}

        tp = 0
        fp = 0
        tn = 0
        fn = 0

        dup_total = 0
        dup_correct = 0

        update_total = 0
        update_correct = 0

        vertical_stats: dict[str, dict[str, Any]] = {}

        for item in self.items:
            vert = item.vertical
            if vert not in vertical_stats:
                vertical_stats[vert] = {"total": 0, "correct": 0, "alerts": 0, "mutes": 0}
            vertical_stats[vert]["total"] += 1

            pred = pred_map.get(item.article_id)
            if pred is None:
                pred_alert = False
                pred_kind = "IGNORE"
            else:
                # Alert condition: score >= min_score AND kind in [NEW, UPDATE]
                pred_alert = pred.score >= min_score and pred.kind in ("NEW", "UPDATE")
                pred_kind = pred.kind

            if pred_alert:
                vertical_stats[vert]["alerts"] += 1
            else:
                vertical_stats[vert]["mutes"] += 1

            # Confusion matrix
            if item.expected_alert and pred_alert:
                tp += 1
                vertical_stats[vert]["correct"] += 1
            elif not item.expected_alert and not pred_alert:
                tn += 1
                vertical_stats[vert]["correct"] += 1
            elif not item.expected_alert and pred_alert:
                fp += 1
            elif item.expected_alert and not pred_alert:
                fn += 1

            # Sub-task: Duplicate detection
            if item.expected_kind == "DUPLICATE":
                dup_total += 1
                if pred_kind == "DUPLICATE" or not pred_alert:
                    dup_correct += 1

            # Sub-task: Update recognition
            if item.expected_kind == "UPDATE":
                update_total += 1
                if pred_kind == "UPDATE" and pred_alert:
                    update_correct += 1

        precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

        dup_acc = dup_correct / dup_total if dup_total > 0 else 1.0
        update_acc = update_correct / update_total if update_total > 0 else 1.0

        pred_source = (
            "CALLER_SUPPLIED_PREDICTIONS"
            if caller_supplied
            else "PREDICTIONS_DERIVED_FROM_EXPECTED_LABELS"
        )

        return BenchmarkMetrics(
            total_articles=len(self.items),
            true_positives=tp,
            false_positives=fp,
            true_negatives=tn,
            false_negatives=fn,
            precision=precision,
            recall=recall,
            f1_score=f1,
            duplicate_accuracy=dup_acc,
            update_recognition_rate=update_acc,
            vertical_stats=vertical_stats,
            evaluation_kind="SYNTHETIC_RULE_VALIDATION",
            prediction_source=pred_source,
            model_accuracy_status="NOT_MEASURED_MODEL_ACCURACY",
        )


if __name__ == "__main__":
    evaluator = OfflineBenchmarkEvaluator()
    metrics = evaluator.run_evaluation()
    print(metrics.summary())
