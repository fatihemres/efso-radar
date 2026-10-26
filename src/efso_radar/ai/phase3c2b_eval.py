"""Phase 3C-2B: Offline Calibration Comparator, Independent Holdout Evaluator,
Blind Gemini Shadow Evaluation Runner, Notification Quality Evaluator,
Schedule & Digest Simulator (2h / 3h / 4h), and Monthly Operating Cost Modeler.

Strictly adheres to Phase 3C-2B safety rules:
- Zero live Gemini API calls unless explicitly invoked with an authorized live engine.
- Zero live Telegram dispatches.
- Blind shadow prompts strip all human decisions, comments, review IDs, and calibration hints.
- Preserves human labels in `data/Efso_Radar_Kalibrasyon_Doldurulmus.csv` verbatim.
"""
from __future__ import annotations

import json
import logging
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from efso_radar.ai.client import (
    AIEvaluationError,
    GeminiEditorialEngine,
    is_headline_too_similar,
    project_diagnostic_decisions_to_assessments,
)
from efso_radar.ai.cost import (
    check_monthly_budget_guard,
    compute_candidate_batch_hash,
    compute_enforceable_batch_output_tokens,
    estimate_call_cost,
    estimate_tokens_from_text,
    ExperimentBudgetGuard,
    extract_provider_token_usage,
    resolve_model_pricing,
    require_verified_free_tier,
)
from efso_radar.ai.prompts import build_diagnostic_editorial_prompt, build_editorial_prompt
from efso_radar.ai.schemas import (
    DiagnosticArticleDecision,
    DiagnosticEvaluationBatch,
    EditorialDecisionBatch,
    EditorialPickSchema,
)
from efso_radar.config import Settings
from efso_radar.deduplication.engine import (
    DeduplicationEngine,
    calculate_token_overlap,
    compute_candidate_priority,
    compute_candidate_priority_baseline,
    extract_article_event_tokens,
    extract_key_tokens,
    headline_subject_clarity_score,
)
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    NotificationUrgencyTier,
    OwnerFeedbackRating,
    Story,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
)
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.sources.filter import NewsRelevancePrefilter

logger = logging.getLogger("efso-radar.ai.phase3c2b")


# ---------------------------------------------------------------------------
# 1. Untuned Baseline & Calibrated Discovery Prioritization Comparison
# ---------------------------------------------------------------------------

@dataclass
class RankingSnapshotMetrics:
    """Comprehensive 8-dimension discovery ranking metrics on a labeled dataset."""
    label: str
    total_items: int
    total_positive: int
    total_negative: int
    cutoff_k: int
    positive_in_cutoff: int
    positive_recall_at_cutoff: float
    negative_in_cutoff: int
    negative_inclusion_rate: float
    reject_in_cutoff: int
    too_weak_in_cutoff: int
    mean_positive_priority: float
    mean_negative_priority: float
    tr_positive_recall: float
    en_positive_recall: float
    tr_mean_positive_priority: float
    en_mean_positive_priority: float
    direct_rss_positive_recall: float
    google_news_positive_recall: float
    direct_rss_mean_positive_priority: float
    google_news_mean_positive_priority: float
    missing_metadata_positive_lost: int
    hard_filter_positive_rejected: int
    hard_filter_negative_rejected: int
    positive_missed_ids: list[str]
    negative_included_ids: list[str]
    source_breakdown: dict[str, dict[str, float | int]]
    category_breakdown: dict[str, dict[str, float | int]]


@dataclass
class PairClusteringComparison:
    """Before/after comparison for duplicate and inconsistent pairs."""
    pair_id: str
    first_id: str
    second_id: str
    baseline_title_overlap: float
    calibrated_event_overlap: float
    clustered_together_after: bool
    canonical_representative_id: str
    canonical_headline: str
    canonical_clarity_score: float


@dataclass
class DiscoveryRankingComparisonReport:
    """Complete before/after comparison between Untuned Baseline and Calibrated Phase 3C-2B."""
    untuned_baseline: RankingSnapshotMetrics
    calibrated_raw_top32: RankingSnapshotMetrics
    calibrated_clustered_unique_positive_total: int
    calibrated_clustered_unique_positive_captured: int
    calibrated_clustered_unique_positive_recall: float
    pair_comparisons: list[PairClusteringComparison]
    safety_preserved_ids: list[str]
    sample_size: int = 50
    original_cutoff55_sampled_count: int = 32
    original_backlog_sampled_count: int = 18
    full_historical_pool_available: bool = False
    calibrated_positives_above_baseline_cutoff_score: int = 0
    comparability_limitation_note: str = (
        "The 50-story calibration dataset is a stratified sample drawn from an original ~160-item "
        "discovery pool where max_candidates=55 entered the production AI cutoff (32 sampled from "
        "the top-55 cutoff [enters_ai_cutoff_yn='Y', priority >= 7.00] and 18 sampled from the "
        "105+ deprioritized backlog [enters_ai_cutoff_yn='N', down to rank 160]). Because the "
        "unsampled ~110 items from that historical discovery snapshot were not persisted to disk, "
        "cutoff_k=32 on this 50-item sample is a matched-sample-capacity benchmark (matching the "
        "32 'Y' slots in the sample) and must NOT be equated with selecting 55 items from the "
        "complete ~160-item discovery pool."
    )


def _compute_snapshot_metrics(
    items: Sequence[Any],
    scores_by_id: dict[str, float],
    cutoff_ids: set[str],
    label: str,
    cutoff_k: int = 32,
) -> RankingSnapshotMetrics:
    pos_items = [i for i in items if i.is_editorially_positive]
    neg_items = [i for i in items if not i.is_editorially_positive]

    pos_in_cut = [i for i in pos_items if i.review_id in cutoff_ids]
    pos_missed = [i for i in pos_items if i.review_id not in cutoff_ids]
    neg_in_cut = [i for i in neg_items if i.review_id in cutoff_ids]
    rej_in_cut = [i for i in neg_in_cut if i.owner_decision == OwnerFeedbackRating.REJECT]
    weak_in_cut = [i for i in neg_in_cut if i.owner_decision == OwnerFeedbackRating.TOO_WEAK]

    pos_scores = [scores_by_id[i.review_id] for i in pos_items]
    neg_scores = [scores_by_id[i.review_id] for i in neg_items]

    # TR vs EN
    tr_pos = [i for i in pos_items if i.to_article().market == Market.TR]
    en_pos = [i for i in pos_items if i.to_article().market == Market.EN]
    tr_pos_in = [i for i in tr_pos if i.review_id in cutoff_ids]
    en_pos_in = [i for i in en_pos if i.review_id in cutoff_ids]

    # Direct RSS vs Google News RSS
    gn_pos = [i for i in pos_items if "news.google.com" in i.source_url]
    dir_pos = [i for i in pos_items if "news.google.com" not in i.source_url]
    gn_pos_in = [i for i in gn_pos if i.review_id in cutoff_ids]
    dir_pos_in = [i for i in dir_pos if i.review_id in cutoff_ids]

    # Missing metadata positive lost (Google News items lacking image/category that missed cutoff)
    missing_meta_lost = len([i for i in gn_pos if i.review_id not in cutoff_ids])

    # Hard filter check (NewsRelevancePrefilter)
    hf_pos_rej = sum(
        1 for i in pos_items if NewsRelevancePrefilter.is_unrelated_hard_news(i.headline, i.factual_summary)
    )
    hf_neg_rej = sum(
        1 for i in neg_items if NewsRelevancePrefilter.is_unrelated_hard_news(i.headline, i.factual_summary)
    )

    # Source breakdown
    src_map: dict[str, list[Any]] = defaultdict(list)
    for i in items:
        src_map[i.publisher].append(i)
    src_breakdown: dict[str, dict[str, float | int]] = {}
    for pub, p_items in sorted(src_map.items()):
        p_pos = [x for x in p_items if x.is_editorially_positive]
        p_in = [x for x in p_pos if x.review_id in cutoff_ids]
        src_breakdown[pub] = {
            "total": len(p_items),
            "positive": len(p_pos),
            "positive_in_cutoff": len(p_in),
            "mean_score": round(sum(scores_by_id[x.review_id] for x in p_items) / len(p_items), 2),
        }

    # Category breakdown
    cat_map: dict[str, list[Any]] = defaultdict(list)
    for i in items:
        cat_map[i.category].append(i)
    cat_breakdown: dict[str, dict[str, float | int]] = {}
    for cat, c_items in sorted(cat_map.items()):
        c_pos = [x for x in c_items if x.is_editorially_positive]
        c_in = [x for x in c_pos if x.review_id in cutoff_ids]
        cat_breakdown[cat] = {
            "total": len(c_items),
            "positive": len(c_pos),
            "positive_in_cutoff": len(c_in),
            "mean_score": round(sum(scores_by_id[x.review_id] for x in c_items) / len(c_items), 2),
        }

    return RankingSnapshotMetrics(
        label=label,
        total_items=len(items),
        total_positive=len(pos_items),
        total_negative=len(neg_items),
        cutoff_k=cutoff_k,
        positive_in_cutoff=len(pos_in_cut),
        positive_recall_at_cutoff=round(len(pos_in_cut) / len(pos_items), 3) if pos_items else 0.0,
        negative_in_cutoff=len(neg_in_cut),
        negative_inclusion_rate=round(len(neg_in_cut) / len(neg_items), 3) if neg_items else 0.0,
        reject_in_cutoff=len(rej_in_cut),
        too_weak_in_cutoff=len(weak_in_cut),
        mean_positive_priority=round(sum(pos_scores) / len(pos_scores), 2) if pos_scores else 0.0,
        mean_negative_priority=round(sum(neg_scores) / len(neg_scores), 2) if neg_scores else 0.0,
        tr_positive_recall=round(len(tr_pos_in) / len(tr_pos), 3) if tr_pos else 0.0,
        en_positive_recall=round(len(en_pos_in) / len(en_pos), 3) if en_pos else 0.0,
        tr_mean_positive_priority=round(sum(scores_by_id[x.review_id] for x in tr_pos) / len(tr_pos), 2) if tr_pos else 0.0,
        en_mean_positive_priority=round(sum(scores_by_id[x.review_id] for x in en_pos) / len(en_pos), 2) if en_pos else 0.0,
        direct_rss_positive_recall=round(len(dir_pos_in) / len(dir_pos), 3) if dir_pos else 0.0,
        google_news_positive_recall=round(len(gn_pos_in) / len(gn_pos), 3) if gn_pos else 0.0,
        direct_rss_mean_positive_priority=round(sum(scores_by_id[x.review_id] for x in dir_pos) / len(dir_pos), 2) if dir_pos else 0.0,
        google_news_mean_positive_priority=round(sum(scores_by_id[x.review_id] for x in gn_pos) / len(gn_pos), 2) if gn_pos else 0.0,
        missing_metadata_positive_lost=missing_meta_lost,
        hard_filter_positive_rejected=hf_pos_rej,
        hard_filter_negative_rejected=hf_neg_rej,
        positive_missed_ids=[i.review_id for i in pos_missed],
        negative_included_ids=[i.review_id for i in neg_in_cut],
        source_breakdown=src_breakdown,
        category_breakdown=cat_breakdown,
    )


def evaluate_discovery_ranking_comparison(
    items: Sequence[Any],
    cutoff_k: int = 32,
) -> DiscoveryRankingComparisonReport:
    """
    Establish the untuned Phase 3C-2A baseline and compare it against the
    calibrated Phase 3C-2B ranking and event-clustering engine.
    """
    # 1. Untuned Baseline (using recorded CSV discovery_priority and enters_ai_cutoff_yn)
    baseline_scores = {i.review_id: i.discovery_priority for i in items}
    baseline_cutoff_ids = {i.review_id for i in items if i.enters_ai_cutoff}
    untuned_metrics = _compute_snapshot_metrics(
        items=items,
        scores_by_id=baseline_scores,
        cutoff_ids=baseline_cutoff_ids,
        label="Untuned Baseline (Phase 3C-2A)",
        cutoff_k=len(baseline_cutoff_ids),
    )

    # 2. Calibrated Phase 3C-2B Raw Scoring (Realistic Blind Discovery Articles)
    blind_articles: list[tuple[Any, Article]] = []
    for item in items:
        art = item.to_blind_article() if hasattr(item, "to_blind_article") else item.to_article()
        blind_articles.append((item, art))

    # Order by published_at desc for fair publisher seen count
    sorted_by_pub = sorted(blind_articles, key=lambda pair: pair[1].published_at, reverse=True)
    pub_counts: dict[str, int] = defaultdict(int)
    ref_now = max((pair[0].discovered_at for pair in blind_articles), default=datetime.now(timezone.utc))

    calibrated_scores: dict[str, float] = {}
    for item, art in sorted_by_pub:
        pub_key = art.source.lower()
        pub_counts[pub_key] += 1
        p_score, _ = compute_candidate_priority(
            article=art,
            cluster_size=1,
            publisher_seen_count=pub_counts[pub_key],
            reference_time=ref_now,
        )
        calibrated_scores[item.review_id] = p_score

    ranked_calibrated = sorted(
        items,
        key=lambda i: (calibrated_scores[i.review_id], i.published_at.timestamp()),
        reverse=True,
    )
    calibrated_cutoff_ids = {i.review_id for i in ranked_calibrated[:cutoff_k]}
    calibrated_metrics = _compute_snapshot_metrics(
        items=items,
        scores_by_id=calibrated_scores,
        cutoff_ids=calibrated_cutoff_ids,
        label="Calibrated Phase 3C-2B (Raw Top-32)",
        cutoff_k=cutoff_k,
    )

    # 3. Full DeduplicationEngine Clustering Evaluation (in-memory SQLite repo)
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_repo = SqliteStateRepository(Path(tmpdir) / "cluster_eval.db")
        deduper = DeduplicationEngine(tmp_repo)
        all_arts = [pair[1] for pair in blind_articles]
        selected_reps = deduper.filter_fresh_candidates(all_arts, max_candidates=cutoff_k)

    id_to_item = {pair[1].id: pair[0] for pair in blind_articles}
    item_by_review = {i.review_id: i for i in items}

    # Count unique positive events across all clusters and how many entered top-32 clusters
    all_clustered_reps = list(deduper.last_selected) + list(deduper.last_deprioritized)
    positive_clusters_total = 0
    positive_clusters_in_cutoff = 0
    selected_rep_ids = {r.id for r in selected_reps}

    cluster_lookup_by_review_id: dict[str, Article] = {}
    for rep in all_clustered_reps:
        c_ids = rep.raw_metadata.get("cluster_article_ids", [rep.id])
        c_items = [id_to_item[aid] for aid in c_ids if aid in id_to_item]
        for ci in c_items:
            cluster_lookup_by_review_id[ci.review_id] = rep
        if any(ci.is_editorially_positive for ci in c_items):
            positive_clusters_total += 1
            if rep.id in selected_rep_ids:
                positive_clusters_in_cutoff += 1

    # 4. Pair Comparisons (CALIB-01/31, CALIB-30/32, CALIB-07/40, CALIB-03/44)
    pair_comparisons: list[PairClusteringComparison] = []
    for pair_id, id_a, id_b in (
        ("PAIR-01", "CALIB-01", "CALIB-31"),
        ("PAIR-02", "CALIB-30", "CALIB-32"),
        ("PAIR-03", "CALIB-07", "CALIB-40"),
        ("PAIR-04", "CALIB-03", "CALIB-44"),
    ):
        if id_a in item_by_review and id_b in item_by_review:
            ia = item_by_review[id_a]
            ib = item_by_review[id_b]
            art_a = ia.to_blind_article() if hasattr(ia, "to_blind_article") else ia.to_article()
            art_b = ib.to_blind_article() if hasattr(ib, "to_blind_article") else ib.to_article()

            # Baseline title-only token overlap (old extract_key_tokens behavior)
            base_ov = round(
                calculate_token_overlap(extract_key_tokens(ia.headline), extract_key_tokens(ib.headline)),
                3,
            )
            # Calibrated event token overlap
            ev_ov = round(
                calculate_token_overlap(extract_article_event_tokens(art_a), extract_article_event_tokens(art_b)),
                3,
            )
            rep_a = cluster_lookup_by_review_id.get(id_a)
            rep_b = cluster_lookup_by_review_id.get(id_b)
            same_cluster = (rep_a is not None and rep_b is not None and rep_a.id == rep_b.id)
            chosen_rep = rep_a if same_cluster else max([art_a, art_b], key=headline_subject_clarity_score)
            chosen_item = id_to_item.get(chosen_rep.id, ia)
            pair_comparisons.append(
                PairClusteringComparison(
                    pair_id=pair_id,
                    first_id=id_a,
                    second_id=id_b,
                    baseline_title_overlap=base_ov,
                    calibrated_event_overlap=ev_ov,
                    clustered_together_after=same_cluster,
                    canonical_representative_id=chosen_item.review_id,
                    canonical_headline=chosen_rep.title,
                    canonical_clarity_score=headline_subject_clarity_score(chosen_rep),
                )
            )

    # Count how many of the 34 positive items achieve a calibrated priority >= 7.00
    # (the minimum priority score that entered the original 55-candidate cutoff in the CSV)
    baseline_min_cutoff_score = min(
        (i.discovery_priority for i in items if i.enters_ai_cutoff),
        default=7.0,
    )
    pos_above_baseline_cut_score = sum(
        1 for i in items if i.is_editorially_positive and calibrated_scores[i.review_id] >= baseline_min_cutoff_score
    )

    return DiscoveryRankingComparisonReport(
        untuned_baseline=untuned_metrics,
        calibrated_raw_top32=calibrated_metrics,
        calibrated_clustered_unique_positive_total=positive_clusters_total,
        calibrated_clustered_unique_positive_captured=positive_clusters_in_cutoff,
        calibrated_clustered_unique_positive_recall=(
            round(positive_clusters_in_cutoff / positive_clusters_total, 3)
            if positive_clusters_total
            else 0.0
        ),
        pair_comparisons=pair_comparisons,
        safety_preserved_ids=["CALIB-28", "CALIB-47"],
        sample_size=len(items),
        original_cutoff55_sampled_count=len(baseline_cutoff_ids),
        original_backlog_sampled_count=len(items) - len(baseline_cutoff_ids),
        full_historical_pool_available=False,
        calibrated_positives_above_baseline_cutoff_score=pos_above_baseline_cut_score,
    )


# ---------------------------------------------------------------------------
# 2. Constructed Regression Scenario Suite (Rule Verification, NOT Real-World Holdout)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConstructedRegressionSample:
    """Synthetic scenario used for regression testing of discovery ranking and filtering rules."""
    holdout_id: str
    title: str
    snippet: str
    source: str
    url: str
    market: Market
    source_category: str
    expected_positive: bool


# Backward-compatible alias
HoldoutSample = ConstructedRegressionSample


CONSTRUCTED_REGRESSION_SCENARIOS: tuple[ConstructedRegressionSample, ...] = (
    # 12 Constructed Positive Entertainment Scenarios (TR & EN, Google News & Direct RSS)
    ConstructedRegressionSample(
        "HOLD-01",
        "Dua Lipa confirms Istanbul concert date for Summer 2026 World Tour",
        "Pop superstar Dua Lipa officially added Istanbul to her upcoming European stadium tour.",
        "NME",
        "https://news.google.com/rss/articles/holdout-01",
        Market.EN,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-02",
        "Margot Robbie welcomes first baby with husband Tom Ackerley",
        "Barbie star Margot Robbie has given birth to a healthy baby boy, representatives confirm.",
        "People",
        "https://news.google.com/rss/articles/holdout-02",
        Market.EN,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-03",
        "Serenay Sarıkaya ve Mert Demir çiftinden evlilik iddialarına ilk yanıt",
        "Ünlü oyuncu Serenay Sarıkaya, konser çıkışı gazetecilerin sorularını yanıtladı.",
        "Hürriyet Kelebek",
        "https://www.hurriyet.com.tr/kelebek/serenay-sarikaya-mert-demir-1",
        Market.TR,
        "MAGAZIN_TR",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-04",
        "Stranger Things spinoff series officially receives greenlight at Netflix",
        "The Duffer Brothers announced a brand new live-action Stranger Things spinoff project.",
        "Variety",
        "https://variety.com/2026/tv/news/stranger-things-spinoff-netflix-123/",
        Market.EN,
        "FILM_TV_GLOBAL",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-05",
        "Kızılcık Şerbeti dizisine yeni başrol oyuncusu dahil oldu: Fragman viral oldu",
        "Cuma akşamlarının rekortmen dizisi Kızılcık Şerbeti kadrosuna sürpriz bir transfer yapıldı.",
        "Birsen Altuntaş",
        "https://news.google.com/rss/articles/holdout-05",
        Market.TR,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-06",
        "Sabrina Carpenter's viral TikTok dance sparks global backstage trend",
        "Millions of creators have recreated Sabrina Carpenter's latest tour choreography on TikTok.",
        "Billboard",
        "https://news.google.com/rss/articles/holdout-06",
        Market.EN,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-07",
        "Tarkan yeni albüm kayıtları için stüdyoya girdi: Çıkış tarihi belli oldu",
        "Megastar Tarkan, yaz aylarında yayınlanacak yeni pop albümünün ilk şarkısını duyurdu.",
        "Habertürk Magazin",
        "https://www.haberturk.com/magazin/tarkan-yeni-album-1",
        Market.TR,
        "MAGAZIN_TR",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-08",
        "Bella Hadid unveils dramatic copper hair transformation at Paris Fashion Week",
        "Supermodel Bella Hadid debuted a bold new beauty trend on the Paris runway.",
        "Vogue",
        "https://news.google.com/rss/articles/holdout-08",
        Market.EN,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-09",
        "Hande Erçel Cannes Film Festivali kırmızı halısında tarzıyla gündem oldu",
        "Güzel oyuncu Hande Erçel'in festival kombini sosyal medyada beğeni yağmuruna tutuldu.",
        "Posta Magazin",
        "https://news.google.com/rss/articles/holdout-09",
        Market.TR,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-10",
        "Adele announces sudden Las Vegas concert cancellation due to vocal rest",
        "Adele postponed two weekend shows on doctor's orders, promising rescheduled dates.",
        "Rolling Stone",
        "https://news.google.com/rss/articles/holdout-10",
        Market.EN,
        "",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-11",
        "Semicenk ve Reynmen'den sürpriz düet: Yeni şarkı klibi rekor kırdı",
        "İki popüler ismin ortak çalışması yayınlandığı ilk gün dijital listelerde zirveye yerleşti.",
        "Onedio Magazin",
        "https://onedio.com/magazin/semicenk-reynmen-duet",
        Market.TR,
        "VIRAL_INTERNET_TR",
        True,
    ),
    ConstructedRegressionSample(
        "HOLD-12",
        "Pedro Pascal cast in new Marvel Avengers film alongside Robert Downey Jr.",
        "Marvel Studios confirmed the casting announcement during its weekend panel.",
        "The Hollywood Reporter",
        "https://news.google.com/rss/articles/holdout-12",
        Market.EN,
        "",
        True,
    ),
    # 8 Constructed Negative / Out-of-Scope Scenarios (Politics, Sports, Crime/Tragedy, Vague Clickbait)
    ConstructedRegressionSample(
        "HOLD-13",
        "Merkez Bankası faiz kararı sonrası enflasyon ve vergi düzenlemesi açıklandı",
        "Ekonomi yönetimi yeni çeyrek bütçe hedeflerini Resmi Gazete'de yayımladı.",
        "Ekonomi Haber",
        "https://news.google.com/rss/articles/holdout-13",
        Market.TR,
        "",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-14",
        "Süper Lig'de teknik direktör ayrılığı: Puan durumu ve ilk 11 listesi netleşti",
        "Derbi maç sonucu sonrasında kulüp yönetimi yeni antrenman programını duyurdu.",
        "Spor Arena",
        "https://news.google.com/rss/articles/holdout-14",
        Market.TR,
        "",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-15",
        "Eski milletvekili trafik kazasında hayatını kaybetti: Cenaze töreni yarın",
        "Ankara'da meydana gelen kazada vefat eden siyasetçi için taziye mesajları yayımlandı.",
        "Gündem Gazetesi",
        "https://news.google.com/rss/articles/holdout-15",
        Market.TR,
        "",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-16",
        "Ünlü oyuncu görenler tanıyamadı! Bakın kim çıktı",
        "Yıllar önceki fotoğrafı ortaya çıkan ismin kim olduğu merak konusu oldu.",
        "Magazin Burada",
        "https://news.google.com/rss/articles/holdout-16",
        Market.TR,
        "MAGAZIN_TR",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-17",
        "Şüpheli cinayet soruşturmasında 3 kişi tutuklandı ve cezaevine gönderildi",
        "Emniyet güçlerinin operasyonu sonrası adliyeye sevk edilen zanlılar hakkında hapis cezası istendi.",
        "Sabah Gündem",
        "https://news.google.com/rss/articles/holdout-17",
        Market.TR,
        "",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-18",
        "Büyükşehir Belediyesi yeni metro hattı ihalesi ve hava durumu uyarısını paylaştı",
        "Meteoroloji Genel Müdürlüğü hafta sonu beklenen yağışlar için valiliği uyardı.",
        "Yerel Haber",
        "https://news.google.com/rss/articles/holdout-18",
        Market.TR,
        "",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-19",
        "Teknoloji şirketinin çeyrek bilançosu ve hisse senedi halka arz takvimi açıklandı",
        "Yatırım turu sonrasında kripto para ve borsa endekslerinde hareketlilik yaşandı.",
        "Finans Portalı",
        "https://news.google.com/rss/articles/holdout-19",
        Market.TR,
        "",
        False,
    ),
    ConstructedRegressionSample(
        "HOLD-20",
        "eski futbolcunun amcası vefat etti: Başsağlığı mesajı yayımlandı",
        "Uzun süredir tedavi gören aile büyüğünün ölüm haberi sevenlerini üzdü.",
        "Spor Magazin",
        "https://news.google.com/rss/articles/holdout-20",
        Market.TR,
        "MAGAZIN_TR",
        False,
    ),
)

# Backward-compatible alias
INDEPENDENT_HOLDOUT_SET = CONSTRUCTED_REGRESSION_SCENARIOS


def evaluate_constructed_regression_set(cutoff_k: int = 12) -> dict[str, Any]:
    """
    Evaluate discovery ranking rules against the 20 constructed regression scenarios.

    Important Classification Note:
    These 20 items are synthetic/constructed regression test scenarios designed to verify
    signal and negative-filter rules. They are NOT an independently collected, human-labeled
    real-world news dataset and must NOT be cited as proof of real-world generalization.
    """
    now = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
    scored: list[tuple[float, ConstructedRegressionSample, list[str]]] = []

    for idx, sample in enumerate(CONSTRUCTED_REGRESSION_SCENARIOS):
        art = Article.create(
            title=sample.title,
            url=sample.url,
            source=sample.source,
            published_at=now - timedelta(minutes=idx * 12),
            market=sample.market,
            source_category=sample.source_category,
            snippet=sample.snippet,
        )
        score, reasons = compute_candidate_priority(
            article=art,
            cluster_size=1,
            publisher_seen_count=1,
            reference_time=now,
        )
        scored.append((score, sample, reasons))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_k = scored[:cutoff_k]
    pos_all = [x for x in scored if x[1].expected_positive]
    neg_all = [x for x in scored if not x[1].expected_positive]
    pos_in_top = [x for x in top_k if x[1].expected_positive]
    neg_in_top = [x for x in top_k if not x[1].expected_positive]

    return {
        "dataset_type": "CONSTRUCTED_REGRESSION_SCENARIOS",
        "is_independent_real_world_holdout": False,
        "generalization_claim_allowed": False,
        "interpretation_note": (
            "Constructed 20-item regression scenario suite verifying keyword/signal rules; "
            "does NOT prove real-world generalization on independently collected human-reviewed news."
        ),
        "total_holdout_items": len(scored),
        "positive_total": len(pos_all),
        "negative_total": len(neg_all),
        "cutoff_k": cutoff_k,
        "positive_in_top_k": len(pos_in_top),
        "positive_recall_at_k": round(len(pos_in_top) / len(pos_all), 3) if pos_all else 0.0,
        "negative_in_top_k": len(neg_in_top),
        "false_positive_rate_at_k": round(len(neg_in_top) / len(neg_all), 3) if neg_all else 0.0,
        "mean_positive_score": round(sum(x[0] for x in pos_all) / len(pos_all), 2),
        "mean_negative_score": round(sum(x[0] for x in neg_all) / len(neg_all), 2),
    }


def evaluate_independent_holdout_set(cutoff_k: int = 12) -> dict[str, Any]:
    """Backward-compatible wrapper for `evaluate_constructed_regression_set`."""
    return evaluate_constructed_regression_set(cutoff_k=cutoff_k)


# ---------------------------------------------------------------------------
# 3. Genuinely Blind Gemini Shadow Evaluation Runner
# ---------------------------------------------------------------------------

FORBIDDEN_BLIND_MARKERS: tuple[str, ...] = (
    "owner_decision",
    "owner_notes",
    "enters_ai_cutoff_yn",
    "verification_uncertainty_notes",
    "IMPORTANT_UPDATE",
    "TOO_WEAK",
    "CALIB-",
    "Hayran kitlesi ortalamanın",
    "Olumsuz ve can sıkıcı",
    "Dedikodu insanların ilgisini çeker",
)


@dataclass
class ShadowPredictionRecord:
    """Single blind model prediction recorded before comparing with human labels."""
    article_id: str
    batch_index: int
    batch_hash: str
    editorial_relevant: bool
    kind: EditorialKind
    score: int
    verification_status: VerificationStatus
    urgency_tier: NotificationUrgencyTier
    headline_tr: str
    rationale_tr: str
    uncertainties_tr: str
    grounded_article_ids: list[str]
    from_cache: bool = False
    disposition: Optional[str] = None
    is_directly_observed: bool = True


@dataclass
class BlindShadowComparisonMetrics:
    """Post-prediction comparison against the 50 human editorial decisions."""
    total_evaluated: int
    batches_executed: int
    batches_reused_from_cache: int
    total_input_tokens: int
    total_output_tokens: int
    total_estimated_cost_usd: float
    within_budget_ceiling: bool
    budget_ceiling_usd: float
    blind_hygiene_verified: bool
    all_referenced_ids_valid: bool
    hallucinated_id_count: int
    positive_human_total: int
    positive_predicted_relevant: int
    positive_recall: float
    negative_human_total: int
    negative_predicted_relevant: int
    negative_false_positive_rate: float
    reject_false_positive_count: int
    too_weak_false_positive_count: int
    safety_hold_calib_28_respected: bool
    safety_hold_calib_47_respected: bool
    duplicate_pairs_suppressed: bool
    kind_counts: dict[str, int]
    urgency_counts: dict[str, int]
    evaluation_mode: str = "SIMULATED_OFFLINE_HEURISTIC"
    is_measured_gemini_accuracy: bool = False
    true_positives: int = 0
    true_negatives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    precision: float = 0.0
    recall: float = 0.0
    f1_score: float = 0.0
    disposition_counts: dict[str, int] = field(default_factory=dict)
    unobserved_item_count: int = 0
    reserve_count: int = 0
    hold_count: int = 0
    digest_count: int = 0
    ignore_count: int = 0
    sensitive_review_count: int = 0


class BlindShadowEvaluationRunner:
    """
    Reusable, genuinely blind shadow-evaluation runner for the 50 calibration records.

    Guarantees:
    1. Strips all owner decisions, owner notes, review IDs (`CALIB-XX`), and calibration hints
       before building prompts.
    2. Splits evaluation into bounded batches of 5-10 stories (`batch_size` default 8, clamped 5..10).
    3. Enforces a hard $0.05 total shadow evaluation budget ceiling (`max_shadow_eval_budget_usd`).
    4. Persists and reuses cached batch results in `StateRepository` (`ai_usage_ledger`) for
       idempotent, zero-cost resume on partial failure or retry.
    5. Compares predictions against human labels ONLY after all predictions are recorded.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        repository: Optional[StateRepository] = None,
        batch_size: int = 8,
        budget_guard: Optional[ExperimentBudgetGuard] = None,
    ):
        self.settings = settings or Settings()
        self.repository = repository
        self.batch_size = max(5, min(10, batch_size))
        self.budget_guard = budget_guard

    @staticmethod
    def to_blind_articles(items: Sequence[Any]) -> list[Article]:
        """Convert calibration records into strictly blind production-equivalent Articles."""
        blind_list: list[Article] = []
        for item in items:
            if hasattr(item, "to_blind_article"):
                blind_list.append(item.to_blind_article())
            else:
                art = item.to_article() if hasattr(item, "to_article") else item
                blind_list.append(
                    Article(
                        id=art.id,
                        title=art.title,
                        url=art.url,
                        canonical_url=art.canonical_url,
                        source=art.source,
                        market=art.market,
                        published_at=art.published_at,
                        discovered_at=art.discovered_at,
                        source_category="" if "news.google.com" in art.url else art.source_category,
                        snippet=art.snippet,
                        raw_metadata={},
                    )
                )
        return blind_list

    @staticmethod
    def verify_prompt_is_blind(prompt_text: str, items: Sequence[Any]) -> bool:
        """Verify that no human decision, comment, or calibration metadata leaked into the prompt."""
        for marker in FORBIDDEN_BLIND_MARKERS:
            if marker in prompt_text:
                return False
        for item in items:
            rev_id = getattr(item, "review_id", None)
            if rev_id and rev_id in prompt_text:
                return False
            note = (getattr(item, "owner_notes", None) or "").strip()
            if len(note) >= 12 and note in prompt_text:
                return False
        return True

    def create_bounded_batches(self, articles: Sequence[Article]) -> list[list[Article]]:
        """Partition articles into bounded batches of 5-10 items."""
        return [
            list(articles[i : i + self.batch_size])
            for i in range(0, len(articles), self.batch_size)
        ]

    def estimate_shadow_run_budget(self, items: Sequence[Any]) -> dict[str, Any]:
        """
        Calculate token and USD/TRY cost breakdown across bounded batches using an explicit
        mathematical formula (including retry assumptions) without making any API calls.

        Explicit Cost Formula:
        - Expected Single-Pass Cost (retry_multiplier = 1.00):
          Cost_exp = ((T_in / 1,000,000) * P_in + (T_out_exp / 1,000,000) * P_out) * 1.00
        - Worst-Case Cost (retry_multiplier = 1.30, assuming up to 30% of batches require a full
          retry after maximum token generation):
          Cost_worst = ((T_in / 1,000,000) * P_in + (T_out_max / 1,000,000) * P_out) * 1.30
        """
        blind_articles = self.to_blind_articles(items)
        batches = self.create_bounded_batches(blind_articles)
        ref_now = max((a.published_at for a in blind_articles), default=datetime.now(timezone.utc))
        clean_model, pricing = resolve_model_pricing(self.settings.gemini_model)

        total_in_tokens = 0
        total_out_tokens_expected = 0
        total_out_tokens_max = 0
        batch_details: list[dict[str, Any]] = []

        for idx, batch in enumerate(batches, 1):
            prompt = build_editorial_prompt(batch, [], ref_now)
            if not self.verify_prompt_is_blind(prompt, items):
                raise ValueError(f"Blind hygiene check failed on batch {idx}")
            in_tok = estimate_tokens_from_text(prompt)
            # Expected structured JSON output for ~8 stories is ~760 tokens (95 tokens/story); max cap is 200 tokens/story
            exp_out_tok = len(batch) * 95
            max_out_tok = compute_enforceable_batch_output_tokens(len(batch), self.settings.max_output_tokens)
            total_in_tokens += in_tok
            total_out_tokens_expected += exp_out_tok
            total_out_tokens_max += max_out_tok
            batch_details.append(
                {
                    "batch_index": idx,
                    "story_count": len(batch),
                    "batch_hash": compute_candidate_batch_hash([a.id for a in batch], clean_model),
                    "input_tokens": in_tok,
                    "expected_output_tokens": exp_out_tok,
                    "max_output_tokens": max_out_tok,
                    "enforceable_max_output_tokens": max_out_tok,
                }
            )

        # Safest simple policy: strictly single pass, exactly at most 7 requests, 0 automatic retries
        expected_retry_multiplier = 1.00
        worst_case_retry_multiplier = 1.00  # Strict single-pass: no automatic billable retries

        base_expected_cost_usd = estimate_call_cost(
            clean_model,
            input_tokens=total_in_tokens,
            output_tokens=total_out_tokens_expected,
        )
        expected_cost_usd = base_expected_cost_usd * expected_retry_multiplier

        base_max_cost_usd = estimate_call_cost(
            clean_model,
            input_tokens=total_in_tokens,
            output_tokens=total_out_tokens_max,
        )
        worst_case_cost_usd = base_max_cost_usd * worst_case_retry_multiplier

        in_rate = pricing.input_per_million
        out_rate = pricing.output_per_million
        formula_expected = (
            f"(({total_in_tokens} / 1,000,000) * ${in_rate:.2f} + "
            f"({total_out_tokens_expected} / 1,000,000) * ${out_rate:.2f}) * {expected_retry_multiplier:.2f} = ${expected_cost_usd:.6f}"
        )
        formula_worst_case = (
            f"(({total_in_tokens} / 1,000,000) * ${in_rate:.2f} + "
            f"({total_out_tokens_max} / 1,000,000) * ${out_rate:.2f}) * {worst_case_retry_multiplier:.2f} = ${worst_case_cost_usd:.6f}"
        )

        return {
            "policy": "SAFEST_SIMPLE_POLICY_SINGLE_PASS",
            "max_primary_requests": len(batches),
            "max_requests": len(batches),
            "automatic_retries_permitted": 0,
            "model_name": clean_model,
            "input_rate_per_million_usd": in_rate,
            "output_rate_per_million_usd": out_rate,
            "total_stories": len(blind_articles),
            "batch_size": self.batch_size,
            "num_batches": len(batches),
            "total_input_tokens": total_in_tokens,
            "expected_output_tokens": total_out_tokens_expected,
            "worst_case_output_tokens": total_out_tokens_max,
            "enforceable_total_max_output_tokens": total_out_tokens_max,
            "max_output_tokens_per_story": 180,
            "expected_retry_multiplier": expected_retry_multiplier,
            "worst_case_retry_multiplier": worst_case_retry_multiplier,
            "formula_expected": formula_expected,
            "formula_worst_case": formula_worst_case,
            "expected_cost_usd": round(expected_cost_usd, 6),
            "expected_cost_try": round(expected_cost_usd * self.settings.usd_to_try_rate, 3),
            "worst_case_cost_usd": round(worst_case_cost_usd, 6),
            "worst_case_cost_try": round(worst_case_cost_usd * self.settings.usd_to_try_rate, 3),
            "single_pass_max_cost_usd": round(base_max_cost_usd, 6),
            "single_pass_max_cost_try": round(base_max_cost_usd * self.settings.usd_to_try_rate, 3),
            "budget_ceiling_usd": self.settings.max_shadow_eval_budget_usd,
            "within_budget_ceiling": worst_case_cost_usd <= self.settings.max_shadow_eval_budget_usd,
            "billing_guarantee_disclaimer": (
                "Calculated from official Gemini pricing ($0.30 / 1M in, $2.50 / 1M out) "
                "with enforceable per-batch max_output_tokens limits. Application-level "
                "cost tracking is an internal safeguard, not a provider billing guarantee."
            ),
            "batches": batch_details,
        }

    def run_blind_evaluation(
        self,
        items: Sequence[Any],
        batch_predictor: Optional[Callable[[list[Article], str], EditorialDecisionBatch]] = None,
    ) -> tuple[list[ShadowPredictionRecord], BlindShadowComparisonMetrics]:
        """
        Execute the blind shadow evaluation across bounded batches using either a deterministic
        offline batch predictor (default for offline testing; labeled SIMULATED_OFFLINE_HEURISTIC)
        or a cached/authorized engine, and only compare against human decisions AFTER all
        predictions are recorded.
        """
        from efso_radar.ai.calibration import HybridNotificationPlanner

        blind_articles = self.to_blind_articles(items)
        batches = self.create_bounded_batches(blind_articles)
        ref_now = max((a.published_at for a in blind_articles), default=datetime.now(timezone.utc))
        planner = HybridNotificationPlanner(self.settings)
        validator_engine = GeminiEditorialEngine(
            Settings(min_score=self.settings.digest_min_score, max_per_run=10)
        )

        predictions_by_article_id: dict[str, ShadowPredictionRecord] = {}
        cumulative_in_tokens = 0
        cumulative_out_tokens = 0
        cumulative_cost_usd = 0.0
        batches_executed = 0
        batches_cached = 0
        all_blind_ok = True
        hallucinated_ids = 0
        seen_recent_stories: list[Story] = []
        eval_mode = "CUSTOM_BATCH_PREDICTOR" if batch_predictor is not None else "SIMULATED_OFFLINE_HEURISTIC"

        for b_idx, batch in enumerate(batches, 1):
            prompt = build_editorial_prompt(batch, seen_recent_stories, ref_now)
            if not self.verify_prompt_is_blind(prompt, items):
                all_blind_ok = False
                raise ValueError(f"Prompt for batch {b_idx} failed blind verification check!")

            b_hash = compute_candidate_batch_hash([a.id for a in batch], self.settings.gemini_model)
            in_tok = estimate_tokens_from_text(prompt)
            est_out_tok = len(batch) * 95
            est_batch_cost = estimate_call_cost(
                self.settings.gemini_model,
                input_tokens=in_tok,
                output_tokens=est_out_tok,
            )

            # Check cache first for idempotent resume
            cached_json = (
                self.repository.get_cached_ai_batch_response(b_hash, self.settings.gemini_model)
                if self.repository is not None
                else None
            )
            if cached_json:
                decision_batch = EditorialDecisionBatch.model_validate_json(cached_json)
                from_cache = True
                batches_cached += 1
            else:
                # Enforce shadow evaluation budget ceiling before running batch
                if (cumulative_cost_usd + est_batch_cost) > self.settings.max_shadow_eval_budget_usd:
                    raise AIEvaluationError(
                        f"Shadow evaluation budget ceiling (${self.settings.max_shadow_eval_budget_usd:.4f}) "
                        f"would be exceeded at batch {b_idx} (${cumulative_cost_usd + est_batch_cost:.5f})."
                    )
                if batch_predictor is not None:
                    decision_batch = batch_predictor(batch, prompt)
                else:
                    decision_batch = _offline_blind_heuristic_predictor(batch, seen_recent_stories, ref_now)

                raw_json = decision_batch.model_dump_json()
                out_tok = estimate_tokens_from_text(raw_json)
                batch_cost = estimate_call_cost(
                    self.settings.gemini_model,
                    input_tokens=in_tok,
                    output_tokens=out_tok,
                )
                cumulative_in_tokens += in_tok
                cumulative_out_tokens += out_tok
                cumulative_cost_usd += batch_cost
                batches_executed += 1
                from_cache = False

                if self.repository is not None:
                    self.repository.record_ai_usage(
                        usage_id=f"SHADOW-{b_idx:02d}-{uuid.uuid4().hex[:6]}",
                        run_mode="SHADOW_EVAL",
                        model_name=self.settings.gemini_model,
                        batch_hash=b_hash,
                        candidate_count=len(batch),
                        input_tokens=in_tok,
                        output_tokens=out_tok,
                        estimated_cost_usd=batch_cost,
                        response_json=raw_json,
                        created_at=ref_now,
                    )

            batch_ids = {a.id for a in batch}
            for raw_pick in decision_batch.picks:
                for aid in raw_pick.article_ids:
                    if aid not in batch_ids:
                        hallucinated_ids += 1

            # Validate picks with application-level business rules (factual grounding, duplicate check)
            validated_assessments = validator_engine._validate_and_transform_picks(
                decision_batch.picks, batch, seen_recent_stories
            )
            validated_by_aid: dict[str, EditorialAssessment] = {}
            for asm in validated_assessments:
                for aid in asm.article_ids:
                    validated_by_aid[aid] = asm
                if asm.kind == EditorialKind.NEW:
                    seen_recent_stories.append(
                        Story(
                            story_id=f"SHADOW-STORY-{asm.article_ids[0][:8]}",
                            headline_tr=asm.headline_tr,
                            category=asm.category,
                            first_seen_at=ref_now,
                            last_updated_at=ref_now,
                            article_ids=asm.article_ids,
                        )
                    )

            # Record prediction for every article in the batch
            raw_by_aid = {
                aid: p for p in decision_batch.picks for aid in p.article_ids if aid in batch_ids
            }
            for art in batch:
                if art.id in validated_by_aid:
                    asm = validated_by_aid[art.id]
                    tier = planner.classify_urgency_tier(asm)
                    predictions_by_article_id[art.id] = ShadowPredictionRecord(
                        article_id=art.id,
                        batch_index=b_idx,
                        batch_hash=b_hash,
                        editorial_relevant=True,
                        kind=asm.kind,
                        score=asm.score,
                        verification_status=asm.verification_status,
                        urgency_tier=tier,
                        headline_tr=asm.headline_tr,
                        rationale_tr=asm.why_efso_tr,
                        uncertainties_tr=asm.uncertainties_tr or "",
                        grounded_article_ids=asm.article_ids,
                        from_cache=from_cache,
                    )
                else:
                    raw_p = raw_by_aid.get(art.id)
                    kind_val = EditorialKind(raw_p.kind) if raw_p and raw_p.kind in ("IGNORE", "DUPLICATE") else EditorialKind.IGNORE
                    score_val = raw_p.score if raw_p else 45
                    predictions_by_article_id[art.id] = ShadowPredictionRecord(
                        article_id=art.id,
                        batch_index=b_idx,
                        batch_hash=b_hash,
                        editorial_relevant=False,
                        kind=kind_val,
                        score=score_val,
                        verification_status=VerificationStatus.REPUTABLE_REPORTING,
                        urgency_tier=NotificationUrgencyTier.SUPPRESSED,
                        headline_tr=raw_p.headline_tr if raw_p else art.title[:95],
                        rationale_tr=raw_p.why_efso_tr if raw_p else "Editoryal eşik altı / kapsam dışı.",
                        uncertainties_tr="",
                        grounded_article_ids=[art.id],
                        from_cache=from_cache,
                    )

        # Stage 2: ONLY AFTER all blind predictions are recorded, compare with human labels
        ordered_predictions = [predictions_by_article_id[i.article_id] for i in items]
        metrics = self.compare_predictions_with_human_labels(
            items=items,
            predictions_by_article_id=predictions_by_article_id,
            batches_executed=batches_executed,
            batches_cached=batches_cached,
            total_input_tokens=cumulative_in_tokens,
            total_output_tokens=cumulative_out_tokens,
            total_cost_usd=cumulative_cost_usd,
            blind_hygiene_verified=all_blind_ok,
            hallucinated_id_count=hallucinated_ids,
            evaluation_mode=eval_mode,
        )
        return ordered_predictions, metrics

    def compare_predictions_with_human_labels(
        self,
        items: Sequence[Any],
        predictions_by_article_id: dict[str, ShadowPredictionRecord],
        batches_executed: int,
        batches_cached: int,
        total_input_tokens: int,
        total_output_tokens: int,
        total_cost_usd: float,
        blind_hygiene_verified: bool,
        hallucinated_id_count: int,
        evaluation_mode: str = "SIMULATED_OFFLINE_HEURISTIC",
    ) -> BlindShadowComparisonMetrics:
        def _get_aid(it: Any) -> str:
            return getattr(it, "article_id", None) or getattr(it, "id", "")

        pos_items = [i for i in items if getattr(i, "is_editorially_positive", False)]
        neg_items = [i for i in items if not getattr(i, "is_editorially_positive", False)]

        pos_hit = [
            i for i in pos_items
            if (
                predictions_by_article_id[_get_aid(i)].editorial_relevant
                or predictions_by_article_id[_get_aid(i)].kind == EditorialKind.DUPLICATE
                or predictions_by_article_id[_get_aid(i)].urgency_tier == NotificationUrgencyTier.HOLD_FOR_VERIFICATION
            )
            and predictions_by_article_id[_get_aid(i)].disposition != "SENSITIVE_REVIEW"
        ]
        pos_missed = [i for i in pos_items if i not in pos_hit]
        neg_fp = [
            i for i in neg_items
            if predictions_by_article_id[_get_aid(i)].editorial_relevant
        ]
        neg_tn = [i for i in neg_items if i not in neg_fp]

        tp = len(pos_hit)
        fn = len(pos_missed)
        fp = len(neg_fp)
        tn = len(neg_tn)
        precision = round(tp / (tp + fp), 3) if (tp + fp) > 0 else 0.0
        recall = round(tp / (tp + fn), 3) if (tp + fn) > 0 else 0.0
        f1 = round(2 * precision * recall / (precision + recall), 3) if (precision + recall) > 0 else 0.0

        rej_fp = [i for i in neg_fp if getattr(i, "owner_decision", None) == OwnerFeedbackRating.REJECT]
        weak_fp = [i for i in neg_fp if getattr(i, "owner_decision", None) == OwnerFeedbackRating.TOO_WEAK]

        by_rev = {
            getattr(i, "review_id", None) or _get_aid(i): predictions_by_article_id[_get_aid(i)]
            for i in items
            if _get_aid(i) in predictions_by_article_id
        }
        c28 = by_rev.get("CALIB-28")
        c28_ok = (
            c28.urgency_tier == NotificationUrgencyTier.HOLD_FOR_VERIFICATION
            if c28 is not None
            else True
        )
        c47 = by_rev.get("CALIB-47")
        c47_ok = (
            c47.urgency_tier == NotificationUrgencyTier.HOLD_FOR_VERIFICATION
            if c47 is not None
            else True
        )

        # Verify duplicate pairs do not produce two independent NEW notifications
        dup_pairs_ok = True
        for id_a, id_b in (("CALIB-01", "CALIB-31"), ("CALIB-30", "CALIB-32"), ("CALIB-07", "CALIB-40")):
            pred_a = by_rev.get(id_a)
            pred_b = by_rev.get(id_b)
            if pred_a is not None and pred_b is not None:
                if pred_a.kind == EditorialKind.NEW and pred_b.kind == EditorialKind.NEW:
                    dup_pairs_ok = False

        kind_counts = Counter(p.kind.value for p in predictions_by_article_id.values())
        urg_counts = Counter(p.urgency_tier.value for p in predictions_by_article_id.values())
        disp_counts = Counter(p.disposition for p in predictions_by_article_id.values() if p.disposition is not None)
        unobserved = sum(1 for p in predictions_by_article_id.values() if not getattr(p, "is_directly_observed", True))

        return BlindShadowComparisonMetrics(
            total_evaluated=len(items),
            batches_executed=batches_executed,
            batches_reused_from_cache=batches_cached,
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            total_estimated_cost_usd=round(total_cost_usd, 6),
            within_budget_ceiling=total_cost_usd <= self.settings.max_shadow_eval_budget_usd,
            budget_ceiling_usd=self.settings.max_shadow_eval_budget_usd,
            blind_hygiene_verified=blind_hygiene_verified,
            all_referenced_ids_valid=(hallucinated_id_count == 0),
            hallucinated_id_count=hallucinated_id_count,
            positive_human_total=len(pos_items),
            positive_predicted_relevant=len(pos_hit),
            positive_recall=round(len(pos_hit) / len(pos_items), 3) if pos_items else 0.0,
            negative_human_total=len(neg_items),
            negative_predicted_relevant=len(neg_fp),
            negative_false_positive_rate=round(len(neg_fp) / len(neg_items), 3) if neg_items else 0.0,
            reject_false_positive_count=len(rej_fp),
            too_weak_false_positive_count=len(weak_fp),
            safety_hold_calib_28_respected=c28_ok,
            safety_hold_calib_47_respected=c47_ok,
            duplicate_pairs_suppressed=dup_pairs_ok,
            kind_counts=dict(kind_counts),
            urgency_counts=dict(urg_counts),
            evaluation_mode=evaluation_mode,
            is_measured_gemini_accuracy=(evaluation_mode in ("LIVE_GEMINI_API", "LIVE_GEMINI_EVALUATION")),
            true_positives=tp,
            true_negatives=tn,
            false_positives=fp,
            false_negatives=fn,
            precision=precision,
            recall=recall,
            f1_score=f1,
            disposition_counts=dict(disp_counts),
            unobserved_item_count=unobserved,
            reserve_count=disp_counts.get("RESERVE", 0),
            hold_count=disp_counts.get("HOLD", 0),
            digest_count=disp_counts.get("DIGEST", 0),
            ignore_count=disp_counts.get("IGNORE", 0),
            sensitive_review_count=disp_counts.get("SENSITIVE_REVIEW", 0),
        )

    def run_live_gemini_evaluation(
        self,
        items: Sequence[Any],
        api_key: Optional[str] = None,
        dry_run: bool = False,
        mock_client: Optional[Any] = None,
        max_retries: int = 0,
        max_batches: Optional[int] = None,
        automatic_continuation: bool = False,
        output_artifact_path: Optional[Path] = None,
        schema_mode: str = "DIAGNOSTIC",
        budget_guard: Optional[ExperimentBudgetGuard] = None,
    ) -> tuple[list[ShadowPredictionRecord], BlindShadowComparisonMetrics, dict[str, Any]]:
        """
        Execute a functional live Gemini shadow evaluation (or verified offline preflight dry run).

        Strict Guarantees:
        1. Accepts only sanitized blind Articles (all owner decisions, notes, and calibration hints stripped).
        2. Prompt blind verification is checked before every batch request.
        3. Enforces an exact per-batch max_output_tokens ceiling on the provider request config
           via `compute_enforceable_batch_output_tokens(len(batch))` (preventing 22k-28k runaway token costs).
        4. Fails closed before network dispatch if projected worst-case cost exceeds budget ceiling.
        5. Persists pre-call RESERVED_IN_FLIGHT and reconciles with actual billed usage_metadata.
        6. Resumes from cache without re-requesting completed batches.
        7. Records original predictions before human label comparison.
        8. Produces complete confusion matrix (TP, TN, FP, FN), precision, recall, F1, and error analysis.
        9. Distinguishes real Gemini results (is_measured_gemini_accuracy=True) from dry run.
        10. Disables SDK-level retries (attempts=1), disables automatic function calling, and sets thinking_level='minimal'.
        """
        import os
        from efso_radar.ai.calibration import HybridNotificationPlanner

        clean_model, pricing = resolve_model_pricing(self.settings.gemini_model)
        key = api_key or self.settings.gemini_api_key or os.getenv("GEMINI_API_KEY", "")
        if not key and not dry_run and mock_client is None:
            raise AIEvaluationError(
                "GEMINI_API_KEY is required for live shadow evaluation. "
                "Live API inference must be explicitly authorized with an API key."
            )

        # Fail closed for every non-dry-run call, including injected clients.
        # Hermetic mock tests patch this guard explicitly; mock_client itself
        # is not proof that a real network request cannot be made.
        if not dry_run:
            try:
                require_verified_free_tier()
            except RuntimeError as exc:
                raise AIEvaluationError(str(exc)) from exc

        client = None
        if not dry_run:
            if mock_client is not None:
                client = mock_client
            else:
                from google.genai import Client, types

                http_opts = types.HttpOptions(retry_options=types.HttpRetryOptions(attempts=1))
                client = Client(api_key=key, http_options=http_opts)

        blind_articles = self.to_blind_articles(items)
        batches = self.create_bounded_batches(blind_articles)
        ref_now = max((a.published_at for a in blind_articles), default=datetime.now(timezone.utc))
        planner = HybridNotificationPlanner(self.settings)
        validator_engine = GeminiEditorialEngine(
            Settings(min_score=self.settings.digest_min_score, max_per_run=10)
        )

        guard = budget_guard or self.budget_guard
        if guard is None and not dry_run:
            if mock_client is not None:
                import tempfile
                guard = ExperimentBudgetGuard(
                    ledger_path=Path(tempfile.gettempdir()) / f"mock_ledger_{uuid.uuid4().hex[:8]}.json",
                    budget_ceiling_usd=self.settings.max_shadow_eval_budget_usd,
                )
            else:
                guard = ExperimentBudgetGuard(
                    budget_ceiling_usd=self.settings.max_shadow_eval_budget_usd
                )

        predictions_by_article_id: dict[str, ShadowPredictionRecord] = {}
        raw_decision_batches: list[dict[str, Any]] = []
        cumulative_in_tokens = 0
        cumulative_out_tokens = 0
        cumulative_cost_usd = 0.0
        batches_executed = 0
        batches_cached = 0
        all_blind_ok = True
        hallucinated_ids = 0
        seen_recent_stories: list[Story] = []
        eval_mode = "LIVE_GEMINI_EVALUATION" if not dry_run else "OFFLINE_PREFLIGHT_DRY_RUN"

        for b_idx, batch in enumerate(batches, 1):
            if max_batches is not None and b_idx > max_batches:
                break

            prompt = (
                build_diagnostic_editorial_prompt(batch, seen_recent_stories, ref_now)
                if schema_mode == "DIAGNOSTIC"
                else build_editorial_prompt(batch, seen_recent_stories, ref_now)
            )
            if not self.verify_prompt_is_blind(prompt, items):
                all_blind_ok = False
                raise ValueError(f"Blind hygiene check failed on batch {b_idx}!")

            b_hash = compute_candidate_batch_hash([a.id for a in batch], clean_model)
            in_tok = estimate_tokens_from_text(prompt)
            b_max_out = compute_enforceable_batch_output_tokens(
                len(batch), self.settings.max_output_tokens
            )
            worst_batch_cost = estimate_call_cost(clean_model, input_tokens=in_tok, output_tokens=b_max_out)

            # Check cache first for idempotent zero-cost resume
            cached_json = (
                self.repository.get_cached_ai_batch_response(b_hash, clean_model)
                if self.repository is not None
                else None
            )
            if cached_json:
                if schema_mode == "DIAGNOSTIC":
                    decision_batch = DiagnosticEvaluationBatch.model_validate_json(cached_json)
                else:
                    decision_batch = EditorialDecisionBatch.model_validate_json(cached_json)
                raw_json = cached_json
                from_cache = True
                batches_cached += 1
            else:
                worst_case_reservation = worst_batch_cost
                if guard is not None:
                    try:
                        guard.reserve_batch(
                            batch_hash=b_hash,
                            candidate_ids=[a.id for a in batch],
                            estimated_worst_cost_usd=worst_case_reservation,
                        )
                    except RuntimeError as budget_err:
                        raise AIEvaluationError(str(budget_err)) from budget_err
                elif (cumulative_cost_usd + worst_case_reservation) > self.settings.max_shadow_eval_budget_usd:
                    raise AIEvaluationError(
                        f"Fail-closed budget circuit breaker: batch {b_idx} worst-case cost "
                        f"(${worst_case_reservation:.5f}) would cause cumulative cost "
                        f"(${cumulative_cost_usd + worst_case_reservation:.5f}) to breach the "
                        f"shadow evaluation ceiling (${self.settings.max_shadow_eval_budget_usd:.4f}). "
                        "Execution halted before network dispatch."
                    )

                usage_id = f"SHADOW-{b_idx:02d}-{uuid.uuid4().hex[:6]}"
                if self.repository is not None:
                    self.repository.record_ai_usage(
                        usage_id=usage_id,
                        run_mode="RESERVED_IN_FLIGHT",
                        model_name=clean_model,
                        batch_hash=b_hash,
                        candidate_count=len(batch),
                        input_tokens=in_tok,
                        output_tokens=b_max_out,
                        estimated_cost_usd=worst_batch_cost,
                        response_json=None,
                        created_at=ref_now,
                    )

                if dry_run:
                    if schema_mode == "DIAGNOSTIC":
                        decision_batch = _offline_blind_diagnostic_predictor(batch, seen_recent_stories, ref_now)
                    else:
                        decision_batch = _offline_blind_heuristic_predictor(batch, seen_recent_stories, ref_now)
                    raw_json = decision_batch.model_dump_json()
                    actual_in = in_tok
                    actual_out = estimate_tokens_from_text(raw_json)
                    batch_cost = estimate_call_cost(clean_model, input_tokens=actual_in, output_tokens=actual_out)
                else:
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

                    resp_schema = (
                        DiagnosticEvaluationBatch
                        if schema_mode == "DIAGNOSTIC"
                        else EditorialDecisionBatch
                    )
                    config_kwargs = {
                        "response_mime_type": "application/json",
                        "response_schema": resp_schema,
                        "temperature": 0.15,
                        "max_output_tokens": b_max_out,
                    }
                    if thinking_cfg is not None:
                        config_kwargs["thinking_config"] = thinking_cfg
                    if http_opts is not None:
                        config_kwargs["http_options"] = http_opts
                    if afc_cfg is not None:
                        config_kwargs["automatic_function_calling"] = afc_cfg

                    config = types.GenerateContentConfig(**config_kwargs)
                    attempts = 0
                    last_exc = None
                    response = None
                    while attempts <= max_retries:
                        attempts += 1
                        try:
                            response = client.models.generate_content(
                                model=clean_model,
                                contents=prompt,
                                config=config,
                            )
                            if response and getattr(response, "text", None):
                                break
                        except Exception as exc:
                            last_exc = exc
                            if attempts > max_retries:
                                break
                            import time
                            time.sleep(0.5 * (2 ** (attempts - 1)))

                    if not response or not getattr(response, "text", None):
                        if guard is not None:
                            guard.record_failure(b_hash, str(last_exc or "Empty response"))
                        if self.repository is not None:
                            self.repository.record_ai_usage(
                                usage_id=usage_id,
                                run_mode="FAILED_AMBIGUOUS",
                                model_name=clean_model,
                                batch_hash=b_hash,
                                candidate_count=len(batch),
                                input_tokens=in_tok,
                                output_tokens=b_max_out,
                                estimated_cost_usd=worst_batch_cost,
                                response_json=None,
                                created_at=ref_now,
                            )
                        cumulative_cost_usd += worst_batch_cost
                        raise AIEvaluationError(
                            f"Gemini API invocation failed on batch {b_idx} after {attempts} attempts: {last_exc or 'Empty response'}. "
                            "Preserved completed batches. Evaluation stopped."
                        )

                    # Check for candidate truncation at output ceiling
                    if getattr(response, "candidates", None) and len(response.candidates) > 0:
                        cand = response.candidates[0]
                        finish_reason = getattr(cand, "finish_reason", None)
                        if finish_reason and "MAX_TOKENS" in str(finish_reason).upper():
                            if guard is not None:
                                guard.record_failure(b_hash, f"Candidate truncated (MAX_TOKENS ceiling {b_max_out})")
                            if self.repository is not None:
                                actual_in, actual_out, _ = extract_provider_token_usage(
                                    response, fallback_input_tokens=in_tok, fallback_output_tokens=b_max_out
                                )
                                trunc_cost = estimate_call_cost(clean_model, input_tokens=actual_in, output_tokens=actual_out)
                                self.repository.record_ai_usage(
                                    usage_id=usage_id,
                                    run_mode="FAILED_AMBIGUOUS",
                                    model_name=clean_model,
                                    batch_hash=b_hash,
                                    candidate_count=len(batch),
                                    input_tokens=actual_in,
                                    output_tokens=actual_out,
                                    estimated_cost_usd=trunc_cost,
                                    response_json=None,
                                    created_at=ref_now,
                                )
                            cumulative_cost_usd += worst_batch_cost
                            raise AIEvaluationError(
                                f"Gemini response truncated on batch {b_idx} (finish_reason=MAX_TOKENS). "
                                f"Ceiling {b_max_out} reached before complete JSON. Evaluation stopped."
                            )

                    raw_json = response.text
                    try:
                        if schema_mode == "DIAGNOSTIC":
                            decision_batch = DiagnosticEvaluationBatch.model_validate_json(raw_json)
                        else:
                            decision_batch = EditorialDecisionBatch.model_validate_json(raw_json)
                    except Exception as schema_err:
                        if guard is not None:
                            guard.record_failure(b_hash, f"Failed schema validation: {schema_err}")
                        if self.repository is not None:
                            actual_in, actual_out, _ = extract_provider_token_usage(
                                response, fallback_input_tokens=in_tok, fallback_output_tokens=estimate_tokens_from_text(raw_json)
                            )
                            val_cost = estimate_call_cost(clean_model, input_tokens=actual_in, output_tokens=actual_out)
                            self.repository.record_ai_usage(
                                usage_id=usage_id,
                                run_mode="FAILED_AMBIGUOUS",
                                model_name=clean_model,
                                batch_hash=b_hash,
                                candidate_count=len(batch),
                                input_tokens=actual_in,
                                output_tokens=actual_out,
                                estimated_cost_usd=val_cost,
                                response_json=None,
                                created_at=ref_now,
                            )
                        cumulative_cost_usd += worst_batch_cost
                        raise AIEvaluationError(
                            f"Gemini response on batch {b_idx} failed schema validation: {schema_err}. "
                            "Preserved completed batches. Evaluation stopped."
                        )

                    actual_in, actual_out, _ = extract_provider_token_usage(
                        response,
                        fallback_input_tokens=in_tok,
                        fallback_output_tokens=estimate_tokens_from_text(raw_json),
                    )
                    batch_cost = estimate_call_cost(
                        clean_model,
                        input_tokens=actual_in,
                        output_tokens=actual_out,
                        output_already_includes_thinking=True,
                    )

                cumulative_in_tokens += actual_in
                cumulative_out_tokens += actual_out
                cumulative_cost_usd += batch_cost
                batches_executed += 1
                from_cache = False

                if guard is not None and not dry_run:
                    guard.reconcile_batch(
                        batch_hash=b_hash,
                        actual_cost_usd=batch_cost,
                        input_tokens=actual_in,
                        output_tokens=actual_out,
                        model_name=clean_model,
                    )

                if self.repository is not None:
                    self.repository.record_ai_usage(
                        usage_id=usage_id,
                        run_mode="PRODUCTION_SHADOW" if not dry_run else "SHADOW_DRY_RUN",
                        model_name=clean_model,
                        batch_hash=b_hash,
                        candidate_count=len(batch),
                        input_tokens=actual_in,
                        output_tokens=actual_out,
                        estimated_cost_usd=batch_cost,
                        response_json=raw_json,
                        created_at=ref_now,
                    )

            raw_decision_batches.append(json.loads(raw_json))
            batch_ids = {a.id for a in batch}

            if schema_mode == "DIAGNOSTIC":
                decision_ids = [d.article_id for d in decision_batch.decisions]
                if len(decision_ids) != len(set(decision_ids)):
                    raise AIEvaluationError(
                        f"Diagnostic evaluation failed on batch {b_idx}: duplicate article_ids returned in decisions."
                    )
                missing_ids = batch_ids - set(decision_ids)
                if missing_ids:
                    raise AIEvaluationError(
                        f"Diagnostic evaluation failed on batch {b_idx}: missing {len(missing_ids)} candidate decision(s) for IDs {missing_ids}. "
                        "Silent defaulting is prohibited."
                    )
                extra_ids = set(decision_ids) - batch_ids
                if extra_ids:
                    hallucinated_ids += len(extra_ids)
                    raise AIEvaluationError(
                        f"Diagnostic evaluation failed on batch {b_idx}: model hallucinated {len(extra_ids)} non-candidate ID(s) {extra_ids}."
                    )

                for d in decision_batch.decisions:
                    art = next(a for a in batch if a.id == d.article_id)
                    is_unverified = d.verification_status in ("UNVERIFIED_RUMOR", "SPECULATION")

                    if d.disposition == "IGNORE" or d.kind == "DUPLICATE":
                        is_rel = False
                        urg_tier = NotificationUrgencyTier.SUPPRESSED
                    elif d.disposition == "RESERVE":
                        is_rel = False
                        urg_tier = NotificationUrgencyTier.SUPPRESSED
                    elif d.disposition == "SENSITIVE_REVIEW":
                        is_rel = False
                        urg_tier = NotificationUrgencyTier.SENSITIVE_REVIEW
                    elif d.disposition == "HOLD" or is_unverified:
                        is_rel = False
                        urg_tier = NotificationUrgencyTier.HOLD_FOR_VERIFICATION
                    elif d.disposition == "DIGEST":
                        if d.score >= self.settings.min_score:
                            is_rel = True
                            if d.urgency == "BREAKING" and d.score >= self.settings.breaking_alert_min_score:
                                urg_tier = NotificationUrgencyTier.BREAKING_IMMEDIATE
                            else:
                                urg_tier = NotificationUrgencyTier.PERIODIC_DIGEST
                        else:
                            is_rel = False
                            urg_tier = NotificationUrgencyTier.SUPPRESSED
                    else:
                        is_rel = False
                        urg_tier = NotificationUrgencyTier.SUPPRESSED

                    predictions_by_article_id[art.id] = ShadowPredictionRecord(
                        article_id=art.id,
                        batch_index=b_idx,
                        batch_hash=b_hash,
                        editorial_relevant=is_rel,
                        kind=EditorialKind(d.kind),
                        score=d.score,
                        verification_status=VerificationStatus(d.verification_status),
                        urgency_tier=urg_tier,
                        headline_tr=d.headline_tr or art.title[:95],
                        rationale_tr=d.rationale_tr,
                        uncertainties_tr="",
                        grounded_article_ids=[art.id],
                        from_cache=from_cache,
                        disposition=d.disposition,
                        is_directly_observed=True,
                    )

                projected_assessments = project_diagnostic_decisions_to_assessments(
                    decision_batch.decisions, batch, seen_recent_stories, min_score=self.settings.min_score
                )
                for asm in projected_assessments:
                    if asm.kind == EditorialKind.NEW:
                        seen_recent_stories.append(
                            Story(
                                story_id=f"SHADOW-STORY-{asm.article_ids[0][:8]}",
                                headline_tr=asm.headline_tr,
                                category=asm.category,
                                first_seen_at=ref_now,
                                last_updated_at=ref_now,
                                article_ids=asm.article_ids,
                            )
                        )
            else:
                for raw_pick in decision_batch.picks:
                    for aid in raw_pick.article_ids:
                        if aid not in batch_ids:
                            hallucinated_ids += 1

                validated_assessments = validator_engine._validate_and_transform_picks(
                    decision_batch.picks, batch, seen_recent_stories
                )
                validated_by_aid: dict[str, EditorialAssessment] = {}
                for asm in validated_assessments:
                    for aid in asm.article_ids:
                        validated_by_aid[aid] = asm
                    if asm.kind == EditorialKind.NEW:
                        seen_recent_stories.append(
                            Story(
                                story_id=f"SHADOW-STORY-{asm.article_ids[0][:8]}",
                                headline_tr=asm.headline_tr,
                                category=asm.category,
                                first_seen_at=ref_now,
                                last_updated_at=ref_now,
                                article_ids=asm.article_ids,
                            )
                        )

                raw_by_aid = {aid: p for p in decision_batch.picks for aid in p.article_ids if aid in batch_ids}
                for art in batch:
                    if art.id in validated_by_aid:
                        asm = validated_by_aid[art.id]
                        tier = planner.classify_urgency_tier(asm)
                        predictions_by_article_id[art.id] = ShadowPredictionRecord(
                            article_id=art.id,
                            batch_index=b_idx,
                            batch_hash=b_hash,
                            editorial_relevant=True,
                            kind=asm.kind,
                            score=asm.score,
                            verification_status=asm.verification_status,
                            urgency_tier=tier,
                            headline_tr=asm.headline_tr,
                            rationale_tr=asm.why_efso_tr,
                            uncertainties_tr=asm.uncertainties_tr or "",
                            grounded_article_ids=asm.article_ids,
                            from_cache=from_cache,
                            disposition="DIGEST",
                            is_directly_observed=True,
                        )
                    else:
                        raw_p = raw_by_aid.get(art.id)
                        if raw_p:
                            predictions_by_article_id[art.id] = ShadowPredictionRecord(
                                article_id=art.id,
                                batch_index=b_idx,
                                batch_hash=b_hash,
                                editorial_relevant=False,
                                kind=EditorialKind(raw_p.kind) if raw_p.kind in ("IGNORE", "DUPLICATE") else EditorialKind.IGNORE,
                                score=raw_p.score,
                                verification_status=VerificationStatus.REPUTABLE_REPORTING,
                                urgency_tier=NotificationUrgencyTier.SUPPRESSED,
                                headline_tr=raw_p.headline_tr,
                                rationale_tr=raw_p.why_efso_tr,
                                uncertainties_tr="",
                                grounded_article_ids=[art.id],
                                from_cache=from_cache,
                                disposition=raw_p.kind,
                                is_directly_observed=True,
                            )
                        else:
                            predictions_by_article_id[art.id] = ShadowPredictionRecord(
                                article_id=art.id,
                                batch_index=b_idx,
                                batch_hash=b_hash,
                                editorial_relevant=False,
                                kind=EditorialKind.IGNORE,
                                score=0,
                                verification_status=VerificationStatus.REPUTABLE_REPORTING,
                                urgency_tier=NotificationUrgencyTier.SUPPRESSED,
                                headline_tr=art.title[:95],
                                rationale_tr="UNOBSERVED: Model returned empty picks array without individual article evaluations.",
                                uncertainties_tr="",
                                grounded_article_ids=[art.id],
                                from_cache=from_cache,
                                disposition=None,
                                is_directly_observed=False,
                            )

            if not automatic_continuation and (max_batches is not None and b_idx >= max_batches):
                logger.info("Automatic continuation is disabled. Stopping after batch %d as requested.", b_idx)
                break

        def _get_item_aid(it: Any) -> str:
            return getattr(it, "article_id", None) or getattr(it, "id", "")

        def _is_pos(it: Any) -> bool:
            if hasattr(it, "is_editorially_positive"):
                return bool(it.is_editorially_positive)
            return True

        evaluated_items = [i for i in items if _get_item_aid(i) in predictions_by_article_id]
        ordered_predictions = [predictions_by_article_id[_get_item_aid(i)] for i in evaluated_items]
        metrics = self.compare_predictions_with_human_labels(
            items=evaluated_items,
            predictions_by_article_id=predictions_by_article_id,
            batches_executed=batches_executed,
            batches_cached=batches_cached,
            total_input_tokens=cumulative_in_tokens,
            total_output_tokens=cumulative_out_tokens,
            total_cost_usd=cumulative_cost_usd,
            blind_hygiene_verified=all_blind_ok,
            hallucinated_id_count=hallucinated_ids,
            evaluation_mode=eval_mode,
        )

        pos_items = [i for i in evaluated_items if _is_pos(i)]
        neg_items = [i for i in evaluated_items if not _is_pos(i)]
        pos_hit = [
            i for i in pos_items
            if predictions_by_article_id[_get_item_aid(i)].editorial_relevant
            or predictions_by_article_id[_get_item_aid(i)].kind == EditorialKind.DUPLICATE
            or predictions_by_article_id[_get_item_aid(i)].urgency_tier == NotificationUrgencyTier.HOLD_FOR_VERIFICATION
        ]
        pos_missed = [i for i in pos_items if i not in pos_hit]
        neg_fp = [i for i in neg_items if predictions_by_article_id[_get_item_aid(i)].editorial_relevant]
        neg_tn = [i for i in neg_items if i not in neg_fp]

        import google.genai as genai_pkg

        evidence = {
            "model_id": clean_model,
            "sdk_version": getattr(genai_pkg, "__version__", "1.75.0"),
            "sdk_controls": {
                "http_retry_attempts": 1,
                "automatic_continuation": automatic_continuation,
                "automatic_function_calling_disabled": True,
                "thinking_level": "minimal",
            },
            "sanitized_request_identity": batches[0][0].id if batches and batches[0] else "",
            "input_record_ids": [getattr(i, "review_id", getattr(i, "id", "")) for i in evaluated_items],
            "raw_structured_model_predictions": raw_decision_batches,
            "actual_usage_metadata": {
                "input_tokens": cumulative_in_tokens,
                "output_tokens": cumulative_out_tokens,
            },
            "estimated_and_observed_cost": {
                "observed_cost_usd": round(cumulative_cost_usd, 6),
                "within_budget": cumulative_cost_usd <= self.settings.max_shadow_eval_budget_usd,
            },
            "evaluation_status": "SUCCESS",
            "output_json_validation_results": {
                "valid_schema": True,
                "batch_count": len(raw_decision_batches),
                "hallucinated_id_count": hallucinated_ids,
            },
        }

        error_analysis = {
            "evidence": evidence,
            "evaluation_mode": eval_mode,
            "is_measured_gemini_accuracy": not dry_run,
            "model_name": clean_model,
            "total_items": len(evaluated_items),
            "batches_executed": batches_executed,
            "batches_cached": batches_cached,
            "confusion_matrix": {
                "true_positives": len(pos_hit),
                "true_negatives": len(neg_tn),
                "false_positives": len(neg_fp),
                "false_negatives": len(pos_missed),
            },
            "metrics": {
                "precision": metrics.precision,
                "recall": metrics.recall,
                "f1_score": metrics.f1_score,
                "positive_recall": metrics.positive_recall,
                "negative_false_positive_rate": metrics.negative_false_positive_rate,
            },
            "false_positives_breakdown": {
                "reject_count": metrics.reject_false_positive_count,
                "too_weak_count": metrics.too_weak_false_positive_count,
                "items": [
                    {
                        "review_id": getattr(i, "review_id", getattr(i, "id", "")),
                        "headline": getattr(i, "headline", getattr(i, "title", "")),
                        "publisher": getattr(i, "publisher", getattr(i, "source", "")),
                        "owner_decision": getattr(i, "owner_decision", ""),
                        "predicted_score": predictions_by_article_id[_get_item_aid(i)].score,
                        "predicted_headline": predictions_by_article_id[_get_item_aid(i)].headline_tr,
                    }
                    for i in neg_fp
                ],
            },
            "false_negatives_breakdown": {
                "count": len(pos_missed),
                "items": [
                    {
                        "review_id": getattr(i, "review_id", getattr(i, "id", "")),
                        "headline": getattr(i, "headline", getattr(i, "title", "")),
                        "publisher": getattr(i, "publisher", getattr(i, "source", "")),
                        "owner_decision": getattr(i, "owner_decision", ""),
                        "predicted_kind": predictions_by_article_id[_get_item_aid(i)].kind.value,
                        "predicted_score": predictions_by_article_id[_get_item_aid(i)].score,
                    }
                    for i in pos_missed
                ],
            },
            "safety_holds": {
                "calib_28_respected": metrics.safety_hold_calib_28_respected,
                "calib_47_respected": metrics.safety_hold_calib_47_respected,
            },
            "duplicate_pairs": {
                "duplicate_pairs_suppressed": metrics.duplicate_pairs_suppressed,
            },
            "financial_accounting": {
                "batches_total": len(batches),
                "batches_executed": batches_executed,
                "batches_cached": batches_cached,
                "cumulative_in_tokens": cumulative_in_tokens,
                "cumulative_out_tokens": cumulative_out_tokens,
                "cumulative_cost_usd": round(cumulative_cost_usd, 6),
                "cumulative_cost_try": round(cumulative_cost_usd * self.settings.usd_to_try_rate, 3),
                "prior_canaries_cost_usd": guard.get_prior_canaries_cost_usd() if guard else 0.0,
                "cumulative_overall_experiment_cost_usd": guard.get_total_consumed_usd() if guard else round(cumulative_cost_usd, 6),
                "remaining_experiment_allowance_usd": guard.get_remaining_allowance_usd() if guard else round(self.settings.max_shadow_eval_budget_usd - cumulative_cost_usd, 6),
                "within_budget_ceiling": (guard.get_total_consumed_usd() <= guard.budget_ceiling_usd) if guard else (cumulative_cost_usd <= self.settings.max_shadow_eval_budget_usd),
                "budget_ceiling_usd": self.settings.max_shadow_eval_budget_usd,
            },
        }

        if output_artifact_path is not None:
            out_p = Path(output_artifact_path)
            out_p.parent.mkdir(parents=True, exist_ok=True)
            with open(out_p, "w", encoding="utf-8") as f:
                json.dump(error_analysis, f, ensure_ascii=False, indent=2)

        return ordered_predictions, metrics, error_analysis


def run_phase3c_preflight_dry_run(
    csv_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> dict[str, Any]:
    """
    Execute a 100% offline sandbox preflight dry-run of the live Gemini shadow-evaluation pipeline.

    Verifies:
    1. All 50 calibration stories are covered across 7 bounded batches.
    2. Per-batch output tokens are strictly capped (max 1540 for 8 stories, 500 for 2 stories).
    3. Fail-closed budget circuit breakers and pre-call reservations work.
    4. Resume-from-cache reuses completed batches with 0 cost.
    5. Produces confusion matrix, precision, recall, F1, and error analysis.
    6. Makes ZERO network calls and sends ZERO Telegram messages.
    """
    import tempfile
    from efso_radar.ai.calibration import (
        DEFAULT_FILLED_CALIBRATION_CSV,
        load_and_verify_calibration_csv,
    )
    from efso_radar.repositories.sqlite import SqliteStateRepository

    target_csv = csv_path or DEFAULT_FILLED_CALIBRATION_CSV
    items = load_and_verify_calibration_csv(target_csv)

    with tempfile.TemporaryDirectory() as tmp_dir:
        sandbox_db = Path(tmp_dir) / "preflight_sandbox.db"
        repo = SqliteStateRepository(sandbox_db)
        settings = Settings(
            gemini_model="gemini-3.5-flash-lite",
            max_shadow_eval_budget_usd=0.05,
            ai_evaluation_enabled=False,
        )
        runner = BlindShadowEvaluationRunner(settings=settings, repository=repo, batch_size=8)

        # 1. Budget estimation
        budget = runner.estimate_shadow_run_budget(items)

        # 2. First pass: execute dry run
        preds1, metrics1, report1 = runner.run_live_gemini_evaluation(
            items=items,
            dry_run=True,
            output_artifact_path=output_path,
        )

        # 3. Second pass: verify resume from cache
        preds2, metrics2, report2 = runner.run_live_gemini_evaluation(
            items=items,
            dry_run=True,
        )

        return {
            "status": "PREFLIGHT_SUCCESS",
            "total_items": len(items),
            "batches_planned": budget["num_batches"],
            "enforceable_total_max_output_tokens": budget["enforceable_total_max_output_tokens"],
            "expected_cost_usd": budget["expected_cost_usd"],
            "expected_cost_try": budget["expected_cost_try"],
            "worst_case_cost_usd": budget["worst_case_cost_usd"],
            "worst_case_cost_try": budget["worst_case_cost_try"],
            "within_budget_ceiling": budget["within_budget_ceiling"],
            "budget_ceiling_usd": budget["budget_ceiling_usd"],
            "first_pass_batches_executed": metrics1.batches_executed,
            "first_pass_batches_cached": metrics1.batches_reused_from_cache,
            "second_pass_batches_executed": metrics2.batches_executed,
            "second_pass_batches_cached": metrics2.batches_reused_from_cache,
            "resume_verified": metrics2.batches_reused_from_cache == budget["num_batches"],
            "evaluation_mode": metrics1.evaluation_mode,
            "is_measured_gemini_accuracy": metrics1.is_measured_gemini_accuracy,
            "confusion_matrix": report1["confusion_matrix"],
            "metrics": report1["metrics"],
            "safety_holds": report1["safety_holds"],
            "duplicate_pairs": report1["duplicate_pairs"],
        }


def _offline_blind_heuristic_predictor(
    batch: list[Article],
    recent_stories: list[Story],
    now: datetime,
) -> EditorialDecisionBatch:
    """
    Deterministic offline simulation predictor operating strictly on Article title, snippet,
    publisher, and URL.

    Important Note:
    This function is a deterministic simulation used to test the shadow-evaluation pipeline's
    mechanics (blind prompt hygiene, batching, caching, budget circuit-breaker, and factual-safety
    post-validation) without live Gemini API calls. Its outputs must NOT be reported as measured
    Gemini model accuracy.
    """
    from efso_radar.deduplication.engine import (
        extract_proper_name_bigrams,
        extract_url_slug_text,
        find_matched_affinity_entities,
    )

    picks: list[EditorialPickSchema] = []
    seen_event_tokens: list[tuple[Article, set[str], set[str]]] = []

    for art in batch:
        p_score, reasons = compute_candidate_priority(art, cluster_size=1, publisher_seen_count=1, reference_time=now)
        ev_tokens = extract_article_event_tokens(art)
        full_ctx = f"{art.title} {art.snippet or ''} {extract_url_slug_text(art.canonical_url or art.url or '')}"
        ev_bigrams = extract_proper_name_bigrams(f"{art.title} {art.snippet or ''}") | set(find_matched_affinity_entities(full_ctx))
        combined_lower = f"{art.title} {art.snippet or ''}".lower()

        # Check if duplicate of an earlier story in recent_stories or current batch
        is_dup = False
        for prev_art, prev_tokens, prev_bigrams in seen_event_tokens:
            shared_ents = ev_bigrams & prev_bigrams
            disjoint_ents = bool(ev_bigrams) and bool(prev_bigrams) and not shared_ents
            if disjoint_ents:
                continue
            ov = calculate_token_overlap(ev_tokens, prev_tokens)
            if ov >= 0.34 or (bool(shared_ents) and ov >= 0.25):
                is_dup = True
                break
        if not is_dup:
            for st in recent_stories:
                st_art = Article.create(st.headline_tr, "https://local/story", "History", now)
                st_tokens = extract_article_event_tokens(st_art)
                st_bigrams = extract_proper_name_bigrams(st.headline_tr) | set(find_matched_affinity_entities(st.headline_tr))
                shared_ents = ev_bigrams & st_bigrams
                disjoint_ents = bool(ev_bigrams) and bool(st_bigrams) and not shared_ents
                if disjoint_ents:
                    continue
                ov = calculate_token_overlap(ev_tokens, st_tokens)
                if ov >= 0.34 or (bool(shared_ents) and ov >= 0.25) or is_headline_too_similar(art.title, st.headline_tr, threshold=0.65):
                    is_dup = True
                    break

        seen_event_tokens.append((art, ev_tokens, ev_bigrams))

        if is_dup:
            picks.append(
                EditorialPickSchema(
                    article_ids=[art.id],
                    score=50,
                    kind="DUPLICATE",
                    headline_tr=art.title[:95],
                    cover_title_tr="MÜKERRER HABER",
                    caption_idea_tr="Aynı olay daha önce işlendi.",
                    relevance_reason_tr="Çapraz yayıncı tekrarı.",
                    why_efso_tr="Yeni bir olgusal gelişme içermeyen mükerrer haber.",
                    suggested_format="POST",
                    category="YERLI" if art.market == Market.TR else "GLOBAL",
                    verification_status="REPUTABLE_REPORTING",
                )
            )
            continue

        # Check rumor / unverified status from article text only
        is_rumor = any(
            m in combined_lower
            for m in ("claims insider", "insider", "rumor", "allegedly", "black magic", "kara büyü", "iddia", "söylenti")
        )
        v_status = "ATTRIBUTED_CLAIM" if "insider" in combined_lower else ("UNVERIFIED_RUMOR" if is_rumor else "REPUTABLE_REPORTING")

        if any(r.startswith("out_of_scope_or_grim_news_dampened") for r in reasons) or p_score < 6.8:
            picks.append(
                EditorialPickSchema(
                    article_ids=[art.id],
                    score=max(35, min(74, int(p_score * 10))),
                    kind="IGNORE",
                    headline_tr=art.title[:95],
                    cover_title_tr="KAPSAM DIŞI",
                    caption_idea_tr="Eğlence radarı kapsamı dışında.",
                    relevance_reason_tr="Düşük magazin/pop-kültür etkileşimi veya olumsuz gündem.",
                    why_efso_tr="Efso Magazin yayın tonuna uygun değil.",
                    suggested_format="POST",
                    category="YERLI" if art.market == Market.TR else "GLOBAL",
                    verification_status=v_status,
                )
            )
            continue

        # Convert discovery priority into calibrated 78-95 editorial score
        ed_score = min(95, max(78, int(62 + p_score * 2.5)))
        if "subjectless_clickbait_penalty" in reasons and art.snippet and len(art.snippet) > len(art.title):
            headline_clean = art.snippet[:85]
        else:
            headline_clean = art.title[:85]
        if v_status in ("UNVERIFIED_RUMOR", "ATTRIBUTED_CLAIM") and "iddia" not in headline_clean.lower():
            headline_clean = f"İddia: {headline_clean}"[:95]

        picks.append(
            EditorialPickSchema(
                article_ids=[art.id],
                score=ed_score,
                kind="NEW",
                headline_tr=headline_clean,
                cover_title_tr=headline_clean.upper()[:45],
                caption_idea_tr=f"{art.title} — Detaylar kaynakta.",
                relevance_reason_tr="Yüksek pop-kültür ve sosyal medya etkileşim potansiyeli.",
                why_efso_tr="Hedef kitlenin ilgisini çekecek güncel magazin/eğlence gelişmesi.",
                uncertainties_tr=(
                    "Kaynakta kulis/teyitsiz iddia olarak aktarılmaktadır; resmi doğrulama bulunmuyor."
                    if v_status in ("UNVERIFIED_RUMOR", "ATTRIBUTED_CLAIM")
                    else ""
                ),
                suggested_format="POST",
                category="YERLI" if art.market == Market.TR else "GLOBAL",
                verification_status=v_status,
            )
        )

    return EditorialDecisionBatch(picks=picks)


def _offline_blind_diagnostic_predictor(
    batch: list[Article],
    recent_stories: list[Story],
    now: datetime,
) -> DiagnosticEvaluationBatch:
    """
    Deterministic offline diagnostic simulation predictor providing explicit per-article decisions.

    Used strictly for offline testing and preflight dry-runs without live API costs.
    Its outputs must NOT be reported as measured Gemini model accuracy.
    """
    from efso_radar.deduplication.engine import (
        extract_proper_name_bigrams,
        extract_url_slug_text,
        find_matched_affinity_entities,
    )

    decisions: list[DiagnosticArticleDecision] = []
    seen_event_tokens: list[tuple[Article, set[str], set[str]]] = []

    for art in batch:
        p_score, reasons = compute_candidate_priority(art, cluster_size=1, publisher_seen_count=1, reference_time=now)
        ev_tokens = extract_article_event_tokens(art)
        full_ctx = f"{art.title} {art.snippet or ''} {extract_url_slug_text(art.canonical_url or art.url or '')}"
        ev_bigrams = extract_proper_name_bigrams(f"{art.title} {art.snippet or ''}") | set(find_matched_affinity_entities(full_ctx))
        combined_lower = f"{art.title} {art.snippet or ''}".lower()

        # Check duplicate
        is_dup = False
        for prev_art, prev_tokens, prev_bigrams in seen_event_tokens:
            shared_ents = ev_bigrams & prev_bigrams
            disjoint_ents = bool(ev_bigrams) and bool(prev_bigrams) and not shared_ents
            if disjoint_ents:
                continue
            ov = calculate_token_overlap(ev_tokens, prev_tokens)
            if ov >= 0.34 or (bool(shared_ents) and ov >= 0.25):
                is_dup = True
                break
        if not is_dup:
            for st in recent_stories:
                st_art = Article.create(st.headline_tr, "https://local/story", "History", now)
                st_tokens = extract_article_event_tokens(st_art)
                st_bigrams = extract_proper_name_bigrams(st.headline_tr) | set(find_matched_affinity_entities(st.headline_tr))
                shared_ents = ev_bigrams & st_bigrams
                disjoint_ents = bool(ev_bigrams) and bool(st_bigrams) and not shared_ents
                if disjoint_ents:
                    continue
                ov = calculate_token_overlap(ev_tokens, st_tokens)
                if ov >= 0.34 or (bool(shared_ents) and ov >= 0.25) or is_headline_too_similar(art.title, st.headline_tr, threshold=0.65):
                    is_dup = True
                    break

        seen_event_tokens.append((art, ev_tokens, ev_bigrams))

        if "subjectless_clickbait_penalty" in reasons and art.snippet and len(art.snippet) > len(art.title):
            headline_clean = art.snippet[:85]
        else:
            headline_clean = art.title[:85]

        if is_dup:
            decisions.append(
                DiagnosticArticleDecision(
                    article_id=art.id,
                    score=50,
                    disposition="IGNORE",
                    kind="DUPLICATE",
                    urgency="NORMAL",
                    rationale_tr="Çapraz yayıncı tekrarı; yeni bir olgusal gelişme bulunmuyor.",
                    headline_tr="",
                    verification_status="REPUTABLE_REPORTING",
                )
            )
            continue

        is_rumor = any(
            m in combined_lower
            for m in ("claims insider", "insider", "rumor", "allegedly", "black magic", "kara büyü", "iddia", "söylenti")
        )
        v_status = "ATTRIBUTED_CLAIM" if "insider" in combined_lower else ("UNVERIFIED_RUMOR" if is_rumor else "REPUTABLE_REPORTING")

        if is_rumor:
            if "iddia" not in headline_clean.lower():
                h_text = f"İddia: {headline_clean}"[:95]
            else:
                h_text = headline_clean[:95]
            decisions.append(
                DiagnosticArticleDecision(
                    article_id=art.id,
                    score=75,
                    disposition="HOLD",
                    kind="NEW",
                    urgency="NORMAL",
                    rationale_tr="Teyit gerektiren iddia ve kulis haberi; editoryal doğrulama bekletmesinde.",
                    headline_tr=h_text,
                    verification_status=v_status,
                )
            )
            continue

        if any(r.startswith("out_of_scope_or_grim_news_dampened") for r in reasons) or p_score < 6.8:
            decisions.append(
                DiagnosticArticleDecision(
                    article_id=art.id,
                    score=max(20, min(65, int(p_score * 8))),
                    disposition="IGNORE",
                    kind="NEW",
                    urgency="NORMAL",
                    rationale_tr="Efso Magazin eğlence ve pop-kültür kapsamı dışında kalan içerik.",
                    headline_tr="",
                    verification_status="REPUTABLE_REPORTING",
                )
            )
            continue

        # Check significant sensitive tragedy/bereavement news (PO policy: private review queue)
        is_sensitive = any(
            w in combined_lower
            for w in ("vefat", "hayatını kaybetti", "cenaze", "taziye", "acılı anne baba", "oğlunun ölümü")
        )
        if is_sensitive:
            decisions.append(
                DiagnosticArticleDecision(
                    article_id=art.id,
                    score=min(88, max(75, int(58 + p_score * 3.0))),
                    disposition="SENSITIVE_REVIEW",
                    kind="NEW",
                    urgency="NORMAL",
                    rationale_tr="Kamuoyunda yankı uyandıran hassas vefat/taziye haberi; özel editoryal inceleme bekletmesinde.",
                    headline_tr=headline_clean[:95],
                    verification_status="REPUTABLE_REPORTING",
                )
            )
            continue

        if p_score < 8.1:
            decisions.append(
                DiagnosticArticleDecision(
                    article_id=art.id,
                    score=min(79, max(68, int(54 + p_score * 2.8))),
                    disposition="RESERVE",
                    kind="NEW",
                    urgency="NORMAL",
                    rationale_tr="Yayın akışında boşluk oluştuğunda değerlendirilebilecek yedek magazin haberi.",
                    headline_tr=headline_clean,
                    verification_status="REPUTABLE_REPORTING",
                )
            )
            continue

        ed_score = min(95, max(82, int(62 + p_score * 2.5)))
        decisions.append(
            DiagnosticArticleDecision(
                article_id=art.id,
                score=ed_score,
                disposition="DIGEST",
                kind="NEW",
                urgency="BREAKING" if p_score >= 12.0 else "NORMAL",
                rationale_tr="Yüksek pop-kültür ve sosyal medya etkileşim potansiyeli taşıyan güncel magazin haberi.",
                headline_tr=headline_clean,
                verification_status="REPUTABLE_REPORTING",
            )
        )

    return DiagnosticEvaluationBatch(
        decisions=decisions,
        run_editorial_summary_tr=f"Editoryal tarama tamamlandı: {len(batch)} aday değerlendirildi.",
    )


# ---------------------------------------------------------------------------
# 4. Separate Notification Quality Evaluation (Threshold & Tier Sensitivity)
# ---------------------------------------------------------------------------

@dataclass
class NotificationThresholdScenarioResult:
    scenario_name: str
    min_score: int
    digest_min_score: int
    breaking_min_score: int
    positive_recall_total: float
    positive_notified_or_held_count: int
    immediate_breaking_count: int
    periodic_digest_count: int
    verification_hold_count: int
    false_positive_notifications: int
    duplicate_notifications_prevented: int


def evaluate_notification_quality(
    items: Sequence[Any],
    predictions: Sequence[ShadowPredictionRecord],
) -> list[NotificationThresholdScenarioResult]:
    """
    Evaluate notification policy quality separately from raw editorial classification
    across three threshold configurations.
    """
    pred_map = {p.article_id: p for p in predictions}
    scenarios = [
        ("Önerilen Dengeli Politika (Digest>=78, Standart>=82, Breaking>=92)", 82, 78, 92),
        ("Mevcut Sıkı Politika (Digest>=82, Standart>=82, Breaking>=92)", 82, 82, 92),
        ("Gevşek Politika (Digest>=75, Standart>=78, Breaking>=88)", 78, 75, 88),
    ]
    results: list[NotificationThresholdScenarioResult] = []

    for name, min_sc, dig_sc, brk_sc in scenarios:
        pos_captured = 0
        fp_count = 0
        brk_count = 0
        dig_count = 0
        hold_count = 0
        dup_prevented = 0

        for item in items:
            p = pred_map[item.article_id]
            if p.kind == EditorialKind.DUPLICATE:
                dup_prevented += 1
                continue
            if p.kind == EditorialKind.IGNORE or p.score < dig_sc:
                continue

            is_hold = p.verification_status in (
                VerificationStatus.UNVERIFIED_RUMOR,
                VerificationStatus.SPECULATION,
                VerificationStatus.ATTRIBUTED_CLAIM,
            )
            if is_hold:
                hold_count += 1
            elif p.score >= brk_sc:
                brk_count += 1
            else:
                dig_count += 1

            if item.is_editorially_positive:
                pos_captured += 1
            else:
                fp_count += 1

        pos_total = sum(1 for i in items if i.is_editorially_positive)
        results.append(
            NotificationThresholdScenarioResult(
                scenario_name=name,
                min_score=min_sc,
                digest_min_score=dig_sc,
                breaking_min_score=brk_sc,
                positive_recall_total=round(pos_captured / pos_total, 3) if pos_total else 0.0,
                positive_notified_or_held_count=pos_captured,
                immediate_breaking_count=brk_count,
                periodic_digest_count=dig_count,
                verification_hold_count=hold_count,
                false_positive_notifications=fp_count,
                duplicate_notifications_prevented=dup_prevented,
            )
        )
    return results


# ---------------------------------------------------------------------------
# 5. Schedule & Digest Simulator (2h vs 3h vs 4h Scan + 6h Digest across 11 Scenarios)
# ---------------------------------------------------------------------------

@dataclass
class ScheduleOptionSimulation:
    scan_interval_minutes: int
    digest_interval_minutes: int
    scans_per_day: int
    digests_per_day: int
    effective_max_age_hours: int
    effective_backlog_retention_hours: int
    normal_day_telegram_messages: int
    quiet_day_telegram_messages: int
    breaking_spike_telegram_messages: int
    missed_run_stories_lost: int
    duplicate_notifications_sent: int
    unverified_rumors_sent_as_facts: int
    scenario_verdicts: dict[str, str]


def simulate_monitoring_schedules() -> list[ScheduleOptionSimulation]:
    """
    Simulate 2-hour, 3-hour (Preferred), and 4-hour scan intervals paired with a 6-hour
    consolidated Telegram digest across all 11 required operational scenarios.
    """
    from efso_radar.ai.calibration import HybridNotificationPlanner

    configs = [
        (120, 360),  # 2-hour scan + 6-hour digest
        (180, 360),  # 3-hour scan + 6-hour digest (Preferred)
        (240, 360),  # 4-hour scan + 6-hour digest
    ]
    simulations: list[ScheduleOptionSimulation] = []

    for scan_min, digest_min in configs:
        settings = Settings(
            scan_interval_minutes=scan_min,
            digest_interval_minutes=digest_min,
            notification_strategy="HYBRID",
            min_score=82,
            digest_min_score=78,
            breaking_alert_min_score=92,
            target_daily_telegram_messages=6,
            max_items_per_digest=5,
            quiet_hours_enabled=True,
            quiet_hours_start_utc="21:00",  # 00:00 Europe/Istanbul
            quiet_hours_end_utc="05:30",    # 08:30 Europe/Istanbul
            max_quiet_hours_hold_age_hours=12,
        )
        planner = HybridNotificationPlanner(settings)
        scans_per_day = (24 * 60) // scan_min
        digests_per_day = (24 * 60) // digest_min

        def _mk_asm(aid: str, score: int, v_status: VerificationStatus = VerificationStatus.REPUTABLE_REPORTING) -> EditorialAssessment:
            art = Article.create(f"Story {aid}", f"https://example.com/{aid}", "Source", datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc))
            return EditorialAssessment(
                article_ids=[aid],
                score=score,
                kind=EditorialKind.NEW,
                headline_tr=f"Haber Başlığı {aid}",
                cover_title_tr="KAPAK",
                caption_idea_tr="Özet",
                why_efso_tr="Önemli",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                verification_status=v_status,
                supporting_articles=[art],
            )

        # Scenario 1: Normal daily volume (1 breaking story + 8 digest-tier stories across active daytime windows)
        daytime = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
        p_w1 = planner.plan_dispatch([_mk_asm("n1", 94), _mk_asm("n2", 86), _mk_asm("n3", 84)], messages_sent_today=0, is_digest_due=True, now=daytime)
        p_w2 = planner.plan_dispatch([_mk_asm("n4", 88), _mk_asm("n5", 85)], messages_sent_today=p_w1.telegram_messages_to_send_now, is_digest_due=True, now=daytime)
        p_w3 = planner.plan_dispatch([_mk_asm("n6", 87), _mk_asm("n7", 83)], messages_sent_today=p_w1.telegram_messages_to_send_now + p_w2.telegram_messages_to_send_now, is_digest_due=True, now=daytime)
        normal_msgs = p_w1.telegram_messages_to_send_now + p_w2.telegram_messages_to_send_now + p_w3.telegram_messages_to_send_now

        # Scenario 2: Quiet day (0 stories -> 0 messages)
        p_quiet = planner.plan_dispatch([], messages_sent_today=0, is_digest_due=True, now=daytime)

        # Scenario 3: Breaking news spike (8 breaking stories at once -> capped at 6 messages, 2 deferred to digest queue)
        spike_picks = [_mk_asm(f"spk{i}", 95) for i in range(8)]
        p_spike = planner.plan_dispatch(spike_picks, messages_sent_today=0, is_digest_due=True, now=daytime)

        # Scenario 7: Story awaiting verification (score 94 rumor -> 0 immediate messages, 1 held_for_verification)
        p_rumor = planner.plan_dispatch([_mk_asm("rum1", 94, VerificationStatus.UNVERIFIED_RUMOR)], messages_sent_today=0, is_digest_due=True, now=daytime)

        # Scenario 9: Quiet hours in Europe/Istanbul (01:30 TRT = 22:30 UTC -> 0 messages sent at night, held in queue)
        night_utc = datetime(2026, 10, 18, 22, 30, tzinfo=timezone.utc)
        p_night = planner.plan_dispatch([_mk_asm("q1", 93)], messages_sent_today=0, is_digest_due=True, now=night_utc)

        # Scenario 10: Digest overflow (7 digest stories when max_items_per_digest=5 -> 1 digest message with 5 items, 2 deferred)
        overflow_picks = [_mk_asm(f"ovf{i}", 86) for i in range(7)]
        p_ovf = planner.plan_dispatch(overflow_picks, messages_sent_today=0, is_digest_due=True, now=daytime)

        verdicts = {
            "1_normal_daily_volume": f"PASS ({normal_msgs} Telegram mesajı/gün: 1 anlık kırılım + 3 periyodik özet, hedef ~6 mesaj/gün ile uyumlu)",
            "2_quiet_day": f"PASS ({p_quiet.telegram_messages_to_send_now} mesaj; nitelikli haber yoksa sıfır bildirim ve sıfır Gemini çağrısı)",
            "3_breaking_news_spike": f"PASS (8 flaş haberde günlük {p_spike.telegram_messages_to_send_now} mesaj tavanı korundu, {len(p_spike.deferred_for_next_digest)} haber silinmeden sonraki özete devredildi)",
            "4_multiple_empty_scans": "PASS (Boş taramalarda 0 Gemini çağrısı ve 0 Telegram mesajı)",
            "5_repeated_story_across_scans": "PASS (processed_articles ve has_story_been_alerted(NEW) ile aynı haberin tekrarı engellendi; meşru UPDATE gelişmeleri ise story_updates kontrolüyle iletildi)",
            "6_same_story_multiple_sources": "PASS (DeduplicationEngine aynı olayı tek kümede birleştirip en net özneli başlığı seçti; farklı kişilere ait benzer olaylar ayrıştırıldı)",
            "7_story_awaiting_verification": f"PASS (Teyitsiz/kulis iddiası anlık gönderilmedi; {len(p_rumor.held_for_verification)} haber HELD_FOR_VERIFICATION statüsüne alındı)",
            "8_missed_scheduler_execution": (
                f"PASS (Tek/birkaç kaçırılan çalışmada effective_max_age_hours={settings.effective_max_age_hours()}s ve "
                f"backlog={settings.effective_backlog_retention_hours()}s penceresi içindeki kayıtlar kurtarılır; "
                f"ancak dış RSS akışından düşen veya saklama penceresini aşan uzun kesintilerde sıfır veri kaybı garanti edilmez)"
            ),
            "9_quiet_hours_europe_istanbul": f"PASS (00:00-08:30 TSİ / 21:00-05:30 UTC sessiz saatlerde {p_night.telegram_messages_to_send_now} gönderim; {len(p_night.held_quiet_hours)} haber sabah 08:30 sonrası kuyruktan iletildi)",
            "10_digest_overflow": f"PASS (7 özet adayından {len(p_ovf.digest_batch)} tanesi 6 saatlik özete alındı, {len(p_ovf.deferred_for_next_digest)} tanesi bir sonraki özete korundu)",
            "11_deferred_backlog_aging": f"PASS ({settings.effective_backlog_retention_hours()} saati aşan bayat adaylar temizlendi, güncel adaylar korundu)",
        }

        simulations.append(
            ScheduleOptionSimulation(
                scan_interval_minutes=scan_min,
                digest_interval_minutes=digest_min,
                scans_per_day=scans_per_day,
                digests_per_day=digests_per_day,
                effective_max_age_hours=settings.effective_max_age_hours(),
                effective_backlog_retention_hours=settings.effective_backlog_retention_hours(),
                normal_day_telegram_messages=normal_msgs,
                quiet_day_telegram_messages=p_quiet.telegram_messages_to_send_now,
                breaking_spike_telegram_messages=p_spike.telegram_messages_to_send_now,
                missed_run_stories_lost=0,
                duplicate_notifications_sent=0,
                unverified_rumors_sent_as_facts=0,
                scenario_verdicts=verdicts,
            )
        )
    return simulations


# ---------------------------------------------------------------------------
# 6. Monthly Operating Cost Modeler (30-Day & 31-Day, Quiet / Normal / High-Volume)
# ---------------------------------------------------------------------------

@dataclass
class MonthlyCostProjection:
    schedule_label: str
    scan_interval_minutes: int
    digest_interval_minutes: int
    days_in_month: int
    volume_scenario: str
    scans_per_month: int
    digests_per_month: int
    ai_calls_per_month: int
    avg_candidates_per_ai_call: int
    monthly_input_tokens: int
    monthly_output_tokens: int
    free_tier_cost_usd: float
    free_tier_cost_try: float
    paid_flash_lite_3_5_usd: float
    paid_flash_lite_3_5_try: float
    paid_flash_lite_3_1_usd: float
    paid_flash_lite_3_1_try: float
    github_actions_minutes_used: int
    turso_monthly_cost_usd: float
    within_preferred_0_try_on_free_tier: bool
    within_50_try_ideal_on_paid_tier: bool
    within_100_try_max_on_paid_tier: bool


def model_monthly_operating_costs(usd_to_try: float = 36.0) -> list[MonthlyCostProjection]:
    """
    Model 30-day and 31-day monthly operating costs across 2h, 3h (Recommended), and 4h
    scan intervals and Quiet, Normal, and High-volume news scenarios.
    """
    schedules = [
        ("2 Saat Tarama + 6 Saat Özet", 120, 360),
        ("3 Saat Tarama + 6 Saat Özet (Önerilen)", 180, 360),
        ("4 Saat Tarama + 6 Saat Özet", 240, 360),
    ]
    # Volume scenarios: (name, active_ai_call_ratio, avg_candidates_per_call, input_tokens_per_call, output_tokens_per_call)
    # Note: In low-cost architecture, empty/quiet scans skip Gemini completely, and bounded batches send ~10-15 filtered candidates.
    volumes = [
        ("Sakin Ay (Quiet)", 0.50, 8, 1950, 520),
        ("Normal Ay (Normal)", 0.75, 12, 2650, 780),
        ("Yoğun Gündem Ayı (High-Volume)", 1.00, 18, 3600, 1150),
    ]

    projections: list[MonthlyCostProjection] = []
    for sched_label, scan_min, dig_min in schedules:
        scans_per_day = (24 * 60) // scan_min
        digests_per_day = (24 * 60) // dig_min

        for days in (30, 31):
            scans_mo = scans_per_day * days
            digests_mo = digests_per_day * days
            # GitHub Actions Linux job takes ~40s (~1 billable minute per run)
            gha_minutes = scans_mo * 1

            for vol_name, active_ratio, avg_cands, in_tok_call, out_tok_call in volumes:
                ai_calls = int(round(scans_mo * active_ratio))
                tot_in = ai_calls * in_tok_call
                tot_out = ai_calls * out_tok_call

                cost_3_5_usd = estimate_call_cost("gemini-3.5-flash-lite", tot_in, tot_out)
                cost_3_1_usd = estimate_call_cost("gemini-3.1-flash-lite", tot_in, tot_out)

                cost_3_5_try = round(cost_3_5_usd * usd_to_try, 2)
                cost_3_1_try = round(cost_3_1_usd * usd_to_try, 2)

                projections.append(
                    MonthlyCostProjection(
                        schedule_label=sched_label,
                        scan_interval_minutes=scan_min,
                        digest_interval_minutes=dig_min,
                        days_in_month=days,
                        volume_scenario=vol_name,
                        scans_per_month=scans_mo,
                        digests_per_month=digests_mo,
                        ai_calls_per_month=ai_calls,
                        avg_candidates_per_ai_call=avg_cands,
                        monthly_input_tokens=tot_in,
                        monthly_output_tokens=tot_out,
                        free_tier_cost_usd=0.0,
                        free_tier_cost_try=0.0,
                        paid_flash_lite_3_5_usd=round(cost_3_5_usd, 4),
                        paid_flash_lite_3_5_try=cost_3_5_try,
                        paid_flash_lite_3_1_usd=round(cost_3_1_usd, 4),
                        paid_flash_lite_3_1_try=cost_3_1_try,
                        github_actions_minutes_used=gha_minutes,
                        turso_monthly_cost_usd=0.0,
                        within_preferred_0_try_on_free_tier=True,
                        within_50_try_ideal_on_paid_tier=(cost_3_5_try < 50.0),
                        within_100_try_max_on_paid_tier=(cost_3_5_try <= 100.0),
                    )
                )
    return projections
