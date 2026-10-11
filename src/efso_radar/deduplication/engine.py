"""Deduplication and cross-publisher event clustering engine.

Provides multi-level filtering:
1. Exact ID & Canonical URL deduplication against processed history.
2. Cross-publisher title clustering to prevent flooding LLM with duplicates of the same event.
3. Priority scoring based on breaking pop-culture development keywords.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Sequence
from difflib import SequenceMatcher

from efso_radar.domain import Article, normalize_text
from efso_radar.repositories.base import StateRepository

# High-signal pop culture keywords indicating new developments
DEVELOPMENT_BOOST_KEYWORDS: set[str] = {
    "açıkladı", "resmen", "yeni", "evlendi", "ayrıldı", "konser", "albüm",
    "premiere", "announces", "announced", "viral", "dating", "breakup",
    "engaged", "releases", "release", "confirms", "reveals", "interview",
    "şarkı", "boşandı", "hamile", "itiraf", "gözaltı", "tutuklandı", "dava",
    "tepki", "olay", "polemik",
}

STOP_WORDS: set[str] = {
    "ve", "ile", "bir", "bu", "şu", "o", "da", "de", "için", "olan", "the",
    "a", "an", "in", "on", "at", "to", "for", "of", "and", "is", "are", "göre",
}


def extract_key_tokens(title: str) -> set[str]:
    """Extract significant tokens and root stems from a title for entity/subject clustering."""
    cleaned = normalize_text(title)
    tokens: set[str] = set()
    for w in cleaned.split():
        if len(w) <= 2 or w in STOP_WORDS:
            continue
        tokens.add(w)
        # Add root prefix for Turkish suffix resilience
        if len(w) >= 5:
            tokens.add(w[:5])
    return tokens



def calculate_token_overlap(set_a: set[str], set_b: set[str]) -> float:
    """
    Calculates overlap using a combination of Jaccard and containment similarity.
    Containment similarity (intersection / min_len) prevents penalizing asymmetric
    headlines where one publisher adds extra journalistic adjectives.
    """
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    jaccard = intersection / len(set_a | set_b)
    containment = intersection / min(len(set_a), len(set_b))
    return 0.4 * jaccard + 0.6 * containment


def article_priority_score(article: Article) -> int:
    """Calculate heuristic editorial priority score for ordering candidates."""
    text = normalize_text(f"{article.title} {article.snippet}")
    score = 0
    words = set(text.split())
    for kw in DEVELOPMENT_BOOST_KEYWORDS:
        if kw in words:
            score += 1
    return score



def compute_candidate_priority(
    article: Article,
    cluster_size: int = 1,
    publisher_seen_count: int = 1,
    reference_time: Optional[datetime] = None,
) -> tuple[float, list[str]]:
    """
    Lightweight, deterministic candidate prioritization strategy.

    Signals:
    1. Recency: Favors breaking developments within last hours.
    2. Entertainment / Development signals: High-signal pop culture keywords.
    3. Source Diversity: Soft dampening to avoid a single high-volume publisher crowding out other outlets.
    4. Category Relevance: Prioritizes core Turkish & Global entertainment/music/film verticals.
    5. Metadata Richness: Visual image availability (+1.5) & informative snippet (+0.5).
    6. Cross-Publisher Consensus: Multi-outlet clusters indicate major verified events (+1.5 to +3.0).

    Returns (priority_score, reasons_list).
    """
    ref_now = reference_time or article.published_at
    reasons: list[str] = []
    score = 0.0

    # 1. Recency
    age_hours = max(0.0, (ref_now - article.published_at).total_seconds() / 3600.0)
    if age_hours <= 2.5:
        score += 3.0
        reasons.append("breaking_recency_le_2h")
    elif age_hours <= 6.0:
        score += 2.0
        reasons.append("recent_le_6h")
    elif age_hours <= 12.0:
        score += 1.0
        reasons.append("moderate_recency_le_12h")
    else:
        score += 0.2
        reasons.append("older_recency")

    # 2. Entertainment & Development signals
    text = normalize_text(f"{article.title} {article.snippet}")
    words = set(text.split())
    kw_hits = [kw for kw in DEVELOPMENT_BOOST_KEYWORDS if kw in words]
    if kw_hits:
        kw_pts = min(4.0, len(kw_hits) * 0.8)
        score += kw_pts
        reasons.append(f"signal_keywords({','.join(kw_hits[:3])})")

    # 3. Source Diversity (Anti-Monopoly Dampening)
    if publisher_seen_count <= 2:
        score += 3.0
        reasons.append("source_diversity_top2")
    elif publisher_seen_count <= 5:
        score += 1.8
        reasons.append("source_diversity_top5")
    elif publisher_seen_count <= 8:
        score += 0.8
        reasons.append("source_diversity_top8")
    else:
        reasons.append("source_frequency_dampened")

    # 4. Category Relevance
    cat = (article.source_category or "").upper()
    if cat in ("MAGAZIN_TR", "VIRAL_INTERNET_TR"):
        score += 2.0
        reasons.append("core_turkish_magazin")
    elif cat in ("FILM_TV_GLOBAL", "MUSIC_GLOBAL", "HOLLYWOOD_GLOBAL", "VIRAL_CELEBRITY_GLOBAL", "MUSIC_CULTURE_GLOBAL"):
        score += 1.5
        reasons.append("core_global_entertainment")
    elif cat:
        score += 0.5
        reasons.append("general_category")

    # 5. Metadata Richness
    if article.image_url:
        score += 1.5
        reasons.append("has_visual_image")
    if len(article.snippet) >= 80:
        score += 0.5
        reasons.append("has_informative_snippet")

    # 6. Cross-Publisher Consensus Cluster Bonus
    if cluster_size >= 3:
        score += 3.0
        reasons.append("cross_publisher_consensus(3+)")
    elif cluster_size == 2:
        score += 1.5
        reasons.append("cross_publisher_consensus(2)")

    return round(score, 2), reasons


class DeduplicationEngine:
    """Multi-level deduplication and cross-publisher clustering."""

    def __init__(self, repository: StateRepository):
        self.repository = repository
        self.last_clusters_count: int = 0
        self.last_selected: list[Article] = []
        self.last_deprioritized: list[Article] = []

    def filter_fresh_candidates(
        self,
        articles: Sequence[Article],
        max_candidates: int = 55,
        similarity_threshold: float = 0.48,
    ) -> list[Article]:
        """
        Filters out already-processed articles and collapses duplicate cross-publisher
        reports of the exact same event into single candidate representatives,
        then deterministically prioritizes top candidates.
        """
        fresh_unprocessed: list[Article] = []
        seen_exact_titles: set[str] = set()

        for a in articles:
            # Level 1: Skip if already in persistent repository
            if self.repository.is_article_processed(a.id):
                continue

            norm_title = normalize_text(a.title)
            if not norm_title or norm_title in seen_exact_titles:
                continue

            seen_exact_titles.add(norm_title)
            fresh_unprocessed.append(a)

        if not fresh_unprocessed:
            self.last_clusters_count = 0
            self.last_selected = []
            self.last_deprioritized = []
            return []

        # Level 2: Cross-publisher cluster collapsing
        clusters: list[list[Article]] = []
        cluster_tokens: list[set[str]] = []

        for article in fresh_unprocessed:
            tokens = extract_key_tokens(article.title)
            matched_cluster_idx = -1

            for idx, c_tokens in enumerate(cluster_tokens):
                overlap = calculate_token_overlap(tokens, c_tokens)
                char_sim = SequenceMatcher(
                    None, normalize_text(article.title), normalize_text(clusters[idx][0].title)
                ).ratio()

                if overlap >= similarity_threshold or char_sim >= 0.75:
                    matched_cluster_idx = idx
                    break

            if matched_cluster_idx >= 0:
                clusters[matched_cluster_idx].append(article)
                cluster_tokens[matched_cluster_idx].update(tokens)
            else:
                clusters.append([article])
                cluster_tokens.append(tokens)

        # Select the best representative for each cluster
        representatives: list[Article] = []
        for cluster in clusters:
            best_article = max(
                cluster,
                key=lambda a: (article_priority_score(a), a.published_at.timestamp()),
            )
            best_article_dict = best_article.model_dump()
            best_article_dict["raw_metadata"]["cluster_article_ids"] = [a.id for a in cluster]
            best_article_dict["raw_metadata"]["cluster_size"] = len(cluster)
            representatives.append(Article(**best_article_dict))

        # Sort initially by recency to calculate publisher frequency fairly
        representatives.sort(key=lambda a: a.published_at, reverse=True)

        publisher_counts: dict[str, int] = defaultdict(int)
        scored_reps: list[tuple[float, Article]] = []
        now_ref = max((a.published_at for a in representatives), default=None)

        for art in representatives:
            pub_key = art.source.lower()
            publisher_counts[pub_key] += 1
            cluster_size = art.raw_metadata.get("cluster_size", 1)
            p_score, reasons = compute_candidate_priority(
                article=art,
                cluster_size=cluster_size,
                publisher_seen_count=publisher_counts[pub_key],
                reference_time=now_ref,
            )
            art_dict = art.model_dump()
            art_dict["raw_metadata"]["priority_score"] = p_score
            art_dict["raw_metadata"]["priority_reasons"] = reasons
            enriched_art = Article(**art_dict)
            scored_reps.append((p_score, enriched_art))

        # Sort all representatives by composite priority score, then by publication timestamp
        scored_reps.sort(
            key=lambda item: (item[0], item[1].published_at.timestamp()),
            reverse=True,
        )

        sorted_articles = [item[1] for item in scored_reps]
        self.last_clusters_count = len(sorted_articles)
        self.last_selected = sorted_articles[:max_candidates]
        self.last_deprioritized = sorted_articles[max_candidates:]

        return self.last_selected
