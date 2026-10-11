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



class DeduplicationEngine:
    """Multi-level deduplication and cross-publisher clustering."""

    def __init__(self, repository: StateRepository):
        self.repository = repository

    def filter_fresh_candidates(
        self,
        articles: Sequence[Article],
        max_candidates: int = 55,
        similarity_threshold: float = 0.48,
    ) -> list[Article]:

        """
        Filters out already-processed articles and collapses duplicate cross-publisher
        reports of the exact same event into single candidate representatives.
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
            return []

        # Level 2: Cross-publisher cluster collapsing
        # If multiple outlets report the same story within the batch, group them.
        clusters: list[list[Article]] = []
        cluster_tokens: list[set[str]] = []

        for article in fresh_unprocessed:
            tokens = extract_key_tokens(article.title)
            matched_cluster_idx = -1

            for idx, c_tokens in enumerate(cluster_tokens):
                overlap = calculate_token_overlap(tokens, c_tokens)
                # Also check sequence matcher for close Turkish phrasing
                char_sim = SequenceMatcher(
                    None, normalize_text(article.title), normalize_text(clusters[idx][0].title)
                ).ratio()

                if overlap >= similarity_threshold or char_sim >= 0.75:
                    matched_cluster_idx = idx
                    break

            if matched_cluster_idx >= 0:
                clusters[matched_cluster_idx].append(article)
                # Expand cluster tokens
                cluster_tokens[matched_cluster_idx].update(tokens)
            else:
                clusters.append([article])
                cluster_tokens.append(tokens)

        # Select the best representative for each cluster (highest priority or most recent)
        representatives: list[Article] = []
        for cluster in clusters:
            best_article = max(
                cluster,
                key=lambda a: (article_priority_score(a), a.published_at.timestamp()),
            )
            # Attach related articles in raw_metadata so AI can reference them
            best_article_dict = best_article.model_dump()
            best_article_dict["raw_metadata"]["cluster_article_ids"] = [a.id for a in cluster]
            best_article_dict["raw_metadata"]["cluster_size"] = len(cluster)
            representatives.append(Article(**best_article_dict))

        # Sort all representatives by heuristic priority and recency
        representatives.sort(
            key=lambda a: (article_priority_score(a), a.published_at.timestamp()),
            reverse=True,
        )

        return representatives[:max_candidates]
