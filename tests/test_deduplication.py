"""Tests for DeduplicationEngine and cross-publisher clustering."""
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from efso_radar.deduplication.engine import (
    DeduplicationEngine,
    calculate_token_overlap,
    extract_key_tokens,
)
from efso_radar.domain import Article, Market
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestDeduplication(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = SqliteStateRepository(Path(self.temp_dir.name) / "test.db")
        self.engine = DeduplicationEngine(self.repo)
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_token_extraction_and_overlap(self):
        tokens_a = extract_key_tokens("Hadise ve sevgilisi ayrılık kararı aldı")
        tokens_b = extract_key_tokens("Hadise sevgilisinden ayrıldı şok karar")
        overlap = calculate_token_overlap(tokens_a, tokens_b)
        self.assertGreater(overlap, 0.3)

    def test_filter_already_processed_articles(self):
        art1 = Article.create("Haber 1", "https://news.com/1", "Source A", self.now)
        art2 = Article.create("Haber 2", "https://news.com/2", "Source B", self.now)

        # Mark art1 as processed in repo
        self.repo.mark_articles_processed([art1])

        fresh = self.engine.filter_fresh_candidates([art1, art2])
        self.assertEqual(len(fresh), 1)
        self.assertEqual(fresh[0].id, art2.id)

    def test_cross_publisher_clustering(self):
        # 3 different outlets reporting the exact same event
        outlet1 = Article.create(
            "Billie Eilish yeni albümünü resmen açıkladı",
            "https://music.com/billie-album-1",
            "Pitchfork",
            self.now,
            snippet="Resmî albüm duyurusu yapıldı.",
        )
        outlet2 = Article.create(
            "Billie Eilish yeni albüm duyurdu: Tarih açıklandı",
            "https://poptimes.com/billie-album-2",
            "Pop Times",
            self.now,
            snippet="Yeni albümün çıkış tarihi belli oldu.",
        )
        unrelated = Article.create(
            "Brad Pitt yeni film setinde görüntülendi",
            "https://movies.com/brad-pitt",
            "Cinema World",
            self.now,
        )

        candidates = self.engine.filter_fresh_candidates([outlet1, outlet2, unrelated])

        # outlet1 and outlet2 should be collapsed into a single representative!
        self.assertEqual(len(candidates), 2)
        # Check that cluster contains both article IDs in metadata
        billie_rep = next(c for c in candidates if "Billie" in c.title)
        cluster_ids = billie_rep.raw_metadata.get("cluster_article_ids", [])
        self.assertEqual(len(cluster_ids), 2)


if __name__ == "__main__":
    unittest.main()
