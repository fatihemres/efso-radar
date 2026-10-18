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

    def test_unrelated_mortality_events_never_merge_calib03_calib44_vs_metin_arolat(self):
        """Regression test for CALIB-03/CALIB-44 audit:
        - CALIB-03 and CALIB-44 (both about Atilla Taş's daughter Sezen Taş) DO merge.
        - An unrelated mortality story about a different person (e.g. Metin Arolat) MUST NEVER merge with them,
          even if both share generic mortality keywords ('hayatını kaybetti', 'cenaze', 'vefat').
        """
        calib_03 = Article.create(
            "14. Kattan Düşerek Hayatını Kaybetmişti: Atilla Taş'ın Kızı Sezen Taş'ın Eski Eşinin Paylaşımı Tepki Çekti!",
            "https://onedio.com/calib-03",
            "Onedio Magazin",
            self.now,
            snippet="Mersin'de 14. kattan düşerek hayatını kaybeden Sezen Taş toprağa verildi. Eski eşi Uğur Haliloğlu'nun cenaze sonrası paylaşımı tepki topladı.",
        )
        calib_44 = Article.create(
            "Atilla Taş'ın acı günü... Kızı 14'üncü kattan düşerek hayatını kaybetti: Eski eşin paylaşımı 'pes' dedirtti!",
            "https://hurriyet.com.tr/calib-44",
            "Hürriyet Kelebek",
            self.now,
            snippet="Şarkıcı Atilla Taş'ın kızı Sezen Taş 14. kattan düşerek yaşamını yitirdi. Eski eşi Uğur Haliloğlu'nun paylaşımı tepki çekti.",
        )
        metin_arolat = Article.create(
            "Sahnedeyken fenalaşarak hayatını kaybetmişti: Metin Arolat son yolculuğuna uğurlandı, cenaze töreninde gözyaşları sel oldu!",
            "https://milliyet.com.tr/metin-arolat",
            "Milliyet Cadde",
            self.now,
            snippet="Ünlü şarkıcı ve yönetmen Metin Arolat hayatını kaybetti. Cenaze töreninde sanat dünyası bir araya geldi.",
        )

        candidates = self.engine.filter_fresh_candidates([calib_03, calib_44, metin_arolat])
        # Must produce exactly 2 clusters: [Sezen Taş (CALIB-03 + CALIB-44)] and [Metin Arolat]
        self.assertEqual(len(candidates), 2)

        sezen_rep = next(c for c in candidates if "Sezen" in c.title or "Atilla Taş" in c.title)
        metin_rep = next(c for c in candidates if "Metin Arolat" in c.title)

        self.assertEqual(set(sezen_rep.raw_metadata["cluster_article_ids"]), {calib_03.id, calib_44.id})
        self.assertEqual(metin_rep.raw_metadata["cluster_article_ids"], [metin_arolat.id])

    def test_unrelated_events_for_same_celebrity_do_not_merge(self):
        """Two completely different events about the same celebrity must remain separate candidates."""
        event_a = Article.create(
            "Hadise Harbiye Açıkhava konserinde sahne şovuyla büyüledi",
            "https://magazin.com/hadise-konser",
            "Magazin A",
            self.now,
            snippet="Ünlü şarkıcı Hadise Harbiye konserinde binlerce hayranıyla buluştu.",
        )
        event_b = Article.create(
            "Hadise yeni reklam anlaşmasıyla rekor ücret aldı",
            "https://magazin.com/hadise-reklam",
            "Magazin B",
            self.now,
            snippet="Hadise bir kozmetik markasının yüzü olarak anlaşma imzaladı.",
        )

        candidates = self.engine.filter_fresh_candidates([event_a, event_b])
        self.assertEqual(len(candidates), 2)


if __name__ == "__main__":
    unittest.main()
