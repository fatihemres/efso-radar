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

    def test_distinct_platform_streaming_guides_never_merge_issue_50(self):
        """Regression test for Issue #50:
        Distinct streaming catalog guides (Netflix vs Disney+) sharing boilerplate phrasing
        ('What to Watch on ... in November 2026') must NOT collapse into one cluster.
        """
        netflix_guide = Article.create(
            "What to Watch on Netflix in November 2026",
            "https://variety.com/feature/whats-on-netflix-movies-shows-1203517873/",
            "Variety",
            self.now,
            snippet="A complete guide to movies, TV shows and original series coming to Netflix in November 2026.",
        )
        disney_guide = Article.create(
            "What to Watch on Disney+ in November 2026",
            "https://variety.com/feature/whats-on-disney-plus-movies-shows-1203517874/",
            "Variety",
            self.now,
            snippet="A complete guide to movies, TV shows and original series coming to Disney+ in November 2026.",
        )

        candidates = self.engine.filter_fresh_candidates([netflix_guide, disney_guide])

        # Must remain TWO separate candidate clusters
        self.assertEqual(
            len(candidates),
            2,
            f"Expected 2 separate candidate clusters, but got {len(candidates)}. "
            f"Articles falsely merged: {[c.title for c in candidates]}",
        )
        netflix_rep = next(c for c in candidates if "Netflix" in c.title)
        disney_rep = next(c for c in candidates if "Disney+" in c.title)

        self.assertEqual(netflix_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(netflix_rep.raw_metadata.get("cluster_article_ids"), [netflix_guide.id])
        self.assertEqual(disney_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(disney_rep.raw_metadata.get("cluster_article_ids"), [disney_guide.id])

    def test_same_platform_streaming_guide_rephrasings_still_merge(self):
        """Positive control: Same platform (Netflix) guide published across two outlets
        covering the identical monthly catalog event must continue clustering together.
        """
        outlet_a = Article.create(
            "What to Watch on Netflix in November 2026",
            "https://variety.com/feature/whats-on-netflix-movies-shows-1203517873/",
            "Variety",
            self.now,
            snippet="A complete guide to movies, TV shows and original series coming to Netflix in November 2026.",
        )
        outlet_b = Article.create(
            "New on Netflix in November 2026: Movies and Shows to Stream",
            "https://hollywoodreporter.com/tv/tv-news/new-on-netflix-november-2026-123456/",
            "The Hollywood Reporter",
            self.now,
            snippet="Here is everything arriving on Netflix streaming in November 2026.",
        )

        candidates = self.engine.filter_fresh_candidates([outlet_a, outlet_b])

        # Must collapse into ONE candidate cluster
        self.assertEqual(len(candidates), 1)
        cluster_rep = candidates[0]
        self.assertEqual(cluster_rep.raw_metadata.get("cluster_size"), 2)
        self.assertEqual(set(cluster_rep.raw_metadata.get("cluster_article_ids")), {outlet_a.id, outlet_b.id})
        self.assertEqual(set(cluster_rep.raw_metadata.get("cluster_publishers")), {"Variety", "The Hollywood Reporter"})


    def test_stale_batch_diagnostic_distinct_entities_remain_separate(self):
        """Diagnostic negative controls from Issue #50 (Arzu Okay vs Meral Zeren; İzzet Yıldızhan vs Güllü):
        Distinct persons sharing Turkish headline idioms ('Son Hali Gündem Oldu' or legal terms)
        must remain separate clusters.
        """
        # Pair 1: Arzu Okay vs Meral Zeren
        arzu = Article.create(
            "Yeşilçam'ın İddialı Sahneleriyle Meşhur Yıldızı Arzu Okay'ın Son Hali Gündem Oldu!",
            "https://haber.com/arzu-okay",
            "Gazete A",
            self.now,
        )
        meral = Article.create(
            "Salako'nun Emine'si Meral Zeren'in Son Hali Gündem Oldu",
            "https://haber.com/meral-zeren",
            "Gazete B",
            self.now,
        )
        c_yesilcam = self.engine.filter_fresh_candidates([arzu, meral])
        self.assertEqual(len(c_yesilcam), 2)

        # Pair 2: İzzet Yıldızhan vs Güllü
        izzet = Article.create(
            "İzzet Yıldızhan hakkında beraat kararı verildi: Dava sonuçlandı",
            "https://haber.com/izzet-yildizhan",
            "Haber 1",
            self.now,
        )
        gullu = Article.create(
            "Güllü davasında gerekçeli karar açıklandı",
            "https://haber.com/gullu",
            "Haber 2",
            self.now,
        )
        c_legal = self.engine.filter_fresh_candidates([izzet, gullu])
        self.assertEqual(len(c_legal), 2)


    def test_honorific_prefix_same_person_still_merges_patrick_stewart(self):
        """Regression test for P2-1:
        Articles reporting the same event with an honorific/prefix ('Sir Patrick Stewart' vs 'Patrick Stewart')
        must collapse into one cluster even if heuristic bigram extraction yields 'sir patrick' vs 'patrick stewart'.
        """
        art_sir = Article.create(
            "Sir Patrick Stewart hayatını kaybetti",
            "https://haber.com/sir-patrick-stewart-vefat",
            "Haber A",
            self.now,
            snippet="Usta oyuncu Sir Patrick Stewart 86 yaşında yaşamını yitirdi.",
        )
        art_plain = Article.create(
            "Patrick Stewart hayatını kaybetti",
            "https://haber.com/patrick-stewart-vefat",
            "Haber B",
            self.now,
            snippet="Hollywood'un efsane ismi Patrick Stewart hayatını kaybetti.",
        )

        candidates = self.engine.filter_fresh_candidates([art_sir, art_plain])
        self.assertEqual(
            len(candidates),
            1,
            f"Expected 1 merged cluster for Patrick Stewart, but got {len(candidates)}: {[c.title for c in candidates]}",
        )
        self.assertEqual(candidates[0].raw_metadata.get("cluster_size"), 2)
        self.assertEqual(set(candidates[0].raw_metadata.get("cluster_article_ids")), {art_sir.id, art_plain.id})

    def test_unrecognized_streaming_platforms_never_merge_hulu_vs_peacock(self):
        """Regression test for P2-2:
        Monthly streaming catalog guides for distinct unrecognized platforms (Hulu vs Peacock)
        must NOT collapse into one cluster despite high boilerplate template similarity.
        """
        hulu_guide = Article.create(
            "What to Watch on Hulu in November 2026",
            "https://variety.com/feature/whats-on-hulu-movies-shows-1203517875/",
            "Variety",
            self.now,
            snippet="A complete guide to movies, TV shows and original series coming to Hulu in November 2026.",
        )
        peacock_guide = Article.create(
            "What to Watch on Peacock in November 2026",
            "https://variety.com/feature/whats-on-peacock-movies-shows-1203517876/",
            "Variety",
            self.now,
            snippet="A complete guide to movies, TV shows and original series coming to Peacock in November 2026.",
        )

        candidates = self.engine.filter_fresh_candidates([hulu_guide, peacock_guide])
        self.assertEqual(
            len(candidates),
            2,
            f"Expected 2 separate candidate clusters for Hulu and Peacock, but got {len(candidates)}: {[c.title for c in candidates]}",
        )
        hulu_rep = next(c for c in candidates if "Hulu" in c.title)
        peacock_rep = next(c for c in candidates if "Peacock" in c.title)
        self.assertEqual(hulu_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(peacock_rep.raw_metadata.get("cluster_size", 1), 1)


    def test_shared_first_name_different_celebrities_never_merge_ryan_reynolds_vs_gosling(self):
        """Regression test for P2-3:
        Different celebrities sharing a common first name (Ryan Reynolds vs Ryan Gosling)
        must NOT merge, even with similar event phrasing ('spotted at a film premiere').
        """
        reynolds = Article.create(
            "Ryan Reynolds spotted at a film premiere",
            "https://variety.com/news/ryan-reynolds-premiere-123",
            "Variety",
            self.now,
            snippet="Ryan Reynolds walked the red carpet at the film premiere in New York.",
        )
        gosling = Article.create(
            "Ryan Gosling spotted at a film premiere",
            "https://variety.com/news/ryan-gosling-premiere-456",
            "Variety",
            self.now,
            snippet="Ryan Gosling walked the red carpet at the film premiere in New York.",
        )

        candidates = self.engine.filter_fresh_candidates([reynolds, gosling])
        self.assertEqual(
            len(candidates),
            2,
            f"Expected 2 separate candidate clusters for Ryan Reynolds and Ryan Gosling, but got {len(candidates)}: {[c.title for c in candidates]}",
        )
        reynolds_rep = next(c for c in candidates if "Reynolds" in c.title)
        gosling_rep = next(c for c in candidates if "Gosling" in c.title)
        self.assertEqual(reynolds_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(gosling_rep.raw_metadata.get("cluster_size", 1), 1)

    def test_shared_first_name_different_celebrities_never_merge_chris_pratt_vs_pine(self):
        """Secondary negative control for P2-3:
        Chris Pratt vs Chris Pine must NOT merge despite sharing first name 'Chris'.
        """
        pratt = Article.create(
            "Chris Pratt attends premiere in London",
            "https://variety.com/news/chris-pratt-london-123",
            "Variety",
            self.now,
            snippet="Chris Pratt was seen attending the premiere in London.",
        )
        pine = Article.create(
            "Chris Pine attends premiere in London",
            "https://variety.com/news/chris-pine-london-456",
            "Variety",
            self.now,
            snippet="Chris Pine was seen attending the premiere in London.",
        )

        candidates = self.engine.filter_fresh_candidates([pratt, pine])
        self.assertEqual(
            len(candidates),
            2,
            f"Expected 2 separate candidate clusters for Chris Pratt and Chris Pine, but got {len(candidates)}: {[c.title for c in candidates]}",
        )
        pratt_rep = next(c for c in candidates if "Pratt" in c.title)
        pine_rep = next(c for c in candidates if "Pine" in c.title)
        self.assertEqual(pratt_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(pine_rep.raw_metadata.get("cluster_size", 1), 1)


    def test_distinct_platform_new_on_catalog_templates_never_merge_issue_50_review(self):
        """Regression test for Issue #50 review finding:
        'New on Netflix in November 2026' vs 'New on Disney+ in November 2026'
        must NOT merge despite the headline starting with common template word 'New'.
        Also verifies unrecognized platforms (Hulu vs Peacock) with 'New on...' stay separate.
        """
        netflix_guide = Article.create(
            "New on Netflix in November 2026",
            "https://variety.com/2026/film/news/new-on-netflix-november-2026-123",
            "Variety",
            self.now,
            snippet="Every movie and show coming to Netflix in November 2026.",
        )
        disney_guide = Article.create(
            "New on Disney+ in November 2026",
            "https://variety.com/2026/film/news/new-on-disney-plus-november-2026-456",
            "Variety",
            self.now,
            snippet="Every movie and show coming to Disney+ in November 2026.",
        )

        candidates = self.engine.filter_fresh_candidates([netflix_guide, disney_guide])
        self.assertEqual(
            len(candidates),
            2,
            f"Expected 2 separate candidate clusters for Netflix and Disney+, but got {len(candidates)}: {[c.title for c in candidates]}",
        )
        netflix_rep = next(c for c in candidates if "Netflix" in c.title)
        disney_rep = next(c for c in candidates if "Disney+" in c.title)
        self.assertEqual(netflix_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(disney_rep.raw_metadata.get("cluster_size", 1), 1)

        # Unrecognized platforms with 'New on...' template
        hulu_guide = Article.create(
            "New on Hulu in November 2026",
            "https://variety.com/2026/film/news/new-on-hulu-november-2026-789",
            "Variety",
            self.now,
            snippet="Every movie and show coming to Hulu in November 2026.",
        )
        peacock_guide = Article.create(
            "New on Peacock in November 2026",
            "https://variety.com/2026/film/news/new-on-peacock-november-2026-101",
            "Variety",
            self.now,
            snippet="Every movie and show coming to Peacock in November 2026.",
        )
        unrecognized_candidates = self.engine.filter_fresh_candidates([hulu_guide, peacock_guide])
        self.assertEqual(
            len(unrecognized_candidates),
            2,
            f"Expected 2 separate candidate clusters for Hulu and Peacock, but got {len(unrecognized_candidates)}: {[c.title for c in unrecognized_candidates]}",
        )
        hulu_rep = next(c for c in unrecognized_candidates if "Hulu" in c.title)
        peacock_rep = next(c for c in unrecognized_candidates if "Peacock" in c.title)
        self.assertEqual(hulu_rep.raw_metadata.get("cluster_size", 1), 1)
        self.assertEqual(peacock_rep.raw_metadata.get("cluster_size", 1), 1)


if __name__ == "__main__":
    unittest.main()
