"""Deterministic tests for Efso Radar Phase 3A News Discovery Engine.

Covers:
- RSS 2.0 & Atom feed parsing
- Turkish character and entity handling
- Timezone normalizations & freshness boundaries
- Tracking parameter stripping & canonical URL resolution
- Media enclosure and thumbnail extraction
- Fault isolation & concurrent fetching
- News relevance prefiltering (hard news vs entertainment protection)
- Source registry and excluded source catalog
"""
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from efso_radar.domain import Article, Market
from efso_radar.sources.filter import NewsRelevancePrefilter
from efso_radar.deduplication.engine import compute_candidate_priority
from efso_radar.sources.models import FeedFormat, ParseResult, SourceDefinition
from efso_radar.sources.multi_source import MultiSourceDiscoveryEngine
from efso_radar.sources.parsers import (
    FeedParser,
    clean_canonical_url,
    clean_html,
    parse_datetime,
)
from efso_radar.sources.registry import (
    DEFAULT_SOURCES,
    EXCLUDED_SOURCES_CATALOG,
    SourceRegistry,
)
from efso_radar.sources.rss import RSSFeedSource


class TestFeedParser(unittest.TestCase):
    """Test suite for RSS 2.0 and Atom XML feed parsers."""

    def setUp(self):
        self.now = datetime(2026, 10, 11, 14, 0, 0, tzinfo=timezone.utc)
        self.src_def_tr = SourceDefinition(
            source_id="test_tr",
            name="Test Magazin",
            url="https://example.com/rss",
            market=Market.TR,
            category="MAGAZIN_TR",
        )
        self.src_def_en = SourceDefinition(
            source_id="test_en",
            name="Test Hollywood",
            url="https://example.com/feed",
            market=Market.EN,
            category="FILM_TV_GLOBAL",
        )

    def test_parse_rss_2_with_turkish_chars_and_enclosure(self):
        sample_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
            <channel>
                <title>Test Magazin Feed</title>
                <item>
                    <title><![CDATA[\xc4\xb0rem Derici &amp; Sevgilisi \xc5\x9eoke Etti: 'A\xc5\x9fk \xc4\xb0tiraf\xc4\xb1' - Test Magazin]]></title>
                    <link>https://example.com/magazin/irem-derici?utm_source=twitter&amp;utm_medium=social&amp;ref=share</link>
                    <pubDate>Sun, 11 Oct 2026 12:30:00 +0300</pubDate>
                    <description><![CDATA[<p>\xc3\x9cnl\xc3\xbc \xc5\x9fark\xc4\xb1c\xc4\xb1 \xc4\xb0rem Derici konser sonras\xc4\xb1 a\xc3\xa7\xc4\xb1klamalarda bulundu.</p>]]></description>
                    <enclosure url="https://image.example.com/irem.jpg" type="image/jpeg" length="12345"/>
                </item>
            </channel>
        </rss>
        """
        articles = FeedParser.parse(sample_xml, self.src_def_tr, now=self.now, max_age_hours=16)
        self.assertEqual(len(articles), 1)
        art = articles[0]

        # Verify title suffix stripped & Turkish characters preserved
        self.assertEqual(art.title, "İrem Derici & Sevgilisi Şoke Etti: 'Aşk İtirafı'")
        self.assertEqual(art.source, "Test Magazin")
        self.assertEqual(art.market, Market.TR)
        self.assertEqual(art.source_category, "MAGAZIN_TR")

        # Verify canonical URL strips tracking parameters
        self.assertEqual(art.canonical_url, "https://example.com/magazin/irem-derici")
        self.assertEqual(art.image_url, "https://image.example.com/irem.jpg")

        # Verify UTC conversion: 12:30:00 +0300 -> 09:30:00 UTC
        expected_pub = datetime(2026, 10, 11, 9, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(art.published_at, expected_pub)
        self.assertIn("İrem Derici konser sonrası", art.snippet)

    def test_parse_atom_feed_with_namespaces(self):
        sample_atom = b"""<?xml version="1.0" encoding="utf-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
            <title>NTV Ya\xc5\x9fam</title>
            <entry>
                <id>https://ntv.com.tr/yasam/konser-duyurusu</id>
                <title type="text">D\xc3\xbcnyaca \xc3\x9cnl\xc3\xbc Y\xc4\xb1ld\xc4\xb1z \xc4\xb0stanbul'a Geliyor</title>
                <link rel="alternate" href="https://ntv.com.tr/yasam/konser-duyurusu?utm_campaign=app"/>
                <link rel="enclosure" href="https://images.ntv.com.tr/konser.jpg" type="image/jpeg"/>
                <published>2026-10-11T11:15:00+03:00</published>
                <summary type="text">\xc4\xb0stanbul Harbiye A\xc3\xa7\xc4\xb1khava sahnesinde b\xc3\xbcy\xc3\xbck konser.</summary>
            </entry>
        </feed>
        """
        articles = FeedParser.parse(sample_atom, self.src_def_tr, now=self.now, max_age_hours=16)
        self.assertEqual(len(articles), 1)
        art = articles[0]
        self.assertEqual(art.title, "Dünyaca Ünlü Yıldız İstanbul'a Geliyor")
        self.assertEqual(art.canonical_url, "https://ntv.com.tr/yasam/konser-duyurusu")
        self.assertEqual(art.image_url, "https://images.ntv.com.tr/konser.jpg")
        # 11:15:00 +0300 -> 08:15:00 UTC
        self.assertEqual(art.published_at, datetime(2026, 10, 11, 8, 15, 0, tzinfo=timezone.utc))

    def test_parse_dublin_core_date_format_tmz_style(self):
        sample_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
            <channel>
                <item>
                    <title>Billie Eilish Surprises Fans with Unannounced Single</title>
                    <link>https://tmz.com/2026/10/11/billie-eilish-surprise/</link>
                    <dc:date>2026-10-11T05:00:00-07:00</dc:date>
                    <description>Pop superstar Billie Eilish drops midnight single.</description>
                </item>
            </channel>
        </rss>
        """
        articles = FeedParser.parse(sample_xml, self.src_def_en, now=self.now, max_age_hours=16)
        self.assertEqual(len(articles), 1)
        # 05:00:00 -0700 -> 12:00:00 UTC
        self.assertEqual(articles[0].published_at, datetime(2026, 10, 11, 12, 0, 0, tzinfo=timezone.utc))

    def test_stale_and_future_articles_filtered(self):
        sample_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
            <channel>
                <item>
                    <title>Valid Fresh Headline</title>
                    <link>https://example.com/fresh</link>
                    <pubDate>Sun, 11 Oct 2026 13:00:00 GMT</pubDate>
                </item>
                <item>
                    <title>Too Old Article (2 Days Old)</title>
                    <link>https://example.com/old</link>
                    <pubDate>Fri, 09 Oct 2026 10:00:00 GMT</pubDate>
                </item>
                <item>
                    <title>Future Article (Far In The Future)</title>
                    <link>https://example.com/future</link>
                    <pubDate>Mon, 12 Oct 2026 14:00:00 GMT</pubDate>
                </item>
            </channel>
        </rss>
        """
        articles = FeedParser.parse(sample_xml, self.src_def_en, now=self.now, max_age_hours=16)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0].title, "Valid Fresh Headline")

    def test_malformed_xml_recovers_gracefully(self):
        broken_xml = b"<rss><channel><item><title>Broken XML without close tags"
        articles = FeedParser.parse(broken_xml, self.src_def_tr, now=self.now)
        self.assertEqual(articles, [])

    def test_clean_canonical_url(self):
        url = "https://variety.com/2026/film/news/avatar-sequel-12345/?utm_source=feed&utm_medium=rss&utm_campaign=syndication#comments"
        cleaned = clean_canonical_url(url)
        self.assertEqual(cleaned, "https://variety.com/2026/film/news/avatar-sequel-12345")

        # Keep functional non-tracking query parameters
        url_with_page = "https://example.com/gallery?id=45&utm_source=twitter"
        cleaned_page = clean_canonical_url(url_with_page)
        self.assertEqual(cleaned_page, "https://example.com/gallery?id=45")


class TestNewsRelevancePrefilter(unittest.TestCase):
    """Test suite for deterministic relevance prefiltering and noise suppression."""

    def setUp(self):
        self.now = datetime(2026, 10, 11, 14, 0, 0, tzinfo=timezone.utc)

    def test_filters_unrelated_sports_stats(self):
        art_sports = Article.create(
            title="Süper Lig 8. Hafta Puan Durumu ve Maç Sonuçları",
            url="https://example.com/spor/puan-durumu",
            source="Gazete Magazin",
            published_at=self.now,
        )
        approved, rejected = NewsRelevancePrefilter.filter_batch([art_sports])
        self.assertEqual(len(approved), 0)
        self.assertEqual(rejected, 1)

    def test_filters_unrelated_macroeconomics(self):
        art_econ = Article.create(
            title="Asgari ücret zammı için ilk rakamlar açıklandı",
            url="https://example.com/ekonomi/asgari-ucret",
            source="Genel Haber",
            published_at=self.now,
        )
        approved, rejected = NewsRelevancePrefilter.filter_batch([art_econ])
        self.assertEqual(len(approved), 0)
        self.assertEqual(rejected, 1)

    def test_filters_unrelated_traffic_blotter(self):
        art_accident = Article.create(
            title="TEM otoyolunda zincirleme trafik kazası meydana geldi",
            url="https://example.com/asayis/kaza",
            source="Genel Haber",
            published_at=self.now,
        )
        approved, rejected = NewsRelevancePrefilter.filter_batch([art_accident])
        self.assertEqual(len(approved), 0)
        self.assertEqual(rejected, 1)

    def test_protects_celebrity_entertainment_news(self):
        # Genuine entertainment stories must NEVER be filtered out
        celebrity_stories = [
            Article.create(
                title="Ünlü oyuncu Serenay Sarıkaya'dan aşk iddialarına ilk yanıt",
                url="https://example.com/magazin/serenay",
                source="Hürriyet Kelebek",
                published_at=self.now,
            ),
            Article.create(
                title="Dizi setinde büyük kavga: Başrol oyuncusu seti terk etti",
                url="https://example.com/magazin/dizi-kavga",
                source="Milliyet Cadde",
                published_at=self.now,
            ),
            Article.create(
                title="Şarkıcı konserinde hayranlarına yeni albüm müjdesi verdi",
                url="https://example.com/magazin/konser",
                source="Habertürk Magazin",
                published_at=self.now,
            ),
            Article.create(
                title="Zendaya and Tom Holland Spotted at Red Carpet Premiere",
                url="https://example.com/hollywood/zendaya",
                source="Variety",
                market=Market.EN,
                published_at=self.now,
            ),
        ]
        approved, rejected = NewsRelevancePrefilter.filter_batch(celebrity_stories)
        self.assertEqual(len(approved), 4)
        self.assertEqual(rejected, 0)

    def test_duplicate_url_and_same_publisher_suppression(self):
        art1 = Article.create(
            title="Hadise'den Yeni Klip Müjdesi Geldi",
            url="https://example.com/hadise-klip",
            source="Kelebek",
            published_at=self.now,
        )
        art2 = Article.create(
            title="Hadise'den Yeni Klip Müjdesi Geldi (Tekrar)",
            url="https://example.com/hadise-klip?ref=share",  # Canonical resolves to same
            canonical_url="https://example.com/hadise-klip",
            source="Kelebek",
            published_at=self.now,
        )
        art3 = Article.create(
            title="Hadise'den Yeni Klip Müjdesi Geldi",  # Same title from same publisher
            url="https://example.com/hadise-klip-farkli-url",
            source="Kelebek",
            published_at=self.now,
        )
        approved, rejected = NewsRelevancePrefilter.filter_batch([art1, art2, art3])
        self.assertEqual(len(approved), 1)
        self.assertEqual(rejected, 2)


class TestSourceRegistry(unittest.TestCase):
    """Test suite for news source registry and excluded candidate catalog."""

    def test_default_registry_sources(self):
        registry = SourceRegistry()
        all_sources = registry.get_all()
        # Verify 11 verified primary sources are registered
        self.assertGreaterEqual(len(all_sources), 11)

        source_ids = {s.source_id for s in all_sources}
        expected_ids = {
            "hurriyet_kelebek", "onedio_magazin", "haberturk_magazin",
            "milliyet_cadde", "ntv_yasam", "variety", "deadline",
            "billboard", "rolling_stone", "tmz", "e_news"
        }
        for exp in expected_ids:
            self.assertIn(exp, source_ids)

    def test_market_filtering(self):
        registry = SourceRegistry()
        tr_sources = registry.get_active(Market.TR)
        en_sources = registry.get_active(Market.EN)

        self.assertTrue(all(s.market == Market.TR for s in tr_sources))
        self.assertTrue(all(s.market == Market.EN for s in en_sources))
        self.assertGreaterEqual(len(tr_sources), 5)
        self.assertGreaterEqual(len(en_sources), 6)

    def test_disable_enable_source(self):
        registry = SourceRegistry()
        self.assertTrue(registry.set_enabled("variety", False))
        active_ids = {s.source_id for s in registry.get_active()}
        self.assertNotIn("variety", active_ids)

        self.assertTrue(registry.set_enabled("variety", True))
        active_ids_after = {s.source_id for s in registry.get_active()}
        self.assertIn("variety", active_ids_after)

    def test_excluded_sources_catalog(self):
        # Verify documented exclusion reasons for People and Entertainment Tonight
        self.assertIn("people", EXCLUDED_SOURCES_CATALOG)
        self.assertIn("HTTP 402", EXCLUDED_SOURCES_CATALOG["people"]["reason"])

        self.assertIn("etonline", EXCLUDED_SOURCES_CATALOG)
        self.assertIn("irregular", EXCLUDED_SOURCES_CATALOG["etonline"]["reason"].lower())


class TestMultiSourceDiscoveryEngine(unittest.TestCase):
    """Test suite for concurrent fetching, fault isolation, and metrics."""

    def setUp(self):
        self.now = datetime(2026, 10, 11, 14, 0, 0, tzinfo=timezone.utc)

    @patch("efso_radar.sources.multi_source.RSSFeedSource.fetch_result")
    def test_concurrent_fetching_and_fault_isolation(self, mock_fetch_result):
        # Setup 3 dummy sources in registry
        s1 = SourceDefinition(source_id="s1", name="Source 1", url="https://s1.com/rss", market=Market.TR)
        s2 = SourceDefinition(source_id="s2", name="Source 2", url="https://s2.com/rss", market=Market.TR)
        s3 = SourceDefinition(source_id="s3", name="Source 3", url="https://s3.com/rss", market=Market.TR)
        registry = SourceRegistry([s1, s2, s3])

        art1 = Article.create(title="Ünlü İsimden Bomba Açıklama", url="https://s1.com/1", source="Source 1", published_at=self.now)
        art3 = Article.create(title="Konser Biletleri Dakikalar İçinde Tükendi", url="https://s3.com/3", source="Source 3", published_at=self.now)

        call_count = {"count": 0}
        def fetch_dispatch(now, max_age_hours):
            call_count["count"] += 1
            if call_count["count"] == 2:
                raise TimeoutError("Connection to s2 timed out after 14.0s")
            art = art1 if call_count["count"] == 1 else art3
            return ParseResult(articles=[art], raw_seen=1, stale_count=0, invalid_count=0)

        mock_fetch_result.side_effect = fetch_dispatch

        engine = MultiSourceDiscoveryEngine(registry=registry, include_google_news=False, max_workers=2)
        candidates, successes = engine.fetch_candidates(now=self.now)

        # Fault isolation verification:
        # Source 2 failed, but Source 1 and Source 3 succeeded!
        self.assertEqual(successes, 2)
        self.assertEqual(len(candidates), 2)
        self.assertIsNotNone(engine.last_metrics)
        metrics = engine.last_metrics

        self.assertEqual(metrics.sources_attempted, 3)
        self.assertEqual(metrics.sources_succeeded, 2)
        self.assertEqual(metrics.parsed_articles, 2)
        self.assertEqual(metrics.articles_discovered, 2)
        self.assertEqual(metrics.eligible_fresh_articles, 2)
        self.assertEqual(metrics.unique_candidates, 2)
        self.assertEqual(len(metrics.source_errors), 1)
        self.assertIn("timed out", list(metrics.source_errors.values())[0])

    def test_candidate_prioritization_signals(self):
        # Verify deterministic priority scoring signals
        art_fresh_rich = Article.create(
            title="Dua Lipa İstanbul'da büyük konser vereceğini resmen açıkladı",
            url="https://variety.com/dualipa",
            source="Variety",
            published_at=self.now,
            snippet="Dua Lipa dünya turnesi kapsamında sahne alacağını ve yeni albüm hazırlıklarını resmen duyurdu.",
            image_url="https://images.com/dualipa.jpg",
            source_category="MUSIC_GLOBAL",
        )
        score_rich, reasons_rich = compute_candidate_priority(
            art_fresh_rich, cluster_size=2, publisher_seen_count=1, reference_time=self.now
        )
        self.assertGreater(score_rich, 5.0)
        self.assertTrue(any("signal_keywords" in r for r in reasons_rich))
        self.assertTrue(any("has_visual_image" in r for r in reasons_rich))
        self.assertTrue(any("cross_publisher_consensus" in r for r in reasons_rich))

        # Publisher frequency dampening
        score_heavy_pub, reasons_heavy = compute_candidate_priority(
            art_fresh_rich, cluster_size=1, publisher_seen_count=10, reference_time=self.now
        )
        self.assertLess(score_heavy_pub, score_rich)
        self.assertTrue(any("source_frequency_dampened" in r for r in reasons_heavy))


if __name__ == "__main__":
    unittest.main()

