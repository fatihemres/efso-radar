import os
import sys
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import radar


class RadarTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 8, tzinfo=timezone.utc)
        self.article = {
            "title": "Famous singer announces new album", "link": "https://news.google.com/rss/articles/abc",
            "source": "Example", "market": "EN", "published_utc": self.now.isoformat(),
            "description": "Artist officially announced album", "id": "abc123",
        }

    def test_legacy_judge_does_not_dispatch_with_a_key(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "synthetic-test-key"}):
            with self.assertRaisesRegex(RuntimeError, "free-tier/no-billing"):
                radar.judge([self.article], {"sent": []}, self.now)

    def test_rss_date_source_and_old_filter(self):
        sample = b'''<rss><channel><item><title>A new album - Pop Times</title><link>https://news.google.com/abc</link><pubDate>Sun, 11 Oct 2026 07:00:00 GMT</pubDate><source>Pop Times</source><description>News</description></item><item><title>Old news</title><link>https://news.google.com/old</link><pubDate>Fri, 09 Oct 2026 07:00:00 GMT</pubDate></item></channel></rss>'''
        articles = radar.parse_rss(sample, "EN", self.now)
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["title"], "A new album")

    def test_processed_article_not_resubmitted(self):
        self.assertFalse(radar.unique_new([self.article], {"abc123": self.now.isoformat()}))

    def test_low_score_rejected(self):
        p = self.pick(score=80)
        self.assertEqual(radar.validate_picks([p], [self.article], []), [])

    def test_high_score_accepted(self):
        self.assertEqual(len(radar.validate_picks([self.pick()], [self.article], [])), 1)

    def test_repeat_of_old_headline_rejected(self):
        sent = [{"story_id": "OLD", "headline_tr": "Yeni albüm duyuruldu"}]
        self.assertEqual(radar.validate_picks([self.pick()], [self.article], sent), [])

    def test_update_requires_prior_alert_and_real_change(self):
        p = self.pick(kind="UPDATE", parent_story_id="OLD", development="Resmî albüm çıkış tarihi açıklandı")
        self.assertEqual(radar.validate_picks([p], [self.article], []), [])
        sent = [{"story_id": "OLD", "headline_tr": "Yeni albüm duyuruldu"}]
        self.assertEqual(len(radar.validate_picks([p], [self.article], sent)), 1)

    def test_telegram_escapes_untrusted_text(self):
        p = radar.validate_picks([self.pick()], [self.article], [])[0]
        p["headline_tr"] = "<script>"
        self.assertIn("&lt;script&gt;", radar.telegram_text(p))

    def pick(self, **override):
        p = {
            "article_ids": ["abc123"], "score": 91, "kind": "NEW",
            "parent_story_id": "", "development": "", "headline_tr": "Yeni albüm duyuruldu",
            "cover_title_tr": "YENİ ALBÜM GELİYOR", "caption_idea_tr": "Albüm hakkında haber.",
            "why_efso_tr": "Pop müzik gündemi.", "suggested_format": "POST",
            "category": "GLOBAL",
        }
        p.update(override)
        return p


if __name__ == "__main__":
    unittest.main()
