"""
Hypothesis-based property tests for DeduplicationEngine and collision reproduction.

Issue #52: Bounded offline QA spike exploring property-based deduplication
invariants, positive same-event clustering, negative distinct-person cases,
cluster inventory consistency, and reproducing the suspected long-template
streaming guide collision (>0.92 SequenceMatcher ratio).
"""
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from hypothesis import given, settings, strategies as st

from efso_radar.deduplication.engine import DeduplicationEngine
from efso_radar.domain import Article
from efso_radar.repositories.sqlite import SqliteStateRepository

# Fixed UTC reference time for hermetic reproducibility
FIXED_NOW = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)

# Pinned CI-friendly settings: bounded examples, deterministic seed, no disk DB, no deadline
HYPOTHESIS_SETTINGS = settings(
    max_examples=30,
    derandomize=True,
    database=None,
    deadline=None,
)


def _create_engine_with_temp_repo(temp_dir: str) -> tuple[DeduplicationEngine, SqliteStateRepository]:
    """Create an isolated DeduplicationEngine backed by a fresh temporary SQLite database."""
    db_path = Path(temp_dir) / "test_dedup_prop.db"
    repo = SqliteStateRepository(db_path)
    engine = DeduplicationEngine(repo)
    return engine, repo


# ============================================================================
# 1. Long-Template Streaming Guide Collision Reproduction (Issue #52)
# ============================================================================

STREAMING_PLATFORMS = ["Netflix", "Disney+", "HBO Max"]

STREAMING_LONG_TEMPLATES = [
    # Independently observed candidate 1 from Issue #52 (>0.95 char_sim)
    "A Complete Guide to All New Shows and Movies on {platform} in November 2026 Including Returning Originals",
    # Independently observed candidate 2 from Issue #52 (>0.95 char_sim)
    "New Shows and Movies Coming to {platform} in November 2026: Complete Streaming Guide and Release Calendar",
    # Additional realistic catalog release schedule template (>0.93 char_sim)
    "Everything Coming to {platform} and Leaving the Service in December 2026: Full Release Schedule and Streaming Lineup",
]


def test_reproduce_long_template_streaming_guide_collision():
    """
    Deterministic reproduction of the exact long-template streaming collision in Issue #52.

    Candidate pair: Netflix vs Disney+ with near-identical boilerplate headlines.
    SequenceMatcher character similarity is >0.955 (exceeds the 0.92 threshold in engine.py:788).
    Because conflicting_affinity_entities falls back to char_sim >= 0.92, the engine falsely
    merges distinct streaming catalogs into a single cluster.

    Expected: 2 separate clusters.
    Current engine behavior: 1 cluster (FAILS).
    """
    with tempfile.TemporaryDirectory() as td:
        engine, _ = _create_engine_with_temp_repo(td)

        a1 = Article.create(
            "A Complete Guide to All New Shows and Movies on Netflix in November 2026 Including Returning Originals",
            "https://outlet-a.example.com/netflix-november-2026",
            "Outlet A",
            FIXED_NOW,
            snippet="Everything coming to Netflix streaming in November 2026.",
        )
        a2 = Article.create(
            "A Complete Guide to All New Shows and Movies on Disney+ in November 2026 Including Returning Originals",
            "https://outlet-b.example.com/disney-november-2026",
            "Outlet B",
            FIXED_NOW,
            snippet="Everything coming to Disney+ streaming in November 2026.",
        )

        results = engine.filter_fresh_candidates([a1, a2])

        assert len(results) == 2, (
            f"False merge: distinct catalogs Netflix and Disney+ collapsed into {len(results)} cluster(s). "
            f"Cluster IDs: {[r.raw_metadata.get('cluster_article_ids') for r in results]}"
        )
        assert engine.last_clusters_count == 2


@HYPOTHESIS_SETTINGS
@given(
    pair=st.sampled_from([
        ("Netflix", "Disney+"),
        ("Netflix", "HBO Max"),
        ("Disney+", "HBO Max"),
    ]),
    template=st.sampled_from(STREAMING_LONG_TEMPLATES),
)
def test_property_distinct_streaming_catalogs_remain_separate(
    pair: tuple[str, str],
    template: str,
):
    """
    Property: Articles announcing distinct streaming platform catalogs must never merge,
    regardless of how much boilerplate headline phrasing they share.
    """
    p1, p2 = pair
    title1 = template.format(platform=p1)
    title2 = template.format(platform=p2)

    with tempfile.TemporaryDirectory() as td:
        engine, _ = _create_engine_with_temp_repo(td)

        a1 = Article.create(
            title1,
            f"https://outlet-alpha.example.com/{p1.lower()}-guide",
            "StreamWire",
            FIXED_NOW,
            snippet=f"Full schedule for {p1} streaming service.",
        )
        a2 = Article.create(
            title2,
            f"https://outlet-beta.example.com/{p2.lower()}-guide",
            "WatchList",
            FIXED_NOW,
            snippet=f"Full schedule for {p2} streaming service.",
        )

        results = engine.filter_fresh_candidates([a1, a2])

        assert len(results) == 2, (
            f"Distinct streaming platforms {p1} and {p2} were falsely merged into {len(results)} cluster. "
            f"Template: {template!r}"
        )
        assert engine.last_clusters_count == 2


# ============================================================================
# 2. Distinct People Sharing a First Name (Negative Constraint)
# ============================================================================

PERSON_PAIRS = [
    ("Ryan Reynolds", "Ryan Gosling"),
    ("Chris Pratt", "Chris Pine"),
]

PERSON_CONSTRAINED_TEMPLATES = [
    "{person} yeni film setinde görüntülendi",
    "{person} galada kırmızı halıda poz verdi",
    "{person} ile özel röportaj",
]


@HYPOTHESIS_SETTINGS
@given(
    pair=st.sampled_from(PERSON_PAIRS),
    template=st.sampled_from(PERSON_CONSTRAINED_TEMPLATES),
)
def test_property_different_people_sharing_first_name_remain_separate(
    pair: tuple[str, str],
    template: str,
):
    """
    Property: Distinct people sharing a common first name must remain separate
    for constrained, unambiguous news templates.
    """
    p1, p2 = pair
    title1 = template.format(person=p1)
    title2 = template.format(person=p2)

    with tempfile.TemporaryDirectory() as td:
        engine, _ = _create_engine_with_temp_repo(td)

        a1 = Article.create(
            title1,
            f"https://outlet-a.example.com/{p1.lower().replace(' ', '-')}",
            "CinemaDaily",
            FIXED_NOW,
        )
        a2 = Article.create(
            title2,
            f"https://outlet-b.example.com/{p2.lower().replace(' ', '-')}",
            "FilmWeekly",
            FIXED_NOW,
        )

        results = engine.filter_fresh_candidates([a1, a2])

        assert len(results) == 2, (
            f"Distinct people {p1} and {p2} were falsely merged into {len(results)} cluster. "
            f"Template: {template!r}"
        )
        assert engine.last_clusters_count == 2


# ============================================================================
# 3. Same Person + Same Concrete Event Rephrased (Positive Duplicate Control)
# ============================================================================

REPHRASED_SAME_EVENT_PAIRS = [
    (
        "Billie Eilish yeni albümünü resmen duyurdu",
        "Billie Eilish yeni albüm çıkaracağını açıkladı: Tarih belli oldu",
    ),
    (
        "Dua Lipa dünya turnesi kapsamında İstanbul konserini duyurdu",
        "Dua Lipa İstanbul konseri resmileşti: Yeni dünya turnesi",
    ),
    (
        "Hadise yeni single klibini hayranlarıyla paylaştı",
        "Hadise yeni şarkısının klibini yayınladı",
    ),
    (
        "Tarkan yeni stüdyo albümü için stüdyoya girdi",
        "Tarkan yeni albüm hazırlığı için stüdyoda",
    ),
    (
        "Brad Pitt yeni Formula 1 filminin çekimlerinde",
        "Brad Pitt F1 filminin setinde görüntülendi",
    ),
]


@HYPOTHESIS_SETTINGS
@given(
    pair=st.sampled_from(REPHRASED_SAME_EVENT_PAIRS),
)
def test_property_same_person_same_event_rephrased_headlines_cluster(
    pair: tuple[str, str],
):
    """
    Property: Two articles covering the same person and same concrete event
    rephrased by different publishers must collapse into a single cluster.
    """
    t1, t2 = pair

    with tempfile.TemporaryDirectory() as td:
        engine, _ = _create_engine_with_temp_repo(td)

        a1 = Article.create(
            t1,
            "https://publisher-one.example.com/news-1",
            "Publisher One",
            FIXED_NOW,
            snippet=f"Detaylar: {t1}",
        )
        a2 = Article.create(
            t2,
            "https://publisher-two.example.com/news-2",
            "Publisher Two",
            FIXED_NOW,
            snippet=f"Detaylar: {t2}",
        )

        results = engine.filter_fresh_candidates([a1, a2])

        assert len(results) == 1, (
            f"Positive duplicate failed to cluster: expected 1 cluster for rephrased coverage, "
            f"got {len(results)}. Titles: {t1!r} vs {t2!r}"
        )
        assert engine.last_clusters_count == 1
        cluster_ids = results[0].raw_metadata.get("cluster_article_ids", [])
        assert a1.id in cluster_ids
        assert a2.id in cluster_ids


# ============================================================================
# 4. Cluster Metadata & Inventory Consistency (Partition Invariant)
# ============================================================================

DISTINCT_TOPIC_ARTICLES = [
    ("Hadise yeni klibi için kamera karşısına geçti", "Müzik Kulisi"),
    ("Brad Pitt F1 filminin çekimlerinde Silverstone pistinde", "Sinema Rehberi"),
    ("Kıvanç Tatlıtuğ yeni dizisinin ilk okuma provasında", "Dizi Dünyası"),
    ("Taylor Swift Eras Tour konser rekorları kırmaya devam ediyor", "Pop Gazetesi"),
    ("Dua Lipa ve sevgilisi Venedik Film Festivali'nde", "Magazin Hattı"),
]


@HYPOTHESIS_SETTINGS
@given(
    selected_indices=st.lists(
        st.integers(min_value=0, max_value=len(DISTINCT_TOPIC_ARTICLES) - 1),
        min_size=2,
        max_size=4,
        unique=True,
    ),
)
def test_property_cluster_inventory_consistency(selected_indices: list[int]):
    """
    Property: For any cohort of unprocessed articles with distinct normalized titles,
    the clustering operation must form a strict partition over input article IDs:
    - Every input article ID appears in cluster_article_ids of exactly one representative.
    - Total items across all cluster_article_ids equals the count of input articles.
    - last_clusters_count matches the count of returned representatives.
    """
    with tempfile.TemporaryDirectory() as td:
        engine, _ = _create_engine_with_temp_repo(td)

        articles = [
            Article.create(
                DISTINCT_TOPIC_ARTICLES[idx][0],
                f"https://outlet-{idx}.example.com/art-{idx}",
                DISTINCT_TOPIC_ARTICLES[idx][1],
                FIXED_NOW,
            )
            for idx in selected_indices
        ]

        results = engine.filter_fresh_candidates(articles, max_candidates=50)

        assert engine.last_clusters_count == len(results)

        collected_ids: list[str] = []
        for rep in results:
            c_ids = rep.raw_metadata.get("cluster_article_ids", [])
            assert len(c_ids) >= 1
            assert rep.id in c_ids
            collected_ids.extend(c_ids)

        input_ids = [a.id for a in articles]
        assert len(collected_ids) == len(input_ids), "Total partitioned IDs must match input count"
        assert set(collected_ids) == set(input_ids), "Every input article must be present in exactly one cluster"
        assert len(collected_ids) == len(set(collected_ids)), "No article ID may appear in multiple clusters"

# ============================================================================
# 5. Joint Events with Different Headlined Participants (Positive Co-Mention)
# ============================================================================

JOINT_EVENT_SCENARIOS = [
    (
        "{p1} Yalı Çapkını yeni sezon galasında kırmızı halıda",
        "{p1} ve {p2} yeni sezon galasında birlikte poz verdi.",
        "{p2} Yalı Çapkını yeni sezon galasında kırmızı halıda",
        "{p2} ve {p1} galada hayranlarıyla buluştu.",
        ("Afra Saraçoğlu", "Mert Ramazan Demir"),
    ),
    (
        "{p1} Comic-Con panelinde Deadpool & Wolverine sürprizini açıkladı",
        "{p1} ve {p2} sahnede hayranların sorularını yanıtladı.",
        "{p2} Comic-Con panelinde Deadpool & Wolverine sürprizini açıkladı",
        "{p2} ve {p1} sahnede bir araya gelerek yeni filmi tanıttı.",
        ("Ryan Reynolds", "Hugh Jackman"),
    ),
    (
        "{p1} ve {p2} Venedik Film Festivali'nde el ele kırmızı halıda",
        "İkili festival kapsamında yeni filmlerinin galasında boy gösterdi.",
        "{p2} ile {p1} Venedik Film Festivali kırmızı halısında kameralara poz verdi",
        "Festivalde birlikte görüntülenen çift dikkatleri üzerine çekti.",
        ("Dua Lipa", "Callum Turner"),
    ),
]


@HYPOTHESIS_SETTINGS
@given(
    scenario=st.sampled_from(JOINT_EVENT_SCENARIOS),
)
def test_property_joint_event_different_headlined_participants_cluster(
    scenario: tuple[str, str, str, str, tuple[str, str]],
):
    """
    Property: Two publishers reporting the exact same joint event where different
    participants are headlined or co-mentioned in title/snippet must cluster together
    and not be falsely separated due to different co-mentioned people.
    """
    t1_tmpl, s1_tmpl, t2_tmpl, s2_tmpl, (p1, p2) = scenario
    t1 = t1_tmpl.format(p1=p1, p2=p2)
    s1 = s1_tmpl.format(p1=p1, p2=p2)
    t2 = t2_tmpl.format(p1=p1, p2=p2)
    s2 = s2_tmpl.format(p1=p1, p2=p2)

    with tempfile.TemporaryDirectory() as td:
        engine, _ = _create_engine_with_temp_repo(td)

        a1 = Article.create(
            t1,
            "https://outlet-alpha.example.com/joint-event-1",
            "Outlet Alpha",
            FIXED_NOW,
            snippet=s1,
        )
        a2 = Article.create(
            t2,
            "https://outlet-beta.example.com/joint-event-2",
            "Outlet Beta",
            FIXED_NOW,
            snippet=s2,
        )

        results = engine.filter_fresh_candidates([a1, a2])

        assert len(results) == 1, (
            f"Joint event false-split: expected 1 cluster for co-mentioned participants {p1} and {p2}, "
            f"got {len(results)}. Titles: {t1!r} vs {t2!r}"
        )
        assert engine.last_clusters_count == 1
        cluster_ids = results[0].raw_metadata.get("cluster_article_ids", [])
        assert a1.id in cluster_ids
        assert a2.id in cluster_ids
