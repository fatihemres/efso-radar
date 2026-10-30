"""Deduplication and cross-publisher event clustering engine.

Provides multi-level filtering:
1. Exact ID & Canonical URL deduplication against processed history.
2. Cross-publisher title clustering to prevent flooding LLM with duplicates of the same event.
3. Priority scoring based on breaking pop-culture development keywords.
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime
from typing import Optional, Sequence
from difflib import SequenceMatcher
from urllib.parse import urlparse

from efso_radar.domain import Article, Market, normalize_text
from efso_radar.repositories.base import StateRepository

# High-signal pop culture keywords indicating new developments (Untuned Baseline set)
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
    "with", "from", "that", "this", "after", "over", "into", "about", "as", "by",
    "sonra", "önce", "kadar", "gibi", "daha", "çok", "yeni", "ilk", "son",
    "haber", "haberi", "haberleri", "detaylı", "detayli", "detay", "magazin",
    "özeti", "ozeti", "özet", "ozet",
}

# Lightweight, maintainable list of high-affinity Turkish & global entertainment properties/celebrities
HIGH_AFFINITY_ENTITIES: set[str] = {
    # Global Music, Pop & Culture Icons (High affinity for 18-35 TR audience)
    "taylor swift", "harry styles", "rihanna", "beyonce", "lady gaga", "the weeknd",
    "billie eilish", "sabrina carpenter", "dua lipa", "olivia rodrigo", "justin bieber",
    "hailey bieber", "selena gomez", "ariana grande", "miley cyrus", "lana del rey",
    "bad bunny", "drake", "kanye west", "eminem", "adele", "shakira", "jennifer lopez",
    "madonna", "britney spears", "barry manilow", "jack white", "raconteurs",
    "remi wolf", "fontaines", "kim kardashian", "kylie jenner", "kendall jenner",
    "bella hadid", "gigi hadid", "olivia culpo", "christian mccaffrey",
    "travis kelce", "cindy crawford", "blackpink", "bts", "ed sheeran",
    "charli xcx", "chappell roan", "cristiano ronaldo", "messi", "karim benzema",
    # Global Film / TV / Streaming Icons & Franchises
    "zendaya", "tom holland", "timothee chalamet", "keanu reeves", "scarlett johansson",
    "penelope cruz", "javier bardem", "john malkovich", "pedro pascal", "margot robbie",
    "cillian murphy", "jenna ortega", "robert downey", "brad pitt", "angelina jolie",
    "leonardo dicaprio", "johnny depp", "ryan gosling", "emma stone", "sydney sweeney",
    "jacob elordi", "paul mescal", "tanushree dutta",
    "harry potter", "wednesday", "stranger things", "squid game", "euphoria",
    "emily in paris", "bridgerton", "the last of us", "house of the dragon",
    "game of thrones", "avengers", "marvel", "star wars", "barbie", "oppenheimer",
    "grammy", "oscar", "oscars", "met gala", "cannes", "eurovision", "coachella",
    "victoria's secret", "netflix", "disney+", "hbo",
    # Turkish Stars, Pop Artists & Hit Series
    "cagatay ulusoy", "kivanc tatlitug", "serenay sarikaya", "hande ercel", "kerem bursin",
    "afra saracoglu", "mert ramazan demir", "dogukan gungor", "ceren karakoc",
    "kizilcik serbeti", "uzak sehir", "esref ruya", "bahar", "inci taneleri", "yali capkini",
    "eda ece", "ata demirer", "sila", "tarkan", "hadise", "gulsen", "zeynep bastik",
    "mabel matiz", "semicenk", "lvbel c5", "c5", "murda", "uzi", "cakal", "blok3",
    "derya ulug", "seda sayan", "acun ilicali", "sinan engin", "cagla sikel",
    "emre altug", "pinar altug", "yagmur atacan", "su atacan", "nukhet duru",
    "filiz akin", "ajda pekkan", "sezen aksu", "kenan imirzalioglu", "burak ozcivit",
    "fahriye evcen", "demet ozdemir", "oguzhan koc", "danla bilic", "cem yilmaz",
    "mert demir", "melis sezen", "aras bulut", "edis", "demet akalin", "cemre solmaz",
    "orkun isitmak", "reynmen", "meryem uzerli", "birce akalay", "pinar deniz",
}

ENTERTAINMENT_SIGNAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "birth_family",
        re.compile(
            r"\b(gives\s+birth|gave\s+birth|welcomes\s+baby|welcomes\s+first|welcomes\s+second|"
            r"second\s+baby|first\s+child|second\s+child|expecting\s+baby|pregnant|newborn|"
            r"dunyaya\s+getirdi|anne\s+oldu|baba\s+oldu|bebek|hamile|anneannesi|baba\s+ocagi\w*|"
            r"memleket\w*|ziyaret\s+etti|tatil\w*|sampiyon\w*|gurur\w*)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "viral_social",
        re.compile(
            r"\b(goes\s+viral|viral\s+tiktok|tiktok\s+fanpage|tiktok\s+trend|viral|tiktok|"
            r"fanpage|dancing\s+alone|gundem\s+oldu|ilk\s+kez\s+goruntulendi|ilk\s+goruntusu|"
            r"degisimi\s+olay|rekor\s+izlenme|rekor\s+kirdi|sosyal\s+medya\w*)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "music_stage",
        re.compile(
            r"\b(walks\s+off\s+stage|cancels\s+\w+|concert\s+cancellation|concert|reunite\w*|"
            r"reunion|comeback|new\s+album|new\s+single|new\s+song|world\s+tour|tour\w*|"
            r"konser\w*|album\w*|yeni\s+sarki|sahnelerden|sahne\w*|duet|turne\w*)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "film_tv_project",
        re.compile(
            r"\b(new\s+film|new\s+movie|new\s+series|new\s+partner|television\s+project|"
            r"tv\s+project|spinoff|spin\s+off|sequel|reboot|casting\s+announcement|casting|"
            r"cast\s+in|joins\s+cast|voiced|premiere|trailer|fragman|oscars?\s+race|oscars?|"
            r"dizi\w*|yeni\s+film|setinde|kamera\s+arkasi|aksiyon\s+yildiz\w*|opusme\s+sahnesi|"
            r"basrol\w*|kadro\w*|rol\w*|galasinda)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "fashion_beauty",
        re.compile(
            r"\b(haircuts?|hair\s+transformation|fashion\s+trend|fashion|beauty\s+trend|beauty|"
            r"style|instyle|red\s+carpet|celebrity\s+appearance|kirmizilar\s+icinde|"
            r"kirmizi\s+hali\w*|estetik\w*|yuz\s+gerdirme|bikinili\s+poz\w*|kombin\w*|"
            r"tarz\w*|moda|sac\s+kesim\w*)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "celebrity_statement_legal",
        re.compile(
            r"\b(clears\s+the\s+air|cheers\s+on|claims\s+insider|black\s+magic|kara\s+buyu|"
            r"evlendi|bosandi|ayrildi|nisanlandi|ask\s+yasiyor|sevgili\w*|acikladi|aciklamasi|"
            r"konustu|isyan\s+etti|itiraf|tepki|polemik|tutuklama\s+talebi|kitap\s+okuma\s+cezasi|"
            r"edebiyat\s+egitimi|karar\s+cikti|fizik\s+tedavi|birlikte\s+calismak)\b",
            re.IGNORECASE,
        ),
    ),
)

OUT_OF_SCOPE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "religion_clerical",
        re.compile(r"\b(pope\b|papal|vatican|vatikan|katolik\s+dunyasi|ruhani\s+lider|diyanet)\b", re.IGNORECASE),
    ),
    (
        "politics_policy",
        re.compile(
            r"\b(government\s+scientists|climate\s+change|presidential\s+run|2028\s+presidential|"
            r"election\s+poll|kabine\s+toplantisi|milletvekili|bakanlik|belediye\s+baskani|"
            r"merkez\s+bankasi|enflasyon|vergi\s+duzenlemesi|resmi\s+gazete)\b|/politics/",
            re.IGNORECASE,
        ),
    ),
    (
        "sports_watch_guide",
        re.compile(
            r"\b(where\s+to\s+watch|game\s+online|canli\s+mac\s+izle|puan\s+durumu|ilk\s+11|"
            r"teknik\s+direktor|super\s+lig|derbi\s+mac|antrenman)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "non_celebrity_blotter_or_geo",
        re.compile(
            r"\b(mauled\s+man\s+to\s+death|wildlife\s+park|uk\s+zoo|euthanized|"
            r"evin\s+bahcesine\s+kopruyle|168\s+metre\s+yukseklikte|zincirleme\s+trafik\s+kazasi|"
            r"trafik\s+kazasinda\s+hayatini|cinayet\s+sorusturmasinda|cezaevine\s+gonderildi|"
            r"metro\s+hatti\s+ihalesi|meteoroloji|hava\s+durumu|ceyrek\s+bilancosu|halka\s+arz)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "tragic_local_mortality",
        re.compile(
            r"\b(\d+\s*kattan\s+duserek\s+hayatini\s+kaybeden|kaybetmenin\s+tarifsiz\s+acisini|"
            r"amcasi\s+vefat\s+etti|cenaze\s+toreni\s+yarin)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "generic_essay_or_b2b",
        re.compile(
            r"\b(teach\s+you\s+about\s+second\s+acts|kisisel\s+gelisim\s+ve\s+ikinci\s+bahar|"
            r"reflects\s+on\s+animated\s+feature)\b",
            re.IGNORECASE,
        ),
    ),
)

# Clickbait subject-hiding patterns (e.g., "Ünlü oyuncu...", "İkinci çocuğunu dünyaya getirdi")
SUBJECTLESS_CLICKBAIT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(ünlü|meşhur|popüler|genç|usta|güzel|yakışıklı)\s+(oyuncu|şarkıcı|isim|çift|fenomen|sunucu)\b", re.IGNORECASE),
    re.compile(r"^(ikinci|ilk|üçüncü)\s+çocuğunu\s+dünyaya\s+getirdi$", re.IGNORECASE),
    re.compile(r"^[a-zçğıöşü]+'?daki\s+köyünü\s+ziyaret\s+etti$", re.IGNORECASE),
    re.compile(r"\b(kim olduğu merak konusu|görenler tanıyamadı|bakın kim çıktı)\b", re.IGNORECASE),
)

ENTERTAINMENT_PUBLISHER_HINTS: tuple[str, ...] = (
    "magazin", "variety", "hollywood reporter", "billboard", "rolling stone",
    "tmz", "people", "deadline", "elle", "vogue", "instyle", "gq", "cosmopolitan",
    "pitchfork", "nme", "e! online", "page six", "us weekly", "entertainment tonight",
    "habertürk", "haberturk", "hürriyet", "hurriyet", "milliyet", "sabah",
    "posta", "snob", "gazetemagazin", "birsen altuntaş", "birsen altuntas",
    "tv100", "ntv", "sözcü", "sozcu", "cumhuriyet", "onedio",
)


def _ascii_norm(text: str) -> str:
    """Normalize text and map Turkish characters to ASCII for resilient matching."""
    s = normalize_text(text)
    return (
        s.replace("ı", "i")
        .replace("ğ", "g")
        .replace("ü", "u")
        .replace("ş", "s")
        .replace("ö", "o")
        .replace("ç", "c")
    )


def extract_url_slug_text(url: str) -> str:
    """Extract informative entity/topic words from direct publisher URL paths."""
    if not url or "news.google.com" in url:
        return ""
    path = url.split("?", 1)[0]
    parts = re.split(r"[/\-_.]", path.lower())
    ignore = {
        "https", "http", "www", "com", "tr", "org", "net", "haber", "magazin",
        "foto", "kelebek", "televizyon", "hayat", "yasam", "galeri", "music",
        "news", "film", "global", "festivals", "markets", "politics", "features",
        "product", "recommendations", "lifestyle", "hurriyet", "pazar", "haberturk",
        "onedio", "variety", "rollingstone", "billboard", "tmz", "eonline", "ntv",
        "haberleri", "detay", "son", "dakika", "html",
    }
    tokens = [p for p in parts if len(p) >= 2 and not p.isdigit() and p not in ignore]
    return " ".join(tokens)


def _strip_tr_possessive(word: str) -> str:
    """Strip apostrophe suffixes (e.g., \"c5'in\" -> \"c5\", \"mendes'in\" -> \"mendes\")."""
    for sep in ("'", "’", "‘", "`"):
        if sep in word:
            base = word.split(sep, 1)[0]
            if len(base) >= 2:
                return base
    return word


def extract_key_tokens(title: str) -> set[str]:
    """Extract significant tokens and root stems from a title for entity/subject clustering."""
    raw_lower = title.lower()
    cleaned = normalize_text(title)
    tokens: set[str] = set()

    for raw_w in raw_lower.split():
        base = _strip_tr_possessive(raw_w.strip(".,!?;:\"'()[]{}"))
        norm_base = _ascii_norm(base)
        if norm_base and norm_base not in STOP_WORDS and (len(norm_base) > 2 or any(c.isdigit() for c in norm_base)):
            tokens.add(norm_base)
            if len(norm_base) >= 5:
                tokens.add(norm_base[:5])

    for w in cleaned.split():
        w_ascii = _ascii_norm(w)
        if w in STOP_WORDS or w_ascii in STOP_WORDS:
            continue
        if len(w_ascii) <= 2 and not any(c.isdigit() for c in w_ascii):
            continue
        tokens.add(w)
        tokens.add(w_ascii)
        if len(w) >= 5:
            tokens.add(w[:5])
        if len(w_ascii) >= 5:
            tokens.add(w_ascii[:5])
    return tokens


CALENDAR_MONTHS: set[str] = {
    # English
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    # Turkish
    "ocak", "subat", "mart", "nisan", "mayis", "haziran",
    "temmuz", "agustos", "eylul", "ekim", "kasim", "aralik",
}

GENERIC_HEADLINE_WORDS: set[str] = {
    "what", "watch", "where", "when", "who", "why", "how",
    "guide", "everything", "best", "every", "top", "all",
    "coming", "leaving", "streaming", "stream", "movies", "shows",
    "series", "films", "season", "episode", "release", "releases",
    "here", "full", "list", "lineup", "schedule", "dates", "date",
    "tv", "video", "photos", "photo", "exclusive", "first", "look",
}


def extract_proper_name_bigrams(text: str) -> set[str]:
    """Extract capitalized proper-name bigrams and individual proper nouns from raw text."""
    chunks = re.split(r"[:!?;,\.\n\(\)\[\]\"“”/]+", text)
    bigrams: set[str] = set()
    generic_starters = {
        "unlu", "meshur", "populer", "genc", "usta", "guzel", "yakisikli",
        "ikinci", "birinci", "yeni", "eski", "son", "ilk", "buyuk", "flas",
        "sonra", "once", "kattan", "duserek", "hayatini", "kaybeden",
        "gundem", "oldu", "hali", "sir", "dr", "prof", "lady", "lord",
    }
    for chunk in chunks:
        words = re.findall(r"\b[A-Za-zÇĞİÖŞÜa-zçğıöşü0-9\'-]+\b", chunk)
        for i in range(len(words) - 1):
            w1, w2 = words[i], words[i + 1]
            if not (w1 and w1[0].isupper() and w2 and w2[0].isupper()):
                continue
            if len(w1) < 2 or len(w2) < 2:
                continue
            if any(c.isdigit() for c in w1) or any(c.isdigit() for c in w2):
                continue
            a1 = _ascii_norm(_strip_tr_possessive(w1))
            a2 = _ascii_norm(_strip_tr_possessive(w2))
            if a1 in CALENDAR_MONTHS or a2 in CALENDAR_MONTHS:
                continue
            if (
                a1 not in generic_starters
                and a2 not in generic_starters
                and a1 not in STOP_WORDS
                and a2 not in STOP_WORDS
                and a1 not in GENERIC_HEADLINE_WORDS
                and a2 not in GENERIC_HEADLINE_WORDS
            ):
                bigrams.add(f"{a1} {a2}")

    all_words = re.findall(r"\b[A-Za-zÇĞİÖŞÜa-zçğıöşü0-9\'-]+\b", text)
    bigram_words = {tok for bg in bigrams for tok in bg.split()}
    for w in all_words:
        if not (w and w[0].isupper()):
            continue
        if len(w) < 3 or any(c.isdigit() for c in w):
            continue
        a = _ascii_norm(_strip_tr_possessive(w))
        if a in bigram_words:
            continue
        if a in CALENDAR_MONTHS or a in STOP_WORDS or a in generic_starters or a in GENERIC_HEADLINE_WORDS:
            continue
        bigrams.add(a)

    return bigrams


def extract_article_event_tokens(article: Article) -> set[str]:
    """
    Extract canonical event & subject tokens across title, snippet, and URL slug
    to cluster cross-publisher and bilingual coverage of the same event.
    """
    slug_text = extract_url_slug_text(article.canonical_url or article.url or "")
    combined_text = f"{article.title} {article.snippet or ''} {slug_text}"
    tokens = extract_key_tokens(combined_text)

    lower_ascii = _ascii_norm(combined_text)
    if any(p in lower_ascii for p in ("dunyaya getirdi", "anne oldu", "gives birth", "gave birth", "welcomes baby", "dogum yapti", "ikinci cocugunu", "ilk cocugunu", "second baby")):
        tokens.update({"_evt_birth_", "_evt_baby_", "birth", "bebek"})
    if any(p in lower_ascii for p in ("koyunu ziyaret", "bulgaristan", "memleketi", "baba ocagi")):
        tokens.update({"_evt_hometown_visit_", "bulgaristan", "koyun", "memleket"})
    if any(p in lower_ascii for p in ("adliye", "ifade verdi", "mahkeme", "dava", "sorusturma", "hapis", "gozalti", "tutuklama talebi", "kitap okuma", "edebiyat")):
        tokens.update({"_evt_legal_", "edebiyat", "kitap"})
    if any(p in lower_ascii for p in ("kattan duserek", "hayatini kaybeden", "vefat", "kaybetmenin tarifsiz")):
        tokens.update({"_evt_mortality_", "vefat"})
    if any(p in lower_ascii for p in ("evlendi", "nikah", "wedding", "married", "dunyaevine")):
        tokens.add("_evt_wedding_")
    if any(p in lower_ascii for p in ("bosandi", "ayrildi", "divorce", "breakup", "splits")):
        tokens.add("_evt_breakup_")

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


def find_matched_affinity_entities(text: str) -> list[str]:
    """Return high-affinity entities matched with strict word boundaries on ASCII-normalized text."""
    ascii_text = _ascii_norm(text)
    matched: list[str] = []
    for ent in HIGH_AFFINITY_ENTITIES:
        ent_ascii = _ascii_norm(ent)
        if re.search(rf"\b{re.escape(ent_ascii)}\b", ascii_text):
            matched.append(ent_ascii)
    return sorted(set(matched))


def headline_subject_clarity_score(article: Article) -> float:
    """
    Evaluate whether an article's headline clearly identifies both the subject (who)
    and the event (what happened), penalizing clickbait titles that hide the subject.
    """
    title = (article.title or "").strip()
    if not title:
        return 0.0

    score = 0.0
    title_entities = find_matched_affinity_entities(title)
    full_entities = find_matched_affinity_entities(
        f"{article.title} {article.snippet or ''} {extract_url_slug_text(article.canonical_url or article.url or '')}"
    )
    if title_entities:
        score += 3.0
    elif full_entities and len(title.split()) <= 5:
        # Entity exists in snippet/slug, but was hidden from the headline (e.g. CALIB-01, CALIB-07)
        score -= 2.5

    if len(title.split()) >= 4:
        score += 1.0
    if any(p.search(title) for p in SUBJECTLESS_CLICKBAIT_PATTERNS):
        score -= 3.5

    if article.market == Market.TR and score > 0:
        score += 0.5

    return round(score, 2)


def article_priority_score(article: Article) -> int:
    """Calculate heuristic editorial priority score for ordering candidates."""
    text = normalize_text(f"{article.title} {article.snippet}")
    score = 0
    words = set(text.split())
    for kw in DEVELOPMENT_BOOST_KEYWORDS:
        if kw in words:
            score += 1
    return score


def compute_candidate_priority_baseline(
    article: Article,
    cluster_size: int = 1,
    publisher_seen_count: int = 1,
    reference_time: Optional[datetime] = None,
) -> tuple[float, list[str]]:
    """
    Untuned Phase 3C-2A baseline candidate prioritization algorithm.
    Preserved intact for reproducible before/after calibration comparisons.
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
        reasons.append(f"signal_keywords({','.join(sorted(kw_hits)[:3])})")

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


def compute_candidate_priority(
    article: Article,
    cluster_size: int = 1,
    publisher_seen_count: int = 1,
    reference_time: Optional[datetime] = None,
) -> tuple[float, list[str]]:
    """
    Calibrated Phase 3C-2B deterministic candidate prioritization strategy.

    Key Improvements over Baseline:
    1. Eliminates systematic Google News RSS penalty (missing image_url / source_category)
       without fabricating metadata.
    2. Expands bilingual (EN + TR) entertainment event signal detection across title, snippet, and URL slug.
    3. Incorporates word-boundary high-affinity entertainment entities/franchises
       relevant to Turkish audiences without hardcoding calibration titles.
    4. Smooths publisher-frequency dampening so high-value stories from active feeds survive.
    5. Dampens out-of-scope tragedy/blotter/politics/sports noise and subjectless clickbait.
    """
    now = reference_time or article.discovered_at or article.published_at
    reasons: list[str] = []
    score = 0.0

    # 1. Recency (interval-resilient up to 3h-4h scan cycles)
    age_hours = max(0.0, (now - article.published_at).total_seconds() / 3600.0)
    if age_hours <= 3.0:
        score += 2.4
        reasons.append("recency_le_3h")
    elif age_hours <= 8.0:
        score += 2.0
        reasons.append("recency_le_8h")
    elif age_hours <= 16.0:
        score += 1.5
        reasons.append("recency_le_16h")
    elif age_hours <= 24.0:
        score += 1.1
        reasons.append("recency_le_24h")
    else:
        score += 0.4
        reasons.append("older_recency")

    title_ascii = _ascii_norm(article.title)
    snippet_ascii = _ascii_norm(article.snippet or "")
    slug_ascii = _ascii_norm(extract_url_slug_text(article.canonical_url or article.url or ""))
    full_ascii = f"{title_ascii} {snippet_ascii} {slug_ascii}".strip()

    # 2. Bilingual Entertainment & Pop-Culture Event Signals
    sig_hits: list[str] = []
    for label, pat in ENTERTAINMENT_SIGNAL_PATTERNS:
        if pat.search(full_ascii):
            sig_hits.append(label)
    if sig_hits:
        pts = min(3.2, len(sig_hits) * 1.15)
        score += pts
        reasons.append(f"signal_keywords({','.join(sig_hits)})")

    # 3. High-Affinity Audience Entities & Franchises
    ent_all = find_matched_affinity_entities(full_ascii)
    ent_title = find_matched_affinity_entities(title_ascii)
    if ent_all:
        ent_pts = 2.2 if len(ent_all) == 1 else 2.7
        score += ent_pts
        reasons.append(f"high_affinity_entity({','.join(ent_all[:2])})")

    # 4. Subject Clarity vs Anonymous Clickbait Headline
    title_words = title_ascii.split()
    if (ent_all and not ent_title and len(title_words) <= 4) or any(p.search(article.title) for p in SUBJECTLESS_CLICKBAIT_PATTERNS):
        score -= 1.5
        reasons.append("subjectless_clickbait_penalty")
    elif ent_title:
        score += 0.6
        reasons.append("explicit_subject_in_headline")

    # 5. Balanced Source Diversity
    if publisher_seen_count <= 3:
        score += 1.4
        reasons.append("source_diversity_top3")
    elif publisher_seen_count <= 6:
        score += 1.0
        reasons.append("source_diversity_top6")
    elif publisher_seen_count <= 9:
        score += 0.5
        reasons.append("source_diversity_top9")
    else:
        reasons.append("source_frequency_dampened")

    # 6. Category Relevance & Metadata Fairness (Google News RSS Parity)
    cat = (article.source_category or "").upper()
    pub_lower = (article.source or "").lower()
    is_fashion_trend = "fashion_beauty" in sig_hits or pub_lower in {"instyle", "vogue", "elle"}
    is_ent_pub = any(h in pub_lower for h in ENTERTAINMENT_PUBLISHER_HINTS)

    if cat in ("MAGAZIN_TR", "VIRAL_INTERNET_TR", "MUSIC_TR"):
        score += 1.8
        reasons.append("core_turkish_entertainment")
    elif cat in (
        "FILM_TV_GLOBAL", "MUSIC_GLOBAL", "HOLLYWOOD_GLOBAL",
        "VIRAL_CELEBRITY_GLOBAL", "VIRAL_INTERNET_GLOBAL",
        "MUSIC_CULTURE_GLOBAL", "CELEBRITY_LIFESTYLE_GLOBAL",
    ):
        score += 1.6
        reasons.append("core_global_entertainment")
    elif is_fashion_trend:
        score += 1.6
        reasons.append("fashion_beauty_trend_vertical")
    elif not cat and (sig_hits or ent_all or is_ent_pub):
        score += 1.6
        reasons.append("inferred_entertainment_vertical")

    if article.image_url:
        score += 0.3
        reasons.append("has_visual_image")
    elif "news.google.com" in (article.url or "") or len(article.snippet or "") >= 50:
        score += 0.3
        reasons.append("syndicated_metadata_parity")

    if len(article.snippet or "") >= 60:
        score += 0.4
        reasons.append("informative_snippet")

    # 7. Cross-Publisher Consensus
    if cluster_size >= 3:
        score += 1.8
        reasons.append("cross_publisher_consensus(3+)")
    elif cluster_size == 2:
        score += 1.0
        reasons.append("cross_publisher_consensus(2)")

    # 8. Out-of-Scope / Hard-News / Tragic Blotter Dampening
    if cat in ("OUT_OF_SCOPE_RELIGION", "OUT_OF_SCOPE_POLITICS", "OUT_OF_SCOPE_SPORTS", "OUT_OF_SCOPE_CRIME", "OUT_OF_SCOPE_ESSAY"):
        score -= 3.8
        reasons.append("out_of_scope_or_grim_news_dampened")
    elif cat == "OUT_OF_SCOPE_LIFESTYLE" and not is_fashion_trend:
        score -= 3.2
        reasons.append("out_of_scope_or_grim_news_dampened")

    for pen_label, pen_pat in OUT_OF_SCOPE_PATTERNS:
        if pen_pat.search(full_ascii) or pen_pat.search(article.url or ""):
            score -= 3.8
            reasons.append(f"out_of_scope_or_grim_news_dampened({pen_label})")
            break

    if cat == "RUMOR_UNVERIFIED" and not ent_all:
        score -= 1.0
        reasons.append("unverified_low_affinity_rumor")

    return round(max(0.5, score), 2), reasons


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
        similarity_threshold: float = 0.45,
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

        # Level 2: Cross-publisher & bilingual event cluster collapsing
        clusters: list[list[Article]] = []
        cluster_title_tokens: list[set[str]] = []
        cluster_event_tokens: list[set[str]] = []
        cluster_entities: list[set[str]] = []

        for article in fresh_unprocessed:
            t_tokens = extract_key_tokens(article.title)
            e_tokens = extract_article_event_tokens(article)
            full_context = f"{article.title} {article.snippet or ''} {extract_url_slug_text(article.canonical_url or article.url or '')}"
            ents = set(find_matched_affinity_entities(full_context)) | extract_proper_name_bigrams(f"{article.title}. {article.snippet or ''}")
            matched_cluster_idx = -1

            for idx in range(len(clusters)):
                title_overlap = calculate_token_overlap(t_tokens, cluster_title_tokens[idx])
                event_overlap = calculate_token_overlap(e_tokens, cluster_event_tokens[idx])
                char_sim = SequenceMatcher(
                    None, normalize_text(article.title), normalize_text(clusters[idx][0].title)
                ).ratio()

                shared_entities = ents & cluster_entities[idx]

                # Check conflicting multiword person names sharing a token (e.g. "Ryan Reynolds" vs "Ryan Gosling")
                multi_a = {e for e in ents if len(e.split()) >= 2}
                multi_b = {e for e in cluster_entities[idx] if len(e.split()) >= 2}
                shared_multi = multi_a & multi_b
                shared_multi_tokens = {tok for sm in shared_multi for tok in sm.split()}
                conflicting_person_names = False
                for ma in multi_a:
                    words_a = set(ma.split())
                    for mb in multi_b:
                        if ma != mb:
                            words_b = set(mb.split())
                            shared_words = words_a & words_b
                            # Shared name token is not conflicting if it belongs to a full identity shared by both
                            if shared_words & shared_multi_tokens:
                                continue
                            diff_a = words_a - words_b
                            diff_b = words_b - words_a
                            if shared_words and diff_a and diff_b:
                                if ma not in multi_b and mb not in multi_a:
                                    conflicting_person_names = True
                                    break
                    if conflicting_person_names:
                        break

                # Check shared individual name tokens to avoid false disjoint classification on partial name matches
                ent_toks_a = {tok for e in ents for tok in e.split() if len(tok) >= 3}
                ent_toks_b = {tok for e in cluster_entities[idx] for tok in e.split() if len(tok) >= 3}
                shared_name_tokens = ent_toks_a & ent_toks_b

                disjoint_named_entities = (
                    conflicting_person_names
                    or (
                        bool(ents)
                        and bool(cluster_entities[idx])
                        and not shared_entities
                        and not shared_name_tokens
                    )
                )
                shared_semantic_tags = {t for t in (e_tokens & cluster_event_tokens[idx]) if t.startswith("_evt_")}

                # Compute non-entity event token overlap so sharing only a celebrity's name
                # across two unrelated stories does not inflate event_overlap.
                entity_word_tokens: set[str] = set()
                for ent_name in (shared_entities | shared_name_tokens):
                    entity_word_tokens.update(extract_key_tokens(ent_name))
                non_ent_a = {t for t in e_tokens if t not in entity_word_tokens and not t.startswith("_evt_")}
                non_ent_b = {t for t in cluster_event_tokens[idx] if t not in entity_word_tokens and not t.startswith("_evt_")}
                non_entity_overlap = calculate_token_overlap(non_ent_a, non_ent_b)

                if disjoint_named_entities:
                    # Articles naming completely different people/entities only merge on near-exact title match (e.g. minor typo)
                    is_same_event = char_sim >= 0.92
                else:
                    is_same_event = (
                        (title_overlap >= similarity_threshold and (not shared_entities or non_entity_overlap >= 0.25))
                        or char_sim >= 0.72
                        or (bool(shared_entities) and bool(shared_semantic_tags))
                        or (bool(shared_entities) and non_entity_overlap >= 0.30)
                    )

                if is_same_event:
                    matched_cluster_idx = idx
                    break

            if matched_cluster_idx >= 0:
                clusters[matched_cluster_idx].append(article)
                cluster_title_tokens[matched_cluster_idx].update(t_tokens)
                cluster_event_tokens[matched_cluster_idx].update(e_tokens)
                cluster_entities[matched_cluster_idx].update(ents)
            else:
                clusters.append([article])
                cluster_title_tokens.append(set(t_tokens))
                cluster_event_tokens.append(set(e_tokens))
                cluster_entities.append(set(ents))

        # Select the best canonical representative for each cluster (preferring clear subject + event headline)
        representatives: list[Article] = []
        now_ref = max((a.published_at for a in fresh_unprocessed), default=None)

        for cluster in clusters:
            best_article = max(
                cluster,
                key=lambda a: (
                    headline_subject_clarity_score(a),
                    compute_candidate_priority(a, cluster_size=len(cluster), reference_time=now_ref)[0],
                    article_priority_score(a),
                    a.published_at.timestamp(),
                ),
            )
            best_article_dict = best_article.model_dump()
            best_article_dict["raw_metadata"]["cluster_article_ids"] = [a.id for a in cluster]
            best_article_dict["raw_metadata"]["cluster_size"] = len(cluster)
            best_article_dict["raw_metadata"]["cluster_publishers"] = list(dict.fromkeys(a.source for a in cluster))
            best_article_dict["raw_metadata"]["cluster_headlines"] = [a.title for a in cluster[:4]]
            representatives.append(Article(**best_article_dict))

        # Sort initially by recency to calculate publisher frequency fairly
        representatives.sort(key=lambda a: a.published_at, reverse=True)

        publisher_counts: dict[str, int] = defaultdict(int)
        scored_reps: list[tuple[float, Article]] = []

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
