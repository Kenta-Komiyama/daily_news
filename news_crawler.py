# -*- coding: utf-8 -*-
"""
ニュース一覧から「記事ページ」だけを正しくたどり、
過去 LOOKBACK_HOURS（JST）に公開された記事の本文抽出＋要約（全件）を CSV/Markdown に保存します。

✅ この版で解決していること
- 元々の取得先（HTML一覧）は残したまま（消えない）
- Business Insider の記事が一覧で時刻を拾えず 0 件になる問題を修正
  - ALLOW_NO_LIST_TIME に businessinsider.jp を追加（一覧時刻が無くても候補に残す）
  - カード全体/リンクテキストも meta 候補に追加して時刻を拾いやすく
- Zenn / Qiita は RSS/Atom から取得（JS依存・DOM変更に強い）
- Zennの日本語URLで落ちる（latin-1 codec）を修正（Refererを送らない＋URL ASCII化）
- OpenAI RSS の XML パース失敗（BOM/制御文字/HTML返却）耐性（ET→BS(xml)フォールバック）
- OpenAI / Microsoft / Google / Anthropic など公式/準公式も追加
- 同じtitleは重複削除（候補段階 + 結果段階）
"""

import os, re, time, datetime as dt, sys, logging, json
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import (
    urljoin, urlparse, parse_qsl, urlencode,
    urlsplit, urlunsplit, quote
)

import requests
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import tz
from dateutil.parser import parse as dtparse
import xml.etree.ElementTree as ET

# ===== ログ・標準出力エンコーディング =====
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
for name in ["trafilatura", "trafilatura.core", "trafilatura.utils"]:
    logging.getLogger(name).setLevel(logging.ERROR)

# ===== 基本設定 =====
JST = tz.gettz("Asia/Tokyo")
NOW = dt.datetime.now(JST)
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))  # 24h
THRESHOLD = NOW - dt.timedelta(hours=LOOKBACK_HOURS)
TIMEOUT = 25

SLEEP_LIST = float(os.environ.get("SLEEP_LIST", "0.35"))       # 一覧ページ間待機
SLEEP_FEED = float(os.environ.get("SLEEP_FEED", "0.20"))       # フィード間待機
SLEEP_ARTICLE = float(os.environ.get("SLEEP_ARTICLE", "0.55")) # 記事ページ間待機
ARTICLE_CHARS_LIMIT = int(os.environ.get("ARTICLE_CHARS_LIMIT", "9000"))
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
OUT_DIR = os.environ.get("OUT_DIR", "out")

os.makedirs(OUT_DIR, exist_ok=True)
DATE_PREFIX = NOW.strftime("%Y-%m-%d")
os.environ["DATE_PREFIX"] = DATE_PREFIX
CSV_PATH = os.path.join(OUT_DIR, f"{DATE_PREFIX}_news_{LOOKBACK_HOURS}h_fulltext.csv")
MD_PATH  = os.path.join(OUT_DIR, f"{DATE_PREFIX}_news_{LOOKBACK_HOURS}h_fulltext.md")

# ===== ユーティリティ：ホスト名正規化 / URL正規化 / タイトル正規化 =====
def norm_host(host: str) -> str:
    h = (host or "").lower()
    if h.startswith("www."):
        h = h[4:]
    return h

def normalize_url(u: str) -> str:
    """utm等の追跡クエリやMedium特有の?sk=...等を除去"""
    try:
        p = urlparse(u)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith(("utm_", "ref", "source", "mkt_tok"))]
        q = [(k, v) for k, v in q if k.lower() not in {"sk"}]
        new = p._replace(query=urlencode(q, doseq=True))
        s = new.geturl()
        s = re.sub(r"\?+$", "", s)
        return s
    except Exception:
        return u

def normalize_title(t: str) -> str:
    """同一タイトル重複排除用：大小/空白/記号ゆらぎをざっくり潰す"""
    s = (t or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[’'\"“”‘’`´]", "", s)
    s = re.sub(r"[：:｜|・•∙●◦]", " ", s)
    s = re.sub(r"[（）()【】\[\]{}<>「」『』]", " ", s)
    s = re.sub(r"[!！?？,，.。/／\\\-—–_]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def ascii_url(u: str) -> str:
    """日本語など非ASCIIを含むURLをrequestsで安全に扱える形にする"""
    p = urlsplit(u)
    path = quote(p.path, safe="/%")
    query = quote(p.query, safe="=&%")
    frag = quote(p.fragment, safe="%")
    return urlunsplit((p.scheme, p.netloc, path, query, frag))

# ===== 対象「一覧」ページ（元のまま維持 + 追加） =====
TARGET_LIST_PAGES = [
    # --- 元の取得先（維持） ---
    "https://business.nikkei.com/latest/?i_cid=nbpnb_latest",
    "https://www.businessinsider.jp/category/business/",
    "https://www.businessinsider.jp/category/tech-news/",
    "https://www.businessinsider.jp/category/science/",
    "https://www.businessinsider.jp/tag/start-up/",
    "https://xtech.nikkei.com/top/it/",
    "https://www.itmedia.co.jp/aiplus/spv/",
    "https://www.techno-edge.net/special/557/recent/%E7%94%9F%E6%88%90AI%E3%82%A6%E3%82%A3%E3%83%BC%E3%82%AF%E3%83%AA%E3%83%BC",
    "https://b.hatena.ne.jp/hotentry/it",
    "https://b.hatena.ne.jp/entrylist/it/AI%E3%83%BB%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92",
    "https://zenn.dev/topics/%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92",
    "https://zenn.dev/topics/ai",
    "https://zenn.dev/topics/deeplearning",
    "https://zenn.dev/topics/nlp",
    "https://zenn.dev/topics/python",
    "https://news.microsoft.com/source/topics/ai/",
    "https://huggingface.co/blog",
    "https://ai-scholar.tech/",
    "https://competition-content.signate.jp/articles",
    "https://www.kaggle.com/blog?sort=hotness",
    "https://www.kdnuggets.com/news/index.html",
    "https://www.kdnuggets.com/tag/artificial-intelligence",
    "https://www.kdnuggets.com/tag/computer-vision",
    "https://www.kdnuggets.com/tag/data-science",
    "https://www.kdnuggets.com/tag/machine-learning",
    "https://www.kdnuggets.com/tag/natural-language-processing",
    "https://www.kdnuggets.com/tag/python",
    "https://www.kdnuggets.com/tag/career-advice",
    "https://www.kdnuggets.com/tags/data-engineering",
    "https://www.kdnuggets.com/tag/language-models",
    "https://www.kdnuggets.com/tag/mlops",
    "https://www.kdnuggets.com/tag/programming",
    "https://www.kdnuggets.com/tag/sql",
    "https://towardsdatascience.com/latest/",
    "https://towardsdatascience.com/tag/editors-pick/",
    "https://towardsdatascience.com/tag/deep-dives/",
    "https://www.analyticsvidhya.com/blog-archive/",
    "https://codezine.jp/data/",
    "https://codezine.jp/case/",
    "https://www.publickey1.jp/",

    # --- 追加（公式/準公式 AI ニュース） ---
    "https://www.microsoft.com/en-us/ai/blog/",
    "https://www.anthropic.com/news",
    "https://blog.google/technology/ai/",
    "https://blog.google/technology/google-deepmind/",
    "https://deepmind.google/blog/",
    "https://research.google/blog/",
]

# ===== 追加：RSS/Atom（Zenn/Qiita/OpenAI など） =====
ZENN_TOPICS = ["ai", "deeplearning", "nlp", "python", "機械学習"]
QIITA_TAGS = ["ai", "machinelearning", "deeplearning", "nlp", "python", "llm", "生成ai"]

FEED_URLS: List[Dict] = []
FEED_URLS.append({"url": "https://openai.com/news/rss.xml", "source": "openai_news_rss"})
for tp in ZENN_TOPICS:
    FEED_URLS.append({"url": f"https://zenn.dev/topics/{tp}/feed", "source": f"zenn_topic:{tp}"})
for tg in QIITA_TAGS:
    FEED_URLS.append({"url": f"https://qiita.com/tags/{tg}/feed.atom", "source": f"qiita_tag:{tg}"})
FEED_URLS.append({"url": "https://qiita.com/popular-items/feed.atom", "source": "qiita_popular"})

# ===== OpenAI（要約） =====
client = None
try:
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        client = OpenAI(api_key=api_key)
    else:
        print("[info] OPENAI_API_KEY 未設定：OpenAI要約はスキップし、ローカル要約を使用します。")
except Exception as e:
    print(f"[warn] OpenAI 初期化失敗: {e} -> ローカル要約使用")

# ===== HTTPユーティリティ =====
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

def req(url: str, accept_xml: bool = False) -> Optional[requests.Response]:
    """
    403対策でUAを変えて一度だけ再試行。
    非ASCII URLも安全に処理（latin-1 ヘッダ事故回避）:
      - Referer を送らない
      - URL をASCII化(パーセントエンコード)してから requests
    """
    try:
        safe_url = ascii_url(url)

        headers = DEFAULT_HEADERS.copy()
        if accept_xml:
            headers["Accept"] = "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7"

        r = requests.get(safe_url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 403:
            headers["User-Agent"] = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            r = requests.get(safe_url, headers=headers, timeout=TIMEOUT)

        if r.status_code != 200:
            print(f"[warn] HTTP {r.status_code}: {url}")
            return None

        return r
    except Exception as e:
        print(f"[error] request failed: {url} -> {e}")
        return None

def soup_from_response(r: requests.Response) -> BeautifulSoup:
    try:
        if not r.encoding or r.encoding.lower() in ("ascii", "latin-1"):
            r.encoding = r.apparent_encoding or "utf-8"
        html = r.text
        if not html or len(html) < 100:
            html = r.content.decode(r.apparent_encoding or "utf-8", errors="replace")
    except Exception:
        html = r.content.decode("utf-8", errors="replace")
    return BeautifulSoup(html, "lxml")

def tx(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""

# ===== 日付パース =====
REL = [
    (re.compile(r"(\d+)\s*分前"), "minutes"),
    (re.compile(r"(\d+)\s*時間前"), "hours"),
    (re.compile(r"(\d+)\s*日前"), "days"),
    (re.compile(r"(\d+)\s*mins?\s*ago", re.I), "minutes"),
    (re.compile(r"(\d+)\s*minutes?\s*ago", re.I), "minutes"),
    (re.compile(r"(\d+)\s*hours?\s*ago", re.I), "hours"),
    (re.compile(r"yesterday", re.I), "yesterday"),
]
ABS = [
    re.compile(r"(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})"),
    re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日"),
    re.compile(r"\b(\d{1,2})/(\d{1,2})\b"),
    re.compile(r"\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})"),
    re.compile(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})"),
]
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"], 1)}

def parse_datetime_text(s: str, base: dt.datetime) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None

    # RSS/Atomの日時などは dateutil で先に試す
    try:
        d = dtparse(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=JST)
        return d.astimezone(JST)
    except Exception:
        pass

    for pat, unit in REL:
        m = pat.search(s)
        if m:
            if unit == "yesterday":
                return base - dt.timedelta(days=1)
            n = int(m.group(1))
            return base - (dt.timedelta(minutes=n) if unit=="minutes"
                           else dt.timedelta(hours=n) if unit=="hours"
                           else dt.timedelta(days=n))

    for pat in ABS[:2]:
        m = pat.search(s)
        if m:
            y, mo, d = map(int, m.groups())
            try: return dt.datetime(y, mo, d, tzinfo=JST)
            except: pass

    m = ABS[2].search(s)
    if m:
        mo, d = map(int, m.groups())
        try: return dt.datetime(NOW.year, mo, d, tzinfo=JST)
        except: pass

    m = ABS[3].search(s)
    if m:
        mon = MONTHS.get(m.group(1).lower(), 0)
        d = int(m.group(2)); y = int(m.group(3))
        if mon:
            try: return dt.datetime(y, mon, d, tzinfo=JST)
            except: pass

    m = ABS[4].search(s)
    if m:
        d = int(m.group(1)); mon = MONTHS.get(m.group(2).lower(), 0); y = int(m.group(3))
        if mon:
            try: return dt.datetime(y, mon, d, tzinfo=JST)
            except: pass

    m = re.search(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(:\d{2})?(Z|[+\-]\d{2}:\d{2})?", s)
    if m:
        y, mo, d = map(int, m.group(1).split("-"))
        hh, mm = map(int, m.group(2).split(":"))
        try:
            return dt.datetime(y, mo, d, hh, mm, 0, tzinfo=JST)
        except:
            pass

    return None

def within_lookback(d: Optional[dt.datetime]) -> bool:
    return bool(d and d >= THRESHOLD)

def any_within(texts: List[str]) -> Tuple[bool, Optional[dt.datetime], str]:
    best_dt, src = None, ""
    for s in texts:
        cand = parse_datetime_text(s, NOW)
        if cand and within_lookback(cand):
            if (best_dt is None) or (cand > best_dt):
                best_dt, src = cand, s
    return (best_dt is not None), best_dt, src

# ===== 記事URL判定 =====
SITE_RULES = {
    "businessinsider.jp": {"include": [r"/post-\d+"], "exclude": [r"/author/", r"/category/", r"/tag/"]},
    "business.nikkei.com": {"include": [r"/atcl/"], "exclude": [r"/author/", r"/category/", r"/tag/"]},
    "xtech.nikkei.com": {"include": [r"/atcl/"], "exclude": [r"/author/", r"/category/", r"/tag/"]},
    "itmedia.co.jp": {"include": [r"/aiplus/articles/"], "exclude": [r"/author/", r"/rsslist", r"/category/", r"/tag/"]},
    "techno-edge.net": {"include": [r"/\d{4}/\d{2}/\d{2}/", r"/article/"], "exclude": [r"/tag/", r"/category/", r"/author/"]},
    "b.hatena.ne.jp": {"hatena_special": True},
    "zenn.dev": {"include": [r"^/[^/]+/articles/[^/]+/?$", r"^/articles/[^/]+/?$"], "exclude": [r"^/users?/", r"^/topics/", r"^/books/", r"^/scraps/", r"^/tags?/"]},
    "openai.com": {"include": [r"^/news/"], "exclude": [r"/team/", r"/researchers/", r"/about/"]},
    "news.microsoft.com": {"include": [r"^/source/"], "exclude": [r"/people/", r"/about/"]},
    "microsoft.com": {"include": [r"^/en-us/ai/blog/"], "exclude": []},
    "huggingface.co": {"include": [r"/blog/"], "exclude": [r"/authors?/"]},
    "ai-scholar.tech": {"include": [r"/ai_news/", r"/ai_trends/", r"/ai_book/", r"/ai_scholar/", r"/article/"], "exclude": [r"/category/", r"/tag/", r"/author/"]},
    "competition-content.signate.jp": {"include": [r"^/articles/[^/]+/?$"], "exclude": [r"/users?/", r"/tags?/"]},
    "kaggle.com": {"include": [r"^/blog/[^/?#]+/?$"], "exclude": []},
    "kdnuggets.com": {"include": [r"^/\d{4}/\d{2}/[^/][^?#]*(?:\.html)?/?(?:\?.*)?$"], "exclude": [r"^/tag/", r"^/tags?/"]},
    "towardsdatascience.com": {"include": [], "exclude": []},
    "medium.com": {"include": [r"^/towards-data-science/[^/]+-[0-9a-fA-F]{12}$", r"^/p/[0-9a-fA-F]{12}$"], "exclude": [r"^/tag/", r"/about/"]},
    "analyticsvidhya.com": {"include": [r"^/blog/\d{4}/\d{2}/[^/].*"], "exclude": [r"/category/", r"/tag/"]},
    "codezine.jp": {"include": [r"^/article/detail/\d+\.html$"], "exclude": [r"^/category/", r"^/tag/"]},
    "publickey1.jp": {"include": [r"^/blog/\d{4}/\d{2}/[^/].*\.html$"], "exclude": []},
    "anthropic.com": {"include": [r"^/news/"], "exclude": [r"/careers", r"/policy", r"/research"]},
    "blog.google": {"include": [r"^/technology/"], "exclude": [r"/about/"]},
    "deepmind.google": {"include": [r"^/blog/"], "exclude": []},
    "research.google": {"include": [r"^/blog/"], "exclude": []},
}
COMMON_EXCLUDES = [r"/author/", r"/users?/", r"/tag/", r"/category/", r"/topics/", r"/people/"]
ZENN_ARTICLE_RE = re.compile(r"^/[^/]+/articles/[^/]+/?$")
ZENN_USERLESS_RE = re.compile(r"^/articles/[^/]+/?$")
KDN_ARTICLE_RE  = re.compile(r"^/\d{4}/\d{2}/[^/][^?#]*(?:\.html)?/?(?:\?.*)?$")

# クロスホスト許可
CROSS_HOST_ALLOW: Dict[str, Set[str]] = {
    "towardsdatascience.com": {"towardsdatascience.com", "medium.com"},
}

# 一覧に時刻が無くても候補に残す
ALLOW_NO_LIST_TIME = {
    "kdnuggets.com",
    "towardsdatascience.com",
    "medium.com",
    "analyticsvidhya.com",
    "kaggle.com",
    "openai.com",
    "news.microsoft.com",
    "huggingface.co",
    "zenn.dev",
    "business.nikkei.com",
    "xtech.nikkei.com",
    "microsoft.com",
    "anthropic.com",
    "blog.google",
    "deepmind.google",
    "research.google",
    "businessinsider.jp",  # ★追加：BIは一覧で時刻が取りにくいので記事側で判定
}

def score_link_by_rules(href: str, base_host_raw: str) -> int:
    p = urlparse(href)
    if not p.scheme.startswith("http"):
        return -999
    path = p.path or "/"
    base_host = norm_host(base_host_raw)
    score = 0

    for pat in COMMON_EXCLUDES:
        if re.search(pat, path):
            score -= 100

    rules_host = base_host
    rules = SITE_RULES.get(rules_host, {})

    for pat in rules.get("include", []):
        if re.search(pat, path):
            score += 20
    for pat in rules.get("exclude", []):
        if re.search(pat, path):
            score -= 100

    if re.search(r"[0-9a-fA-F]{12}$", path):
        score += 3

    score += min((path.strip("/").count("/")), 4)
    return score

def pick_article_anchor(card, base_url: str) -> Optional[str]:
    base_host_raw = urlparse(base_url).netloc
    base_host = norm_host(base_host_raw)

    # Hatena: 外部記事は a.entry-link 優先
    if SITE_RULES.get(base_host, {}).get("hatena_special"):
        a = card.select_one("a.entry-link[href]")
        if a:
            return normalize_url(urljoin(base_url, a["href"]))

    anchors = card.find_all("a", href=True)
    if not anchors:
        return None

    # Zenn: 厳格＋短縮
    if base_host == "zenn.dev":
        for a in anchors:
            href = normalize_url(urljoin(base_url, a["href"]))
            path = urlparse(href).path or "/"
            if ZENN_ARTICLE_RE.match(path) or ZENN_USERLESS_RE.match(path):
                return href
        return None

    # KDnuggets: 年/月/slug
    if base_host == "kdnuggets.com":
        for a in anchors:
            href = normalize_url(urljoin(base_url, a["href"]))
            path = urlparse(href).path or "/"
            if KDN_ARTICLE_RE.match(path) and not re.search(r"^/tag/|^/tags?/", path):
                return href
        return None

    # 日経は /atcl/ を最優先
    if base_host in {"business.nikkei.com", "xtech.nikkei.com"}:
        for a in anchors:
            href = normalize_url(urljoin(base_url, a["href"]))
            path = urlparse(href).path or "/"
            if re.search(r"/atcl/", path) and not any(re.search(pat, path) for pat in COMMON_EXCLUDES):
                return href

    # BIは /post- を優先
    if base_host == "businessinsider.jp":
        for a in anchors:
            href = normalize_url(urljoin(base_url, a["href"]))
            if re.search(r"/post-\d+", urlparse(href).path or ""):
                return href

    # 一般スコアリング
    best_href, best_score = None, -10**9
    for a in anchors:
        href = normalize_url(urljoin(base_url, a["href"]))
        if href.startswith(("mailto:", "tel:", "#")):
            continue
        score = score_link_by_rules(href, base_host_raw)

        a_text = (a.get_text(" ", strip=True) or "").lower()
        a_cls = " ".join(a.get("class", [])).lower()
        for kw in ["author", "プロフィール", "筆者", "投稿者", "users", "タグ", "category", "topics"]:
            if kw in a_text or kw in a_cls:
                score -= 30
        for good in ["title", "headline", "entry-title", "news-title", "permalink"]:
            if good in a_cls:
                score += 10
        if a.has_attr("rel") and "permalink" in [x.lower() for x in a["rel"]]:
            score += 10

        if score > best_score:
            best_score, best_href = score, href

    if best_href:
        path = urlparse(best_href).path or "/"
        for pat in COMMON_EXCLUDES:
            if re.search(pat, path):
                return None
    return best_href

# ===== 一覧抽出（HTML） =====
def extract_list_candidates(url: str, allowed_hosts: Optional[Set[str]] = None) -> List[Dict]:
    r = req(url)
    if not r:
        return []
    soup = soup_from_response(r)
    items, seen = [], set()
    base_host_raw = urlparse(url).netloc
    base_host = norm_host(base_host_raw)

    allowed_norm = {norm_host(h) for h in allowed_hosts} if allowed_hosts is not None else None

    candidates = soup.select("article, li, div, section, dd")
    if not candidates:
        candidates = soup.find_all(True)

    def push_card(card):
        link = pick_article_anchor(card, url)
        if not link:
            return

        link_host_norm = norm_host(urlparse(link).netloc)
        if allowed_norm is not None and link_host_norm not in allowed_norm:
            if not SITE_RULES.get(base_host, {}).get("hatena_special"):
                return

        # タイトル
        title_el = None
        for sel in ["a h1", "a h2", "a h3", "h1 a", "h2 a", "h3 a", "h1", "h2", "h3"]:
            title_el = card.select_one(sel)
            if title_el: break
        title = tx(title_el) if title_el else tx(card)
        if not title:
            try:
                title = tx(card.find("a", href=True))
            except Exception:
                title = link

        # 一覧側の時刻（粗判定）
        meta = []
        meta += [tx(x) for x in card.select("time")]
        for cls in ["time","date","timestamp","modDate","update","c-article__time","c-card__time","pubdate"]:
            el = card.find(class_=cls)
            if el:
                meta.append(tx(el))

        # ★追加：リンクテキスト（BIで時刻がここに入ることがある）
        try:
            a = card.find("a", href=True)
            if a:
                meta.append(tx(a))
        except Exception:
            pass

        # ★追加：カード全体テキスト（保険）
        meta.append(tx(card))

        # 既存：先頭の要素も
        meta += [tx(x) for x in card.find_all(["span","small","p"], limit=3)]

        ok, dttm, src = any_within(meta)
        if not ok and base_host not in ALLOW_NO_LIST_TIME:
            return

        key = (title.strip(), normalize_url(link))
        if key in seen:
            return
        seen.add(key)
        items.append({
            "source_list": url,
            "title": title.strip(),
            "link": normalize_url(link),
            "list_time_guess": dttm.isoformat() if ok and dttm else "",
            "list_time_raw": src if ok else "",
        })

    for card in candidates:
        push_card(card)

    if not items:
        for a in soup.find_all("a", href=True):
            faux = soup.new_tag("div")
            a_parent = a.parent or faux
            try:
                a_parent.append(a)
            except Exception:
                pass
            push_card(a_parent)

    return items

def collect_from_list(url: str) -> List[Dict]:
    base_host_raw = urlparse(url).netloc
    base_host = norm_host(base_host_raw)

    allowed: Optional[Set[str]] = {base_host}
    if SITE_RULES.get(base_host, {}).get("hatena_special"):
        allowed = None
    if base_host in CROSS_HOST_ALLOW:
        allowed = CROSS_HOST_ALLOW[base_host]
    return extract_list_candidates(url, allowed_hosts=allowed)

# ===== RSS/Atom パース（壊れ対策込み） =====
def _parse_root_to_items(root: ET.Element) -> List[Dict]:
    def strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    items: List[Dict] = []

    if strip_ns(root.tag).lower() in ("rss", "rdf", "rdf:rdf"):
        channel = None
        for ch in list(root):
            if strip_ns(ch.tag).lower() == "channel":
                channel = ch
                break
        if channel is None:
            channel = root
        for it in channel.findall(".//item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub = (it.findtext("pubDate") or it.findtext("date") or "").strip()
            if not link:
                guid = (it.findtext("guid") or "").strip()
                if guid.startswith("http"):
                    link = guid
            if title and link:
                items.append({"title": title, "link": normalize_url(link), "published_raw": pub})
        return items

    if strip_ns(root.tag).lower() == "feed":
        ns = "{http://www.w3.org/2005/Atom}"
        for ent in root.findall(f".//{ns}entry"):
            title = (ent.findtext(f"{ns}title") or "").strip()
            pub = (ent.findtext(f"{ns}published") or "").strip()
            upd = (ent.findtext(f"{ns}updated") or "").strip()

            link = ""
            for lk in ent.findall(f"{ns}link"):
                rel = (lk.get("rel") or "alternate").lower()
                href = lk.get("href") or ""
                if rel == "alternate" and href:
                    link = href
                    break
            if not link:
                lks = ent.findall(f"{ns}link")
                if lks:
                    link = lks[0].get("href") or ""

            if title and link:
                items.append({"title": title, "link": normalize_url(link), "published_raw": pub or upd})
        return items

    for ent in root.findall(".//*"):
        if strip_ns(ent.tag).lower() == "entry":
            title = ""
            link = ""
            pub = ""
            for ch in list(ent):
                tg = strip_ns(ch.tag).lower()
                if tg == "title":
                    title = (ch.text or "").strip()
                elif tg == "link":
                    link = ch.get("href") or (ch.text or "").strip()
                elif tg in ("published", "updated", "pubdate", "date"):
                    pub = (ch.text or "").strip()
            if title and link:
                items.append({"title": title, "link": normalize_url(link), "published_raw": pub})
    return items

def parse_feed_items(feed_url: str) -> List[Dict]:
    r = req(feed_url, accept_xml=True)
    if not r:
        return []

    raw = r.content or b""
    if not raw:
        return []

    raw = raw.lstrip(b"\xef\xbb\xbf").lstrip()

    try:
        root = ET.fromstring(raw)
        return _parse_root_to_items(root)
    except Exception as e1:
        try:
            soup = BeautifulSoup(raw, "xml")
            items: List[Dict] = []
            for node in soup.find_all(["item", "entry"]):
                title = (node.find("title").get_text(strip=True) if node.find("title") else "").strip()
                link = ""
                lk = node.find("link")
                if lk:
                    link = lk.get("href") or lk.get_text(strip=True) or ""
                pub = ""
                for tag_name in ["pubDate", "published", "updated", "dc:date", "date"]:
                    t = node.find(tag_name)
                    if t and t.get_text(strip=True):
                        pub = t.get_text(strip=True)
                        break
                if title and link and link.startswith("http"):
                    items.append({"title": title, "link": normalize_url(link), "published_raw": pub})
            if items:
                return items
        except Exception:
            pass

        print(f"[warn] feed parse failed: {feed_url} -> {e1}")
        return []

def collect_from_feed(feed: Dict) -> List[Dict]:
    url = feed["url"]
    print(f"🧾 Feed: {url}")
    items = parse_feed_items(url)

    out: List[Dict] = []
    for it in items:
        title = (it.get("title") or "").strip()
        link = normalize_url(it.get("link") or "")
        if not title or not link.startswith("http"):
            continue

        pub_dt = parse_datetime_text(it.get("published_raw",""), NOW)

        out.append({
            "source_list": url,
            "title": title,
            "link": link,
            "list_time_guess": pub_dt.isoformat() if pub_dt else "",
            "list_time_raw": it.get("published_raw",""),
        })

    print(f"  -> {len(out)} items")
    return out

# ===== 本文抽出（trafilatura → readability → p連結） =====
from trafilatura import fetch_url, extract as trafi_extract
from readability import Document

def extract_article(url: str) -> Dict:
    out = {"text": "", "published_dt": None, "published_raw": "", "title_override": "", "canonical_url": ""}

    r = req(url)
    soup = None
    if r:
        soup = soup_from_response(r)

    if soup is not None:
        try:
            html = str(soup)
            ttext = trafi_extract(html, include_comments=False, favor_recall=True, with_metadata=True)
            if ttext and len(ttext) >= 200:
                out["text"] = ttext.strip()
        except Exception:
            pass

    if not r:
        try:
            downloaded = fetch_url(ascii_url(url))
            if downloaded:
                ttext = trafi_extract(downloaded, include_comments=False, favor_recall=True, with_metadata=True)
                if ttext:
                    out["text"] = ttext.strip()
        except Exception:
            pass

    if (soup is not None) and (len(out["text"]) < 200):
        try:
            doc = Document(str(soup))
            main_html = doc.summary()
            main_soup = BeautifulSoup(main_html, "lxml")
            paras = [tx(p) for p in main_soup.find_all("p")]
            text = "\n".join([p for p in paras if len(p) >= 20]).strip()
            if len(text) > len(out["text"]):
                out["text"] = text
            if not out["title_override"]:
                out["title_override"] = doc.title() or ""
        except Exception:
            pass

    if (soup is not None) and (len(out["text"]) < 200):
        paras = []
        art = soup.find("article")
        target = art if art else soup
        for p in target.find_all("p"):
            txtp = tx(p)
            if len(txtp) >= 20:
                paras.append(txtp)
        if paras:
            joined = "\n".join(paras)
            if len(joined) > len(out["text"]):
                out["text"] = joined

    canonical_url = None
    if soup is not None:
        title_tag = soup.find("meta", property="og:title") or soup.find("title")
        if title_tag:
            out["title_override"] = title_tag.get("content", "") if title_tag.has_attr("content") else tx(title_tag)
        can = soup.find("link", rel=lambda x: x and "canonical" in x)
        if can and can.get("href"):
            canonical_url = can["href"]
        ogu = soup.find("meta", property="og:url")
        if (not canonical_url) and ogu and ogu.get("content"):
            canonical_url = ogu["content"]
    out["canonical_url"] = normalize_url(canonical_url) if canonical_url else ""

    cand_dt, best_src = None, ""
    if soup is not None:
        time_texts = []
        metas = [
            ('meta', {'property':'article:published_time'}),
            ('meta', {'property':'article:modified_time'}),
            ('meta', {'property':'og:updated_time'}),
            ('meta', {'name':'pubdate'}),
            ('meta', {'name':'publish-date'}),
            ('meta', {'name':'date'}),
            ('meta', {'name':'DC.date'}),
            ('meta', {'itemprop':'datePublished'}),
            ('meta', {'itemprop':'dateModified'}),
        ]
        for sel in metas:
            m = soup.find(*sel)
            if m and m.get("content"):
                time_texts.append(m["content"])
        for el in soup.select("time"):
            if el.get("datetime"):
                time_texts.append(el["datetime"])
            ttxt = tx(el)
            if ttxt:
                time_texts.append(ttxt)
        for cls in ["time","date","timestamp","modDate","update","c-article__time","c-card__time","pubdate"]:
            el = soup.find(class_=cls)
            if el:
                ttxt = tx(el)
                if ttxt:
                    time_texts.append(ttxt)

        for sc in soup.find_all("script", type=lambda t: t and "ld+json" in t):
            try:
                data = json.loads(sc.string or "")
                objs = data if isinstance(data, list) else [data]
                for o in objs:
                    if isinstance(o, dict):
                        for k in ["datePublished", "dateModified", "uploadDate"]:
                            if k in o and o[k]:
                                time_texts.append(str(o[k]))
            except Exception:
                pass

        for s in time_texts:
            dtm = parse_datetime_text(s, NOW)
            if dtm and ((cand_dt is None) or (dtm > cand_dt)):
                cand_dt, best_src = dtm, s

    if (cand_dt is None) and (soup is not None) and (norm_host(urlparse(url).netloc) == "zenn.dev"):
        try:
            next_data = soup.find("script", id="__NEXT_DATA__")
            if next_data and next_data.string:
                data = json.loads(next_data.string)
                def dfs(o, found):
                    if isinstance(o, dict):
                        for k, v in o.items():
                            if k in {"publishedAt","published_at","released_at","datePublished"} and isinstance(v, str):
                                found.append(v)
                            dfs(v, found)
                    elif isinstance(o, list):
                        for x in o:
                            dfs(x, found)
                bag = []
                dfs(data, bag)
                for s in bag:
                    dd = parse_datetime_text(s, NOW)
                    if dd:
                        cand_dt = dd
                        best_src = s
                        break
        except Exception:
            pass

    out["published_dt"] = cand_dt
    out["published_raw"] = best_src
    return out

# ===== 要約（OpenAI → ローカル） =====
def local_fallback_summary(title: str, url: str, body: str) -> str:
    text = (body or "").strip()
    sents = re.split(r"(?:。|\.\s+|\n)+", text)
    sents = [s.strip() for s in sents if s.strip()]
    gist = "。".join(sents[:3]) + ("。" if sents[:3] else "")
    words = re.findall(r"[A-Za-z][A-Za-z\-]{2,}|[ァ-ヴー]{2,}|[一-龥]{2,}", text)
    top = []
    if words:
        from collections import Counter
        for w, _ in Counter(words).most_common(5):
            lw = w.lower()
            if lw in {"https","http","www","com"}:
                continue
            top.append(w)
            if len(top) >= 5:
                break
    bullets = "\n".join([f"- ポイント: {w}" for w in top]) if top else "- ポイント: 主要事項は本文参照"
    if not gist:
        gist = f"{title} の要点を簡易にまとめました。本文抽出が十分でない可能性があります。"
    return f"""{gist}
{bullets}
影響/示唆: 本文の内容から関連分野への影響を検討してください。
出典: {url}"""

def summarize_article(title: str, url: str, body: str) -> str:
    body_trim = (body or "")[:ARTICLE_CHARS_LIMIT]
    if client is not None:
        prompt = f"""
あなたは有能なニュースエディターです。以下の本文に基づき、日本語で簡潔な要約を作成してください。

出力:
- 2〜4文の要旨
- 「ポイント:」3〜5個（本文にある事実・数値・固有名詞のみ）
- 「影響/示唆:」1〜2文
- 「出典:」にURL 1行

制約: 本文にない推測はしない。見出しではなく本文を根拠にする。

[タイトル] {title}
[URL] {url}

[本文（冒頭 {len(body_trim)} 文字）]
{body_trim}
""".strip()
        try:
            resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
            return resp.output_text
        except Exception as e:
            print(f"[warn] OpenAI要約に失敗: {e} -> ローカル要約にフォールバック")
    return local_fallback_summary(title, url, body)

# ===== 重複削除（title単位：新しい方を残す） =====
def pick_newer_by_time(a: Dict, b: Dict, time_key: str) -> Dict:
    ta = a.get(time_key) or ""
    tb = b.get(time_key) or ""
    if ta and tb:
        return a if ta >= tb else b
    if ta and not tb:
        return a
    if tb and not ta:
        return b
    return a

def dedupe_by_title_keep_latest(rows: List[Dict], time_key: str) -> List[Dict]:
    best: Dict[str, Dict] = {}
    for r in rows:
        t_raw = (r.get("title") or "").strip()
        if not t_raw:
            continue
        key = normalize_title(t_raw)
        if key not in best:
            best[key] = r
        else:
            best[key] = pick_newer_by_time(best[key], r, time_key=time_key)
    return list(best.values())

# ===== メイン =====
def main():
    print(f"[info] NOW (JST): {NOW.isoformat()} / lookback: {LOOKBACK_HOURS}h")

    candidates: List[Dict] = []

    # 1) HTML一覧から候補収集（元々の取得先を維持）
    for lp in TARGET_LIST_PAGES:
        print(f"🔎 List: {lp}")
        rows = collect_from_list(lp)
        print(f"  -> {len(rows)} candidates")
        candidates.extend(rows)
        time.sleep(SLEEP_LIST)

    # 2) RSS/Atomから候補収集（Zenn/Qiita/OpenAI）
    for fd in FEED_URLS:
        rows = collect_from_feed(fd)
        candidates.extend(rows)
        time.sleep(SLEEP_FEED)

    # 3) 候補：URL正規化してから title で重複削除（list_time_guess が新しい方）
    for r in candidates:
        r["link"] = normalize_url(r.get("link",""))
        r["title"] = (r.get("title") or "").strip()

    candidates = dedupe_by_title_keep_latest(candidates, time_key="list_time_guess")
    print(f"\n🧮 Unique candidates (dedup by title): {len(candidates)}")

    # 4) 本文抽出 & lookback再判定 & 要約
    results: List[Dict] = []
    for i, it in enumerate(candidates, 1):
        url = it["link"]
        title = it["title"]
        if not url.startswith("http"):
            continue

        print(f"\n🌐 [{i}/{len(candidates)}] {title[:60]} ...")
        art = extract_article(url)

        pub_dt = art["published_dt"]
        if not pub_dt and it.get("list_time_guess"):
            try:
                pub_dt = dt.datetime.fromisoformat(it["list_time_guess"])
            except Exception:
                pub_dt = None

        if not within_lookback(pub_dt):
            print("  -> Skip (older than lookback or time unknown)")
            time.sleep(SLEEP_ARTICLE)
            continue

        final_url = url
        if art.get("canonical_url"):
            cu = urlparse(art["canonical_url"])
            uh = norm_host(urlparse(url).netloc)
            ch = norm_host(cu.netloc)
            if (uh == ch) or (uh in CROSS_HOST_ALLOW and ch in CROSS_HOST_ALLOW.get(uh, set())):
                final_url = art["canonical_url"]

        title_use = (art["title_override"] or title).strip()
        body = art["text"] or ""
        summary = summarize_article(title_use, final_url, body)

        results.append({
            "title": title_use,
            "url": final_url,
            "published_at": pub_dt.isoformat() if pub_dt else "",
            "published_raw": art["published_raw"] or it.get("list_time_raw",""),
            "source_list": it.get("source_list",""),
            "body_chars": len(body),
            "excerpt": body[:240].replace("\n"," "),
            "summary": summary
        })
        time.sleep(SLEEP_ARTICLE)

    # 5) 結果：titleで重複削除（published_at が新しい方）
    results = dedupe_by_title_keep_latest(results, time_key="published_at")

    # 6) 保存
    df = pd.DataFrame(results)
    if not df.empty:
        df.sort_values("published_at", ascending=False, inplace=True)
        df = df[["title","url","published_at","published_raw","source_list","body_chars","excerpt","summary"]]
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")

    lines = [f"# {DATE_PREFIX} 直近{LOOKBACK_HOURS}時間：本文抽出&要約（JST）\n"]
    for _, r in df.iterrows():
        lines.append(f"## {r['title']}\n")
        if r['published_at']: lines.append(f"- 公開推定(JST): {r['published_at']}")
        if r['published_raw']: lines.append(f"- 抽出元テキスト: {r['published_raw']}")
        if r['source_list']: lines.append(f"- 取得元リスト/フィード: {r['source_list']}")
        lines.append(f"- URL: {r['url']}")
        lines.append(f"- 本文抽出サイズ: {r['body_chars']} chars")
        if r['summary']:
            lines.append("\n" + r['summary'])
        lines.append("\n---\n")

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("\n✅ Done.")
    print(f"- CSV: {CSV_PATH}")
    print(f"- MD : {MD_PATH}")

if __name__ == "__main__":
    main()
