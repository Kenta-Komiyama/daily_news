# -*- coding: utf-8 -*-
"""
ニュース一覧から「記事ページ」だけを正しくたどり、
過去24時間（JST）に公開された記事の本文抽出＋要約（全件）を CSV/Markdown に保存します。

- Accept-Encoding を明示（zstd回避）、apparent_encoding で文字化け対策
- trafilatura が空の場合は readability-lxml で本文抽出にフォールバック
- Zenn は /{user}/articles/{slug} だけ厳格に許可
- KDnuggets のタグ/ニュースは /YYYY/MM/slug(.html)[/][?...] を記事と判定し、一覧に日時がなくても候補に残す
- Towards Data Science は一覧が towardsdatascience.com、記事が medium.com 配下なので cross-host 許可
- すべてのホスト名判定は正規化（www除去・小文字）で統一
- 要約は OpenAI(Responses API, gpt-5-mini) → 失敗/未設定時はローカル要約にフォールバック（全件 summary 出力）
"""

import os, re, time, datetime as dt, sys, logging
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
from dateutil import tz

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

SLEEP_LIST = float(os.environ.get("SLEEP_LIST", "0.4"))       # 一覧ページ間待機
SLEEP_ARTICLE = float(os.environ.get("SLEEP_ARTICLE", "0.6")) # 記事ページ間待機
ARTICLE_CHARS_LIMIT = int(os.environ.get("ARTICLE_CHARS_LIMIT", "9000"))
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
OUT_DIR = os.environ.get("OUT_DIR", "out")

os.makedirs(OUT_DIR, exist_ok=True)
DATE_PREFIX = NOW.strftime("%Y-%m-%d")
os.environ["DATE_PREFIX"] = DATE_PREFIX  # ← メール送信補助
CSV_PATH = os.path.join(OUT_DIR, f"{DATE_PREFIX}_news_{LOOKBACK_HOURS}h_fulltext.csv")
MD_PATH  = os.path.join(OUT_DIR, f"{DATE_PREFIX}_news_{LOOKBACK_HOURS}h_fulltext.md")

# ===== ユーティリティ：ホスト名正規化 =====
def norm_host(host: str) -> str:
    """netloc を www. 除去・小文字化して正規化"""
    h = (host or "").lower()
    if h.startswith("www."):
        h = h[4:]
    return h

# ===== 対象「一覧」ページ（拡張版） =====
TARGET_LIST_PAGES = [
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
    "https://openai.com/ja-JP/news/",
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
]

# ===== OpenAI（要約用。無ければローカル要約へ） =====
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

# ===== HTTPユーティリティ（zstd回避&文字化け対策） =====
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Referer": "",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",  # ← zstdは要求しない
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

def req(url: str) -> Optional[requests.Response]:
    try:
        headers = DEFAULT_HEADERS.copy()
        headers["Referer"] = url
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"[warn] HTTP {r.status_code}: {url}")
            return None
        if not r.encoding or r.encoding.lower() in ("ascii", "latin-1"):
            r.encoding = r.apparent_encoding or "utf-8"
        return r
    except Exception as e:
        print(f"[error] request failed: {url} -> {e}")
        return None

def soup_from_response(r: requests.Response) -> BeautifulSoup:
    try:
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
]
ABS = [
    re.compile(r"(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})"),
    re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日"),
    re.compile(r"\b(\d{1,2})/(\d{1,2})\b"),
]
def parse_datetime_text(s: str, base: dt.datetime) -> Optional[dt.datetime]:
    s = s.strip() if s else ""
    if not s: return None
    for pat, unit in REL:
        m = pat.search(s)
        if m:
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
    m = re.search(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(:\d{2})?(Z|[+\-]\d{2}:\d{2})?", s)
    if m:
        y, mo, d = map(int, m.group(1).split("-"))
        hh, mm = map(int, m.group(2).split(":"))
        try:
            return dt.datetime(y, mo, d, hh, mm, 0, tzinfo=JST)
        except: pass
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

# ===== 記事URL判定（プロフィール/タグ除外 & Zenn厳格 & Medium対応） =====
SITE_RULES = {
    "businessinsider.jp": {"include": [r"/post-\d+"], "exclude": [r"/author/", r"/category/", r"/tag/"]},
    "business.nikkei.com": {"include": [r"/atcl/"], "exclude": [r"/author/", r"/category/", r"/tag/"]},
    "xtech.nikkei.com": {"include": [r"/atcl/"], "exclude": [r"/author/", r"/category/", r"/tag/"]},
    "itmedia.co.jp": {"include": [r"/aiplus/articles/"], "exclude": [r"/author/", r"/rsslist", r"/category/", r"/tag/"]},
    "techno-edge.net": {"include": [r"/\d{4}/\d{2}/\d{2}/", r"/article/"], "exclude": [r"/tag/", r"/category/", r"/author/"]},
    "b.hatena.ne.jp": {"hatena_special": True},  # a.entry-link が外部記事
    "zenn.dev": {"include": [r"^/[^/]+/articles/[^/]+/?$"], "exclude": [r"^/users?/", r"^/topics/", r"^/books/", r"^/scraps/", r"^/tags?/"]},
    "openai.com": {"include": [r"/news/"], "exclude": [r"/team/", r"/researchers/", r"/about/"]},
    "news.microsoft.com": {"include": [r"/source/"], "exclude": [r"/people/", r"/about/"]},
    "huggingface.co": {"include": [r"/blog/"], "exclude": [r"/authors?/"]},

    # 追加分
    "ai-scholar.tech": {"include": [r"/ai_news/", r"/ai_trends/", r"/ai_book/", r"/ai_scholar/", r"/article/"], "exclude": [r"/category/", r"/tag/", r"/author/"]},
    "competition-content.signate.jp": {"include": [r"^/articles/[^/]+/?$"], "exclude": [r"/users?/", r"/tags?/"]},
    "kaggle.com": {"include": [r"^/blog/[^/?#]+/?$"], "exclude": []},  # /blog?sort=... は一覧、記事は /blog/{slug}
    "kdnuggets.com": {"include": [r"^/\d{4}/\d{2}/[^/][^?#]*(?:\.html)?/?(?:\?.*)?$"], "exclude": [r"^/tag/", r"^/tags?/"]},
    "towardsdatascience.com": {"include": [], "exclude": []},  # 実体は medium.com 側
    "medium.com": {"include": [r"^/towards-data-science/[^/]+-[0-9a-fA-F]{12}$", r"^/p/[0-9a-fA-F]{12}$"], "exclude": [r"^/tag/", r"/about/"]},
    "analyticsvidhya.com": {"include": [r"^/blog/\d{4}/\d{2}/[^/].*"], "exclude": [r"/category/", r"/tag/"]},
    "codezine.jp": {"include": [r"^/article/detail/\d+\.html$"], "exclude": [r"^/category/", r"^/tag/"]},
    "publickey1.jp": {"include": [r"^/blog/\d{4}/\d{2}/[^/].*\.html$"], "exclude": []},
}
COMMON_EXCLUDES = [r"/author/", r"/users?/", r"/tag/", r"/category/", r"/topics/", r"/people/"]
ZENN_ARTICLE_RE = re.compile(r"^/[^/]+/articles/[^/]+/?$")
KDN_ARTICLE_RE  = re.compile(r"^/\d{4}/\d{2}/[^/][^?#]*(?:\.html)?/?(?:\?.*)?$")  # 末尾/ とクエリ許容

# 一覧のドメイン→記事リンク許可ドメイン集合（cross-host）※キーは正規化ホスト
CROSS_HOST_ALLOW: Dict[str, Set[str]] = {
    "towardsdatascience.com": {"towardsdatascience.com", "medium.com"},
}

# 一覧に日時が無くても候補に残す特例（記事ページ側で24h判定）※要正規化
ALLOW_NO_LIST_TIME = {
    "kdnuggets.com",
    "towardsdatascience.com",
    "medium.com",
    "analyticsvidhya.com",
}

def score_link_by_rules(href: str, base_host_raw: str) -> int:
    p = urlparse(href)
    if not p.scheme.startswith("http"):
        return -999
    path = p.path or "/"
    link_host = norm_host(p.netloc)
    base_host = norm_host(base_host_raw)
    score = 0

    # 共通NG
    for pat in COMMON_EXCLUDES:
        if re.search(pat, path):
            score -= 100

    # base_host が TDS の場合は link_host（=medium.com）側のルールを参照
    rules_host = link_host if base_host == "towardsdatascience.com" else base_host
    rules = SITE_RULES.get(rules_host, SITE_RULES.get(base_host, {}))

    for pat in rules.get("include", []):
        if re.search(pat, path):
            score += 20
    for pat in rules.get("exclude", []):
        if re.search(pat, path):
            score -= 100

    # Medium の12hex ID終端に微加点
    if re.search(r"[0-9a-fA-F]{12}$", path):
        score += 3

    # パスの深さで微加点
    score += min((path.strip("/").count("/")), 4)
    return score

def pick_article_anchor(card, base_url: str) -> Optional[str]:
    base_host_raw = urlparse(base_url).netloc
    base_host = norm_host(base_host_raw)

    # Hatena: 外部記事は a.entry-link を優先
    if SITE_RULES.get(base_host, {}).get("hatena_special"):
        a = card.select_one("a.entry-link[href]")
        if a:
            return urljoin(base_url, a["href"])

    anchors = card.find_all("a", href=True)
    if not anchors:
        return None

    # Zenn: /{user}/articles/{slug} の完全型のみ許可
    if base_host == "zenn.dev":
        for a in anchors:
            href = urljoin(base_url, a["href"])
            path = urlparse(href).path or "/"
            if ZENN_ARTICLE_RE.match(path):
                return href
        return None

    # KDnuggets: /YYYY/MM/slug(.html)[/][?...] のみ許可（タグ/カテゴリ除外）
    if base_host == "kdnuggets.com":
        for a in anchors:
            href = urljoin(base_url, a["href"])
            path = urlparse(href).path or "/"
            if KDN_ARTICLE_RE.match(path) and not re.search(r"^/tag/|^/tags?/", path):
                return href
        return None

    # 以降は一般スコアリング
    best_href, best_score = None, -10**9
    for a in anchors:
        href = urljoin(base_url, a["href"])
        if href.startswith(("mailto:", "tel:", "#")):
            continue
        score = score_link_by_rules(href, base_host_raw)

        a_text = (a.get_text(" ", strip=True) or "").lower()
        a_cls = " ".join(a.get("class", [])).lower()
        for kw in ["author", "プロフィール", "筆者", "投稿者", "users", "タグ", "category", "topics"]:
            if kw in a_text or kw in a_cls:
                score -= 30
        for good in ["title", "headline", "entry-title", "news-title"]:
            if good in a_cls:
                score += 10

        if score > best_score:
            best_score, best_href = score, href

    if best_href:
        path = urlparse(best_href).path or "/"
        for pat in COMMON_EXCLUDES:
            if re.search(pat, path):
                return None
    return best_href

# ===== 一覧抽出 =====
def extract_list_candidates(url: str, allowed_hosts: Optional[Set[str]] = None) -> List[Dict]:
    r = req(url)
    if not r: return []
    soup = soup_from_response(r)
    items, seen = [], set()
    base_host_raw = urlparse(url).netloc
    base_host = norm_host(base_host_raw)

    # allowed_hosts も正規化して比較
    allowed_norm = {norm_host(h) for h in allowed_hosts} if allowed_hosts is not None else None

    for card in soup.select("article, li, div"):
        link = pick_article_anchor(card, url)
        if not link:
            continue

        link_host_norm = norm_host(urlparse(link).netloc)

        # 許可ホストの絞り込み（Hatena特例は外部OK）
        if allowed_norm is not None and link_host_norm not in allowed_norm:
            if not SITE_RULES.get(base_host, {}).get("hatena_special"):
                continue

        # タイトル
        title_el = None
        for sel in ["a h1", "a h2", "a h3", "h1 a", "h2 a", "h3 a", "h1", "h2", "h3"]:
            title_el = card.select_one(sel)
            if title_el: break
        title = tx(title_el) if title_el else tx(card)
        if not title:
            try:
                title = tx(card.find("a", href=True))
            except:
                title = link

        # 一覧側の時刻（粗判定）
        meta = []
        meta += [tx(x) for x in card.select("time")]
        for cls in ["time","date","timestamp","modDate","update","c-article__time","c-card__time","pubdate"]:
            el = card.find(class_=cls)
            if el: meta.append(tx(el))
        meta += [tx(x) for x in card.find_all(["span","small","p"], limit=3)]
        ok, dttm, src = any_within(meta)

        # 特例: 一覧に日時が無いサイトは候補に残す（記事ページで24h判定）
        if not ok and base_host not in ALLOW_NO_LIST_TIME:
            continue

        key = (title.strip(), link)
        if key in seen:
            continue
        seen.add(key)
        items.append({
            "source_list": url,
            "title": title.strip(),
            "link": link,
            "list_time_guess": dttm.isoformat() if ok and dttm else "",
            "list_time_raw": src if ok else "",
        })
    return items

def collect_from_list(url: str) -> List[Dict]:
    base_host_raw = urlparse(url).netloc
    base_host = norm_host(base_host_raw)

    # デフォルトは同一ホストのみ
    allowed: Optional[Set[str]] = {base_host}

    # Hatena は外部記事OK
    if SITE_RULES.get(base_host, {}).get("hatena_special"):
        allowed = None

    # cross-host 許可（TDS → medium.com）
    if base_host in CROSS_HOST_ALLOW:
        allowed = CROSS_HOST_ALLOW[base_host]

    return extract_list_candidates(url, allowed_hosts=allowed)

# ===== 本文抽出（trafilatura → readability → p連結） =====
from trafilatura import fetch_url, extract as trafi_extract
from readability import Document

def extract_article(url: str) -> Dict:
    out = {"text": "", "published_dt": None, "published_raw": "", "title_override": ""}

    # 1) requestsで取得（zstd回避ヘッダ＆文字化け対策）
    r = req(url)
    soup = None
    if r:
        soup = soup_from_response(r)

    # 2) trafilatura（HTML文字列）で本文抽出
    if soup is not None:
        try:
            html = str(soup)
            ttext = trafi_extract(html, include_comments=False, favor_recall=True, with_metadata=True)
            if ttext and len(ttext) >= 200:
                out["text"] = ttext.strip()
        except Exception:
            pass

    # requests失敗時の最終手段としてURLダウンローダ
    if not r:
        try:
            downloaded = fetch_url(url)
            if downloaded:
                ttext = trafi_extract(downloaded, include_comments=False, favor_recall=True, with_metadata=True)
                if ttext:
                    out["text"] = ttext.strip()
        except Exception:
            pass

    # 3) readability フォールバック
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

    # 4) <article>/p 連結の最終保険
    if (soup is not None) and (len(out["text"]) < 200):
        paras = []
        art = soup.find("article")
        target = art if art else soup
        for p in target.find_all("p"):
            txt = tx(p)
            if len(txt) >= 20:
                paras.append(txt)
        if paras:
            joined = "\n".join(paras)
            if len(joined) > len(out["text"]):
                out["text"] = joined

    # タイトル（OG:title優先）
    if soup is not None:
        title_tag = soup.find("meta", property="og:title") or soup.find("title")
        if title_tag:
            out["title_override"] = title_tag.get("content", "") if title_tag.has_attr("content") else tx(title_tag)

    # 公開日時抽出
    if soup is not None:
        time_texts = []
        for sel in [
            ('meta', {'property':'article:published_time'}),
            ('meta', {'name':'pubdate'}),
            ('meta', {'name':'publish-date'}),
            ('meta', {'name':'date'}),
            ('meta', {'itemprop':'datePublished'}),
        ]:
            m = soup.find(*sel)
            if m and m.get("content"): time_texts.append(m["content"])
        for el in soup.select("time"):
            if el.get("datetime"): time_texts.append(el["datetime"])
            txt = tx(el)
            if txt: time_texts.append(txt)
        for cls in ["time","date","timestamp","modDate","update","c-article__time","c-card__time","pubdate"]:
            el = soup.find(class_=cls)
            if el:
                txt = tx(el)
                if txt: time_texts.append(txt)

        cand_dt, best_src = None, ""
        for s in time_texts:
            dtm = None
            m = re.search(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(Z|[+\-]\d{2}:\d{2})?", s)
            if m:
                from dateutil import tz as _tz
                y, mo, d = map(int, m.group(1).split("-"))
                hh, mm, ss = map(int, m.group(2).split(":"))
                try:
                    dtm = dt.datetime(y, mo, d, hh, mm, ss, tzinfo=_tz.tzutc() if (m.group(3)=="Z") else JST).astimezone(JST)
                except: pass
            if not dtm:
                dtm = parse_datetime_text(s, NOW)
            if dtm and ((cand_dt is None) or (dtm > cand_dt)):
                cand_dt, best_src = dtm, s
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
            if len(top) >= 5: break
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
            resp = client.responses.create(model=OPENAI_MODEL, input=prompt)  # temperature等は渡さない
            return resp.output_text
        except Exception as e:
            print(f"[warn] OpenAI要約に失敗: {e} -> ローカル要約にフォールバック")
    return local_fallback_summary(title, url, body)

# ===== メイン =====
def main():
    print(f"[info] NOW (JST): {NOW.isoformat()} / lookback: {LOOKBACK_HOURS}h")
    # 1) 一覧から候補収集
    candidates: List[Dict] = []
    for lp in TARGET_LIST_PAGES:
        print(f"🔎 List: {lp}")
        rows = collect_from_list(lp)
        print(f"  -> {len(rows)} candidates")
        candidates.extend(rows)
        time.sleep(SLEEP_LIST)

    # 重複除去（title+link）
    seen = set()
    uniq = []
    for r in candidates:
        k = (r["title"], r["link"])
        if k in seen: continue
        seen.add(k)
        uniq.append(r)
    print(f"\n🧮 Unique candidates: {len(uniq)}")

    # 2) 本文抽出 & 24h再判定 & 要約（全件必須）
    results = []
    for i, it in enumerate(uniq, 1):
        url = it["link"]
        title = it["title"]
        print(f"\n🌐 [{i}/{len(uniq)}] {title[:60]} ...")
        art = extract_article(url)

        pub_dt = art["published_dt"] or (dt.datetime.fromisoformat(it["list_time_guess"]) if it.get("list_time_guess") else None)
        if not within_lookback(pub_dt):
            print("  -> Skip (older than lookback)")
            time.sleep(SLEEP_ARTICLE)
            continue

        title_use = art["title_override"] or title
        body = art["text"] or ""

        # 全件サマリ必須
        summary = summarize_article(title_use, url, body)

        results.append({
            "title": title_use,
            "url": url,
            "published_at": pub_dt.isoformat() if pub_dt else "",
            "published_raw": art["published_raw"] or it.get("list_time_raw",""),
            "source_list": it.get("source_list",""),
            "body_chars": len(body),
            "excerpt": body[:240].replace("\n"," "),
            "summary": summary
        })
        time.sleep(SLEEP_ARTICLE)

    # 3) 保存
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
        if r['source_list']: lines.append(f"- 取得元リスト: {r['source_list']}")
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

# ===== 実行 =====
if __name__ == "__main__":
    main()
