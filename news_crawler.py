# -*- coding: utf-8 -*-
"""
ニュース一覧から「記事ページ」だけを正しくたどり、
過去 LOOKBACK_HOURS（JST）に公開された記事の本文抽出＋要約（全件）を CSV/Markdown に保存します。

今回の修正:
- OpenAI: HTML取得は403になりがちなので news/blog はRSS（/news/rss.xml 等）から取得（403回避）
- Zenn: 一覧HTMLで記事抽出が0件→トピックRSS（/topics/<topic>/feed）から取得（安定）
- Qiita: タグRSS（/tags/<tag>/feed.atom）から取得（安定）
- 同じ title の重複削除（候補段階 + 結果段階）

補足:
- RSSからは published/updated を「一覧時刻」として入れる（記事側でも再抽出するが、救済になる）
"""

import os, re, time, datetime as dt, sys, logging, json
from typing import List, Dict, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode

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
LOOKBACK_HOURS = int(os.environ.get("LOOKBACK_HOURS", "24"))
THRESHOLD = NOW - dt.timedelta(hours=LOOKBACK_HOURS)
TIMEOUT = 25

SLEEP_LIST = float(os.environ.get("SLEEP_LIST", "0.4"))
SLEEP_ARTICLE = float(os.environ.get("SLEEP_ARTICLE", "0.6"))
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
    t = (t or "").strip()
    t = re.sub(r"\s+", " ", t)
    return t

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
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Referer": "",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

def req(url: str, accept_xml: bool = False) -> Optional[requests.Response]:
    """403対策でUAを変えて一度だけ再試行"""
    try:
        headers = DEFAULT_HEADERS.copy()
        headers["Referer"] = url
        if accept_xml:
            headers["Accept"] = "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5"
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 403:
            headers["User-Agent"] = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
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

def soup_from_response(r: requests.Response, xml: bool = False) -> BeautifulSoup:
    try:
        html = r.text
        if not html or len(html) < 100:
            html = r.content.decode(r.apparent_encoding or "utf-8", errors="replace")
    except Exception:
        html = r.content.decode("utf-8", errors="replace")
    return BeautifulSoup(html, "xml" if xml else "lxml")

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
            try:
                return dt.datetime(y, mo, d, tzinfo=JST)
            except:
                pass

    m = ABS[2].search(s)
    if m:
        mo, d = map(int, m.groups())
        try:
            return dt.datetime(NOW.year, mo, d, tzinfo=JST)
        except:
            pass

    m = ABS[3].search(s)
    if m:
        mon = MONTHS.get(m.group(1).lower(), 0)
        d = int(m.group(2)); y = int(m.group(3))
        if mon:
            try:
                return dt.datetime(y, mon, d, tzinfo=JST)
            except:
                pass

    m = ABS[4].search(s)
    if m:
        d = int(m.group(1)); mon = MONTHS.get(m.group(2).lower(), 0); y = int(m.group(3))
        if mon:
            try:
                return dt.datetime(y, mon, d, tzinfo=JST)
            except:
                pass

    # ISO 8601
    m = re.search(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(:\d{2})?(Z|[+\-]\d{2}:\d{2})?", s)
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

# ===== RSS/Atom 取り込み（Zenn/Qiita/OpenAIなど用） =====
def fetch_feed_items(feed_url: str) -> List[Dict]:
    """
    RSS/Atomを読み、{title, link, list_time_guess, list_time_raw, source_list} を返す
    """
    r = req(feed_url, accept_xml=True)
    if not r:
        return []
    soup = soup_from_response(r, xml=True)

    items: List[Dict] = []
    # RSS: <item> / Atom: <entry>
    nodes = soup.find_all(["item", "entry"])
    for n in nodes:
        # title
        title = tx(n.find("title"))
        title = normalize_title(title)
        if not title:
            continue

        # link: Atomは <link href="...">
        link = ""
        link_tag = n.find("link")
        if link_tag:
            if link_tag.get("href"):
                link = link_tag["href"]
            else:
                link = tx(link_tag)
        link = normalize_url(link)
        if not link or not link.startswith("http"):
            continue

        # date candidates
        dt_raws = []
        for tag_name in ["published", "updated", "pubDate", "dc:date", "date"]:
            t = n.find(tag_name)
            if t and (tx(t) or t.get("content")):
                dt_raws.append(tx(t) or t.get("content", ""))

        # RFC822っぽいのは dateutil で拾えないことがあるので簡易補助
        list_dt = None
        best_src = ""
        for s in dt_raws:
            # 1) 既存parse
            dd = parse_datetime_text(s, NOW)
            if dd:
                list_dt, best_src = dd, s
                break
            # 2) RFC822（例: Tue, 17 Dec 2025 10:00:00 GMT）
            try:
                from email.utils import parsedate_to_datetime
                pdt = parsedate_to_datetime(s)
                if pdt:
                    list_dt = pdt.astimezone(JST)
                    best_src = s
                    break
            except Exception:
                pass

        items.append({
            "source_list": feed_url,
            "title": title,
            "link": link,
            "list_time_guess": list_dt.isoformat() if list_dt else "",
            "list_time_raw": best_src if best_src else "",
        })

    return items

# ===== HTML「一覧」ページ（従来） =====
TARGET_LIST_PAGES = [
    # （ここは従来どおり。Zenn/Qiita/OpenAIはRSS側で取るので、無理に入れなくてもOK）
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
    "https://huggingface.co/blog",
    "https://ai-scholar.tech/",
    "https://competition-content.signate.jp/articles",
    "https://www.kaggle.com/blog?sort=hotness",
    "https://www.kdnuggets.com/news/index.html",
    "https://towardsdatascience.com/latest/",
    "https://www.analyticsvidhya.com/blog-archive/",
    "https://codezine.jp/data/",
    "https://www.publickey1.jp/",
]

# ===== RSSで取得するフィード（追加・重要） =====
FEED_PAGES = [
    # OpenAI（403回避でRSS推奨）
    "https://openai.com/news/rss.xml",
    # Zenn topics: /topics/<topic>/feed
    "https://zenn.dev/topics/ai/feed",
    "https://zenn.dev/topics/nlp/feed",
    "https://zenn.dev/topics/deeplearning/feed",
    "https://zenn.dev/topics/python/feed",
    "https://zenn.dev/topics/%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92/feed",
    # Qiita tags: /tags/<tag>/feed.atom （タグページURLに feed.atom を付ける形）
    "https://qiita.com/tags/AI/feed.atom",
    "https://qiita.com/tags/LLM/feed.atom",
    "https://qiita.com/tags/DeepLearning/feed.atom",
    "https://qiita.com/tags/Python/feed.atom",
    "https://qiita.com/tags/%E8%87%AA%E7%84%B6%E8%A8%80%E8%AA%9E%E5%87%A6%E7%90%86/feed.atom",
    "https://qiita.com/tags/%E6%A9%9F%E6%A2%B0%E5%AD%A6%E7%BF%92/feed.atom",
]

# ===== 記事URL判定（最低限） =====
COMMON_EXCLUDES = [r"/author/", r"/users?/", r"/tag/", r"/category/", r"/topics/", r"/people/"]

def is_probably_article(url: str) -> bool:
    """
    RSSは基本的に記事URLが来るので緩め。
    HTML一覧から来る場合のノイズ除去の安全弁。
    """
    u = normalize_url(url)
    p = urlparse(u)
    path = p.path or "/"
    for pat in COMMON_EXCLUDES:
        if re.search(pat, path):
            return False
    return True

# ===== HTML一覧抽出（従来の簡易版：現行ロジックでもOKだが、ここは最小限に） =====
def extract_list_candidates_html(url: str) -> List[Dict]:
    r = req(url)
    if not r:
        return []
    soup = soup_from_response(r, xml=False)

    items: List[Dict] = []
    seen: Set[Tuple[str, str]] = set()

    # broadly scan articles
    candidates = soup.select("article, li, div, section, dd")
    if not candidates:
        candidates = soup.find_all(True)

    for card in candidates:
        a = card.find("a", href=True)
        if not a:
            continue
        link = normalize_url(urljoin(url, a["href"]))
        if not link.startswith("http"):
            continue
        if not is_probably_article(link):
            continue

        title = normalize_title(tx(a) or tx(card))
        if not title or len(title) < 3:
            continue

        # list time (best-effort)
        meta_texts = [tx(x) for x in card.select("time")]
        meta_texts += [tx(x) for x in card.find_all(["span","small","p"], limit=6)]
        list_dt = None
        list_raw = ""
        for s in meta_texts:
            dd = parse_datetime_text(s, NOW)
            if dd:
                list_dt = dd
                list_raw = s
                break

        key = (title, link)
        if key in seen:
            continue
        seen.add(key)

        items.append({
            "source_list": url,
            "title": title,
            "link": link,
            "list_time_guess": list_dt.isoformat() if list_dt else "",
            "list_time_raw": list_raw,
        })

    return items

# ===== 本文抽出（trafilatura → readability → p連結） =====
from trafilatura import fetch_url, extract as trafi_extract
from readability import Document

def extract_article(url: str) -> Dict:
    out = {"text": "", "published_dt": None, "published_raw": "", "title_override": "", "canonical_url": ""}

    r = req(url)
    soup = None
    if r:
        soup = soup_from_response(r)

    # trafilatura
    if soup is not None:
        try:
            html = str(soup)
            ttext = trafi_extract(html, include_comments=False, favor_recall=True, with_metadata=True)
            if ttext and len(ttext) >= 200:
                out["text"] = ttext.strip()
        except Exception:
            pass

    # requests失敗時はfetch_url
    if not r:
        try:
            downloaded = fetch_url(url)
            if downloaded:
                ttext = trafi_extract(downloaded, include_comments=False, favor_recall=True, with_metadata=True)
                if ttext:
                    out["text"] = ttext.strip()
        except Exception:
            pass

    # readability fallback
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

    # <article> p concat fallback
    if (soup is not None) and (len(out["text"]) < 200):
        paras = []
        art = soup.find("article")
        target = art if art else soup
        for p in target.find_all("p"):
            t = tx(p)
            if len(t) >= 20:
                paras.append(t)
        if paras:
            joined = "\n".join(paras)
            if len(joined) > len(out["text"]):
                out["text"] = joined

    # title / canonical
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

    # published dt extraction
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
            t = tx(el)
            if t:
                time_texts.append(t)
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
            dd = parse_datetime_text(s, NOW)
            if dd and ((cand_dt is None) or (dd > cand_dt)):
                cand_dt, best_src = dd, s

    # Zenn: __NEXT_DATA__ rescue（必要なら残す）
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
                        cand_dt, best_src = dd, s
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

# ===== 重複削除（title単位） =====
def dedupe_by_title_keep_latest(rows: List[Dict], time_key: str) -> List[Dict]:
    """
    同一 title は1件にする。time_key(ISO文字列)が新しい方を採用。
    """
    best: Dict[str, Dict] = {}
    for r in rows:
        t = normalize_title(r.get("title", ""))
        if not t:
            continue
        cur = best.get(t)
        if cur is None:
            best[t] = r
            continue
        a = r.get(time_key, "") or ""
        b = cur.get(time_key, "") or ""
        if a and b:
            if a > b:
                best[t] = r
        elif a and not b:
            best[t] = r
        # else keep cur
    return list(best.values())

# ===== メイン =====
def main():
    print(f"[info] NOW (JST): {NOW.isoformat()} / lookback: {LOOKBACK_HOURS}h")

    candidates: List[Dict] = []

    # 1) RSS（Zenn/Qiita/OpenAIなど）から候補収集
    for fp in FEED_PAGES:
        print(f"🧾 Feed: {fp}")
        rows = fetch_feed_items(fp)
        print(f"  -> {len(rows)} items")
        candidates.extend(rows)
        time.sleep(SLEEP_LIST)

    # 2) HTML一覧からも候補収集（任意）
    for lp in TARGET_LIST_PAGES:
        print(f"🔎 List: {lp}")
        rows = extract_list_candidates_html(lp)
        print(f"  -> {len(rows)} candidates")
        candidates.extend(rows)
        time.sleep(SLEEP_LIST)

    # 3) 候補の重複削除（title単位で圧縮）
    #    RSSはpublishedが入るので、list_time_guessが新しい方を残す
    candidates = dedupe_by_title_keep_latest(candidates, time_key="list_time_guess")
    print(f"\n🧮 Unique candidates (by title): {len(candidates)}")

    # 4) 本文抽出 & 24h判定 & 要約
    results: List[Dict] = []
    for i, it in enumerate(candidates, 1):
        url = it["link"]
        title = it["title"]
        print(f"\n🌐 [{i}/{len(candidates)}] {title[:60]} ...")

        art = extract_article(url)

        # 公開日時: 記事側が取れればそれ、無ければRSS/一覧時刻(list_time_guess)
        list_guess = None
        if it.get("list_time_guess"):
            try:
                list_guess = dt.datetime.fromisoformat(it["list_time_guess"])
            except Exception:
                list_guess = None

        pub_dt = art["published_dt"] or list_guess
        if not within_lookback(pub_dt):
            print("  -> Skip (older than lookback or time unknown)")
            time.sleep(SLEEP_ARTICLE)
            continue

        # canonical が同一ホストなら差し替え
        final_url = url
        if art.get("canonical_url"):
            cu = urlparse(art["canonical_url"])
            uh = norm_host(urlparse(url).netloc)
            ch = norm_host(cu.netloc)
            if uh == ch:
                final_url = art["canonical_url"]

        title_use = normalize_title(art["title_override"] or title)
        body = art["text"] or ""
        summary = summarize_article(title_use, final_url, body)

        results.append({
            "title": title_use,
            "url": final_url,
            "published_at": pub_dt.isoformat() if pub_dt else "",
            "published_raw": art["published_raw"] or it.get("list_time_raw", ""),
            "source_list": it.get("source_list", ""),
            "body_chars": len(body),
            "excerpt": body[:240].replace("\n", " "),
            "summary": summary
        })
        time.sleep(SLEEP_ARTICLE)

    # 5) 結果の重複削除（title単位）
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
        if r['published_at']:
            lines.append(f"- 公開推定(JST): {r['published_at']}")
        if r['published_raw']:
            lines.append(f"- 抽出元テキスト: {r['published_raw']}")
        if r['source_list']:
            lines.append(f"- 取得元リスト: {r['source_list']}")
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
