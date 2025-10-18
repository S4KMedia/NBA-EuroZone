#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS -> Hugo posts with professional rewrite (sports journalism style)
- Bilingual: EN & EL directly via Cloudflare Workers AI
- Not-too-short newsroom summary (3–7 paragraphs)
- Adds source attribution at the end
- Dedupe via .state/posted.json
- Cover download from enclosure/media/first <img>
"""

import os, re, json, time, hashlib, pathlib, html, asyncio
from datetime import datetime
from urllib.parse import urlparse
from typing import Dict, Any, Optional, List, Tuple

import yaml
import feedparser
import frontmatter
import httpx
from bs4 import BeautifulSoup
from slugify import slugify

# --- Paths ---
ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTENT_EN = ROOT / "content" / "en" / "posts"
CONTENT_EL = ROOT / "content" / "el" / "posts"
COVERS_DIR = ROOT / "static" / "images" / "covers"
STATE_DIR = ROOT / ".state"
STATE_FILE = STATE_DIR / "posted.json"
FEEDS_FILE = ROOT / "config" / "feeds.yml"

# --- Env / Config ---
DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "").strip()  # fallback only
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID", "").strip()
CF_API_TOKEN  = os.getenv("CF_API_TOKEN", "").strip()
CF_MODEL      = os.getenv("CF_MODEL", "@cf/meta/llama-3.1-70b-instruct").strip()

TIMEZONE_OFFSET = "+02:00"  # ok for timestamp string

# ---------- helpers ----------

def load_yaml(path: pathlib.Path):
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_state():
    if not STATE_FILE.exists():
        return {"seen": {}}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": {}}

def save_state(state):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def hash_id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]

def clean_text(t: str) -> str:
    t = html.unescape(t or "")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def build_date(dt_struct) -> str:
    try:
        if dt_struct:
            return datetime(*dt_struct[:6]).isoformat() + TIMEZONE_OFFSET
    except Exception:
        pass
    return datetime.now().isoformat() + TIMEZONE_OFFSET

def first_image_from_html(html_text: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html_text or "", "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            return img["src"]
    except Exception:
        pass
    return None

async def download_image(client: httpx.AsyncClient, url: str, dest_dir: pathlib.Path, base_slug: str) -> Optional[str]:
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        ext = pathlib.Path(urlparse(url).path).suffix.lower()
        if not ext or len(ext) > 5:
            ext = ".jpg"
        filename = f"{base_slug}-cover{ext}"
        dest_path = dest_dir / filename
        r = await client.get(url, timeout=30)
        if r.status_code == 200 and r.content:
            dest_path.write_bytes(r.content)
            return f"/images/covers/{filename}"
    except Exception:
        return None
    return None

def ensure_dirs():
    CONTENT_EN.mkdir(parents=True, exist_ok=True)
    CONTENT_EL.mkdir(parents=True, exist_ok=True)
    COVERS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Cloudflare Workers AI ----------

def cf_endpoint() -> str:
    if not CF_ACCOUNT_ID:
        raise RuntimeError("CF_ACCOUNT_ID env is missing")
    model = CF_MODEL or "@cf/meta/llama-3.1-70b-instruct"
    return f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/ai/run/{model}"

CF_REWRITE_SYSTEM_PROMPT = (
    "You are a professional European basketball news writer. "
    "Rewrite the provided feed content into an original, newsroom-quality article.\n"
    "Constraints:\n"
    "1) Do NOT copy phrases verbatim; produce fresh wording.\n"
    "2) Aim for 3–7 short paragraphs (not too short overall).\n"
    "3) Keep a neutral, informative tone; add safe, commonly known context where helpful.\n"
    "4) Avoid speculation, stick to what's in the feed.\n"
    "5) No bullet lists unless necessary; write cohesive paragraphs.\n"
    "6) End the article with a single line 'Source:' as provided."
)

def build_user_prompt(title: str, text: str, lang: str, source_name: str, source_url: str) -> dict:
    lang_note = "Write the article in English." if lang.lower().startswith("en") else \
                "Write the article in Greek, in clean journalistic Greek."
    user = (
        f"{lang_note}\n\n"
        f"Title: {title}\n\n"
        f"Feed content:\n{text}\n\n"
        f"Use this exact source line at the end:\n"
        f"{'Source:' if lang.lower().startswith('en') else 'Πηγή:'} {source_name} ({source_url})"
    )
    return {
        "messages": [
            {"role": "system", "content": CF_REWRITE_SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ]
    }

async def cf_rewrite(client: httpx.AsyncClient, title: str, text: str, lang: str, source_name: str, source_url: str) -> Optional[str]:
    if not CF_API_TOKEN:
        raise RuntimeError("CF_API_TOKEN env is missing")
    payload = build_user_prompt(title, text, lang, source_name, source_url)
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    try:
        r = await client.post(cf_endpoint(), json=payload, headers=headers, timeout=60)
        r.raise_for_status()
        jd = r.json()
        result = jd.get("result") or {}
        out = result.get("response")
        if not out:
            output = result.get("output")
            if isinstance(output, list) and output:
                node = output[0]
                if isinstance(node, dict):
                    content = node.get("content")
                    if isinstance(content, list) and content and isinstance(content[0], dict):
                        out = content[0].get("text")
        return (out or "").strip() or None
    except Exception as e:
        print(f"[CF] rewrite failed ({lang}): {e}")
        return None

# ---------- DeepL fallback (EL only) ----------
DEEPL_ENDPOINT = "https://api-free.deepl.com/v2/translate"

async def deepl_translate(client: httpx.AsyncClient, text: str) -> str:
    if not DEEPL_API_KEY:
        return text
    try:
        data = {"auth_key": DEEPL_API_KEY, "text": text, "source_lang": "EN", "target_lang": "EL"}
        r = await client.post(DEEPL_ENDPOINT, data=data, timeout=30)
        r.raise_for_status()
        jd = r.json()
        return jd["translations"][0]["text"]
    except Exception:
        return text

# ---------- feed parsing ----------

def select_feed_text(entry) -> Tuple[str, str]:
    title = clean_text(entry.get("title") or "")
    raw_html = None
    if entry.get("content"):
        try:
            raw_html = entry["content"][0].get("value")
        except Exception:
            raw_html = None
    if not raw_html:
        raw_html = entry.get("summary") or entry.get("description") or ""
    soup = BeautifulSoup(raw_html or "", "html.parser")
    paragraphs = [clean_text(p.get_text(" ", strip=True)) for p in soup.find_all(["p","div","li"])
                  if clean_text(p.get_text(" ", strip=True))]
    text = "\n\n".join(paragraphs) if paragraphs else clean_text(soup.get_text(" ", strip=True))
    return title, text

async def process_entry(client: httpx.AsyncClient, feed_meta: dict, entry, used_slugs_en: set):
    link = entry.get("link") or ""
    source_host = urlparse(link).netloc or urlparse(feed_meta.get("url","")).netloc or "source"
    source_name = source_host.replace("www.", "")
    published = build_date(entry.get("published_parsed"))

    title, feed_text = select_feed_text(entry)
    if not title and not feed_text:
        return None

    base_slug = slugify(title or link, lowercase=True, max_length=80) or "news"
    slug = base_slug if base_slug not in used_slugs_en else f"{base_slug}-{hash_id(link)[:6]}"
    used_slugs_en.add(slug)

    cover_url = None
    enc = entry.get("enclosures") or []
    if enc:
        cover_url = enc[0].get("href")
    if not cover_url and entry.get("media_content"):
        try:
            cover_url = entry["media_content"][0].get("url")
        except Exception:
            pass
    if not cover_url and entry.get("summary"):
        maybe = first_image_from_html(entry["summary"])
        if maybe:
            cover_url = maybe
    cover_rel = None
    if cover_url:
        cover_rel = await download_image(client, cover_url, COVERS_DIR, slug)

    # rewrite EN + EL
    body_en = await cf_rewrite(client, title, feed_text, "EN", source_name, link)
    body_el = await cf_rewrite(client, title, feed_text, "EL", source_name, link)
    if not body_el and body_en and DEEPL_API_KEY:
        body_el = await deepl_translate(client, body_en)

    if not body_en:
        body_en = f"{title}\n\n{feed_text[:1000]}\n\nSource: {source_name} ({link})"
    if not body_el:
        body_el = f"{title}\n\n{feed_text[:1000]}\n\nΠηγή: {source_name} ({link})"

    base_tags = list(set((entry.get("tags") and [t["term"] for t in entry["tags"]]) or []) | set(feed_meta.get("tags", [])))
    countries = [c for c in [feed_meta.get("country")] if c]
    teams = [t for t in [feed_meta.get("team")] if t]

    fm_common = {
        "date": published,
        "draft": False,
        "categories": ["news"],
        "tags": base_tags,
        "players": [],
        "teams": teams,
        "leagues": ["NBA"],
        "countries": countries,
        "topics": [],
        "type": "posts",
    }
    if cover_rel:
        fm_common["cover"] = {"image": cover_rel, "alt": "", "caption": ""}

    fm_en = {**fm_common, "title": title or "Update", "description": (feed_text[:240] or "")}
    fm_el = {**fm_common, "title": title or "Ενημέρωση", "description": (feed_text[:240] or "")}

    path_en = CONTENT_EN / f"{slug}.md"
    path_el = CONTENT_EL / f"{slug}.md"

    with path_en.open("w", encoding="utf-8") as f:
        frontmatter.dump(frontmatter.Post(body_en.strip(), **fm_en), f)
    with path_el.open("w", encoding="utf-8") as f:
        frontmatter.dump(frontmatter.Post(body_el.strip(), **fm_el), f)

    return path_en, path_el

async def process_feed(client: httpx.AsyncClient, feed_meta: dict, state: dict) -> int:
    url = feed_meta["url"]
    d = feedparser.parse(url)
    seen = state.setdefault("seen", {})
    new_count = 0

    used_slugs_en = {p.stem for p in CONTENT_EN.glob("*.md")}

    for entry in d.entries[:20]:
        link = entry.get("link") or ""
        title = clean_text(entry.get("title") or "")
        sig = hash_id(link or title or url)
        if sig in seen:
            continue

        created = await process_entry(client, feed_meta, entry, used_slugs_en)
        if created:
            path_en, _ = created
            seen[sig] = {"link": link, "title": title, "slug": path_en.stem, "ts": int(time.time())}
            new_count += 1

    return new_count

async def main():
    ensure_dirs()
    feeds_spec = load_yaml(FEEDS_FILE) or {}
    sources = feeds_spec.get("sources", [])
    state = load_state()

    headers = {"User-Agent": "NBA-EuroZone Rewriter/1.0"}
    async with httpx.AsyncClient(follow_redirects=True, headers=headers) as client:
        total_new = 0
        for feed in sources:
            try:
                added = await process_feed(client, feed, state)
                total_new += added
            except Exception as e:
                print(f"[WARN] Feed failed: {feed.get('url')} :: {e}")

    save_state(state)
    print(f"Done. New posts: {total_new}")

if __name__ == "__main__":
    asyncio.run(main())
