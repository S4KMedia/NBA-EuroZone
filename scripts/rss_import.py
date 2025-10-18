#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS -> Hugo bilingual posts (EN primary, optional EL via DeepL)
- Reads feeds from config/feeds.yml
- Dedupe via .state/posted.json
- Writes to content/en/posts/ and content/el/posts/
- Downloads cover images to static/images/covers/
"""

import os, re, json, time, hashlib, pathlib, html
from datetime import datetime
from urllib.parse import urlparse
from typing import Dict, Any, Optional, List

import yaml
import feedparser
import frontmatter
import httpx
from bs4 import BeautifulSoup
from slugify import slugify

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTENT_EN = ROOT / "content" / "en" / "posts"
CONTENT_EL = ROOT / "content" / "el" / "posts"
COVERS_DIR = ROOT / "static" / "images" / "covers"
STATE_DIR = ROOT / ".state"
STATE_FILE = STATE_DIR / "posted.json"
FEEDS_FILE = ROOT / "config" / "feeds.yml"

DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "").strip()
DEEPL_ENDPOINT = "https://api-free.deepl.com/v2/translate"  # free endpoint
TIMEZONE_OFFSET = "+02:00"  # Europe/Berlin (χειμερινή/θερινή αλλά ok για timestamp string)

# ---------- helpers ----------

def load_yaml(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {"seen": {}}
    try:
        with STATE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": {}}

def save_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def hash_id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:24]

def first_image_from_html(html_text: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            return img["src"]
    except Exception:
        pass
    return None

async def download_image(client: httpx.AsyncClient, url: str, dest_dir: pathlib.Path, base_slug: str) -> Optional[str]:
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        parsed = urlparse(url)
        ext = pathlib.Path(parsed.path).suffix.lower()
        if not ext or len(ext) > 5:
            ext = ".jpg"
        filename = f"{base_slug}-cover{ext}"
        dest_path = dest_dir / filename
        r = await client.get(url, timeout=20)
        if r.status_code == 200 and r.content:
            dest_path.write_bytes(r.content)
            rel = f"/images/covers/{filename}"
            return rel
    except Exception:
        return None
    return None

def clean_text(t: str) -> str:
    t = html.unescape(t or "")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def build_date(dt_struct) -> str:
    # feedparser returns time.struct_time or None
    try:
        if dt_struct:
            return datetime(*dt_struct[:6]).isoformat() + TIMEZONE_OFFSET
    except Exception:
        pass
    # fallback: now
    return datetime.now().isoformat() + TIMEZONE_OFFSET

def uniq_slug(base: str, used: set) -> str:
    s = slugify(base, lowercase=True, max_length=80)
    if s not in used:
        return s
    i = 2
    while f"{s}-{i}" in used:
        i += 1
    return f"{s}-{i}"

async def deepl_translate(client: httpx.AsyncClient, text: str, source_lang: str, target_lang: str) -> str:
    if not DEEPL_API_KEY:
        return text
    try:
        data = {
            "auth_key": DEEPL_API_KEY,
            "text": text,
            "source_lang": source_lang.upper(),
            "target_lang": target_lang.upper(),
        }
        r = await client.post(DEEPL_ENDPOINT, data=data, timeout=30)
        r.raise_for_status()
        jd = r.json()
        return jd["translations"][0]["text"]
    except Exception:
        # on any failure, return original
        return text

def ensure_dirs():
    CONTENT_EN.mkdir(parents=True, exist_ok=True)
    CONTENT_EL.mkdir(parents=True, exist_ok=True)
    COVERS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- main ----------

async def process_feed(client: httpx.AsyncClient, feed: Dict[str, Any], state: Dict[str, Any]) -> int:
    """
    Returns number of new posts created.
    feed schema example:
      - country: Slovenia
        team: Mavericks
        url: https://...
        tags: ["Slovenia","NBA","Luka Doncic"]
    """
    url = feed["url"]
    allow_tags = feed.get("tags", [])
    d = feedparser.parse(url)

    seen = state.setdefault("seen", {})
    new_count = 0

    used_slugs_en = {p.name for p in CONTENT_EN.glob("*.md")}
    used_slugs_el = {p.name for p in CONTENT_EL.glob("*.md")}

    for entry in d.entries[:20]:  # limit per run
        link = entry.get("link") or ""
        title = clean_text(entry.get("title") or "")
        summary = clean_text(entry.get("summary") or entry.get("description") or "")
        published = build_date(entry.get("published_parsed"))

        # unique key
        sig = hash_id(link or title)
        if sig in seen:
            continue

        # choose cover
        cover_url = None
        # (a) enclosure
        enc = entry.get("enclosures") or []
        if enc:
            cover_url = enc[0].get("href")
        # (b) media_content
        if not cover_url and entry.get("media_content"):
            try:
                cover_url = entry["media_content"][0].get("url")
            except Exception:
                pass
        # (c) first img from summary
        if not cover_url and summary:
            img = first_image_from_html(summary)
            if img:
                cover_url = img

        # build slug
        base_slug = slugify(title or link, lowercase=True, max_length=80)
        slug_en = uniq_slug(base_slug, used_slugs_en)
        used_slugs_en.add(slug_en)

        cover_rel = None
        if cover_url:
            try:
                cover_rel = await download_image(client, cover_url, COVERS_DIR, slug_en)
            except Exception:
                cover_rel = None

        # front matter EN
        fm_en = {
            "title": title or "Untitled",
            "date": published,
            "draft": False,
            "description": summary[:240] if summary else "",
            "tags": list(set((entry.get("tags") and [t["term"] for t in entry["tags"]]) or []) | set(allow_tags)),
            "categories": ["news"],
            "players": [],
            "teams": [],
            "leagues": ["NBA"],
            "countries": list({feed.get("country")} - {None}),
            "topics": [],
        }
        if cover_rel:
            fm_en["cover"] = {
                "image": cover_rel,
                "alt": "",
                "caption": "",
            }

        # content EN (very short – link out)
        body_en = []
        body_en.append(f"> Source: [{urlparse(link).netloc}]({link})")
        body_en.append("")
        body_en.append("*(Auto-imported via RSS. Short excerpt; please read the original source for full context.)*")
        content_en = "\n".join(body_en)

        # write EN
        post_path_en = CONTENT_EN / f"{slug_en}.md"
        post_en = frontmatter.Post(content_en, **fm_en)
        with post_path_en.open("w", encoding="utf-8") as f:
            frontmatter.dump(post_en, f)

        # optional EL via DeepL
        if DEEPL_API_KEY:
            title_el = await deepl_translate(client, title, "EN", "EL") if title else ""
            desc_el = await deepl_translate(client, fm_en["description"], "EN", "EL") if fm_en.get("description") else ""
            body_el_txt = await deepl_translate(client, "Auto-imported via RSS. Short excerpt; please read the original source for full context.", "EN", "EL")
            body_el = []
            body_el.append(f"> Πηγή: [{urlparse(link).netloc}]({link})")
            body_el.append("")
            body_el.append(f"*({body_el_txt})*")
            content_el = "\n".join(body_el)

            fm_el = dict(fm_en)
            fm_el["title"] = title_el or fm_en["title"]
            fm_el["description"] = desc_el or fm_en.get("description") or ""
            # countries in Greek? άστα όπως είναι προς το παρόν
            post_path_el = CONTENT_EL / f"{slug_en}.md"  # ίδιο slug για απλότητα
            post_el = frontmatter.Post(content_el, **fm_el)
            with post_path_el.open("w", encoding="utf-8") as f:
                frontmatter.dump(post_el, f)

        # mark as seen
        seen[sig] = {
            "link": link,
            "title": title,
            "slug": slug_en,
            "ts": int(time.time()),
        }
        new_count += 1

    return new_count

async def main():
    ensure_dirs()
    feeds_spec = load_yaml(FEEDS_FILE) or {}
    sources = feeds_spec.get("sources", [])

    state = load_state()

    async with httpx.AsyncClient(follow_redirects=True, headers={"User-Agent":"NBA-EuroZone RSS Importer/1.0"}) as client:
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
    import asyncio
    asyncio.run(main())
