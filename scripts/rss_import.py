#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, time, hashlib, pathlib, textwrap
from datetime import datetime, timezone
from urllib.parse import urlparse
import yaml, feedparser, requests, frontmatter
from bs4 import BeautifulSoup
from slugify import slugify

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTENT_EN = ROOT / "content" / "en" / "posts"
CONTENT_EL = ROOT / "content" / "el" / "posts"
STATE = ROOT / ".state"
STATE.mkdir(exist_ok=True)
SEEN_FILE = STATE / "seen.json"

DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "").strip()
DEEPL_TARGET_LANG = os.getenv("DEEPL_TARGET_LANG", "EL").upper()

def load_seen():
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_seen(seen):
    SEEN_FILE.write_text(json.dumps(seen, ensure_ascii=False, indent=2), encoding="utf-8")

def read_feeds():
    cfg = yaml.safe_load((ROOT / "config" / "feeds.yml").read_text(encoding="utf-8"))
    return cfg.get("sources", [])

def fetch_html(url, timeout=12):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass
    return ""

def extract_main_text(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    # remove scripts/ads
    for tag in soup(["script","style","noscript","iframe"]):
        tag.decompose()
    # naive main content (keeps paragraphs)
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = "\n\n".join([p for p in paragraphs if len(p) > 40])
    return text.strip()

def clean_title(t):
    t = re.sub(r"\s+", " ", t or "").strip()
    # small newsroom touch
    t = t.replace(" - ", ": ").strip()
    return t

def deepl_translate(text, target_lang="EL"):
    if not DEEPL_API_KEY or not text.strip():
        return ""
    try:
        r = requests.post(
            "https://api-free.deepl.com/v2/translate",
            data={"auth_key": DEEPL_API_KEY, "text": text, "target_lang": target_lang},
            timeout=20,
        )
        j = r.json()
        return j["translations"][0]["text"]
    except Exception:
        return ""

def write_post(lang, slug, title, date_iso, body, source_url, tags=None, categories=None, extra=None):
    folder = CONTENT_EN if lang=="en" else CONTENT_EL
    path = folder / slug / "index.md"
    path.parent.mkdir(parents=True, exist_ok=True)

    fm = {
        "title": title,
        "date": date_iso,
        "categories": categories or ["news"],
        "tags": tags or [],
        "source": source_url,
        "draft": False,
    }
    if extra: fm.update(extra)

    post = frontmatter.Post(body, **fm)
    path.write_text(frontmatter.dumps(post), encoding="utf-8")
    return path

def main():
    CONTENT_EN.mkdir(parents=True, exist_ok=True)
    CONTENT_EL.mkdir(parents=True, exist_ok=True)

    seen = load_seen()
    feeds = read_feeds()
    new_count = 0

    for feed in feeds:
        d = feedparser.parse(feed["url"])
        for entry in d.entries[:6]:  # limit per run
            guid = entry.get("id") or entry.get("link")
            if not guid: 
                continue
            h = hashlib.sha1(guid.encode("utf-8")).hexdigest()
            if h in seen:
                continue

            title = clean_title(entry.get("title","").strip())
            link = entry.get("link","").strip()
            date = entry.get("published_parsed")
            dt = datetime.fromtimestamp(time.mktime(date), tz=timezone.utc) if date else datetime.now(timezone.utc)
            date_iso = dt.isoformat()

