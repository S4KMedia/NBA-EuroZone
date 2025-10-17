#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS → Full article → rewrite → DeepL(EL) → smart tags → images (R2 optional) → Hugo posts EN/EL.
Safe defaults: αν λείπουν DeepL/R2 secrets, συνεχίζει (EN μόνο ή τοπικές εικόνες).
"""

import os, re, json, time, hashlib, pathlib, textwrap, io
from datetime import datetime, timezone
from urllib.parse import urlparse
import yaml, feedparser, requests, frontmatter
from bs4 import BeautifulSoup
from slugify import slugify
from PIL import Image

# ---------- Paths ----------
ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTENT_EN = ROOT / "content" / "en" / "posts"
CONTENT_EL = ROOT / "content" / "el" / "posts"
STATE = ROOT / ".state"; STATE.mkdir(exist_ok=True)
SEEN_FILE = STATE / "seen.json"
IMG_LOCAL_DIR = ROOT / "static" / "images" / "covers"; IMG_LOCAL_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Env (DeepL / R2) ----------
DEEPL_API_KEY = os.getenv("DEEPL_API_KEY", "").strip()
DEEPL_TARGET_LANG = "EL"

R2_ACCOUNT_ID = os.getenv("CF_R2_ACCOUNT_ID", "").strip()
R2_ACCESS_KEY_ID = os.getenv("CF_R2_ACCESS_KEY_ID", "").strip()
R2_SECRET_ACCESS_KEY = os.getenv("CF_R2_SECRET_ACCESS_KEY", "").strip()
R2_BUCKET = os.getenv("CF_R2_BUCKET", "").strip()
R2_PUBLIC_BASEURL = os.getenv("CF_R2_PUBLIC_BASEURL", "").strip().rstrip("/")

USE_R2 = all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET, R2_PUBLIC_BASEURL])

# ---------- Smart tagging lists (expand later) ----------
TAG_DB = {
    "players": ["Giannis Antetokounmpo","Luka Doncic","Victor Wembanyama","Nikola Jokic","Rudy Gobert",
                "Domantas Sabonis","Franz Wagner","Bogdan Bogdanovic","Alperen Sengun","Kristaps Porzingis"],
    "teams": ["Milwaukee Bucks","Dallas Mavericks","San Antonio Spurs","Denver Nuggets","Boston Celtics",
              "Real Madrid","Fenerbahce","Barcelona","Panathinaikos","Olympiacos"],
    "leagues": ["NBA","EuroLeague","ACB","BBL","LNB","Serie A"],
    "countries": ["Greece","Slovenia","France","Serbia","Spain","Italy","Germany","Turkey","Lithuania"],
    "topics": ["transfer","draft","playoffs","contract","injury","preseason"]
}

# ---------- Helpers ----------
def load_seen():
    if SEEN_FILE.exists():
        try: return json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_seen(d):
    SEEN_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def read_feeds():
    cfg = yaml.safe_load((ROOT / "config" / "feeds.yml").read_text(encoding="utf-8"))
    return cfg.get("sources", [])

def http_get(url, timeout=15):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0 (EuroZone Bot)"})
        if r.status_code == 200: return r
    except Exception:
        pass
    return None

def extract_fulltext(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script","style","noscript","iframe"]): t.decompose()
    # remove common share blocks
    for c in soup.select('[class*="share"], [class*="social"], [id*="share"]'): c.decompose()
    ps = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = "\n\n".join([p for p in ps if len(p) > 40])
    return text.strip()

def get_og_image(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", property="og:image")
    if og and og.get("content"): return og["content"]
    # fallback: first <img> with decent size
    img = soup.find("img")
    if img and img.get("src"): return img["src"]
    return ""

def clean_title(t):
    t = (t or "").strip()
    t = re.sub(r"\s+", " ", t)
    t = t.replace(" - ", ": ").strip()
    return t

def newsroom_rewrite(lead):
    # Μικρό, “καθαρό” lead (χωρίς LLM για σταθερότητα)
    lead = re.sub(r"\s+", " ", lead).strip()
    return lead if len(lead) <= 320 else lead[:317] + "..."

def deepl_translate(text, target_lang="EL"):
    if not DEEPL_API_KEY or not text.strip(): return ""
    try:
        r = requests.post(
            "https://api-free.deepl.com/v2/translate",
            data={"auth_key": DEEPL_API_KEY, "text": text, "target_lang": target_lang},
            timeout=30,
        )
        j = r.json()
        return j["translations"][0]["text"]
    except Exception:
        return ""

def pick_tags(text):
    t = text.lower()
    tags = {"players":[], "teams":[], "leagues":[], "countries":[], "topics":[]}
    for k, vals in TAG_DB.items():
        for v in vals:
            if v.lower() in t: tags[k].append(v)
    return tags

def optimize_image_bytes(raw):
    try:
        im = Image.open(io.BytesIO(raw)).convert("RGB")
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=82, optimize=True, progressive=True)
        return out.getvalue()
    except Exception:
        return raw  # fallback

def upload_r2(key, data):
    if not USE_R2: return ""
    try:
        import boto3
        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        )
        s3.put_object(Bucket=R2_BUCKET, Key=key, Body=data, ContentType="image/jpeg", ACL="public-read")
        return f"{R2_PUBLIC_BASEURL}/{key}"
    except Exception:
        return ""

def store_cover(slug, img_url):
    if not img_url: return "", ""
    r = http_get(img_url, timeout=20)
    if not r: return "", ""
    data = optimize_image_bytes(r.content)
    key = f"covers/{slug}-cover.jpg"

    # Prefer R2
    if USE_R2:
        public = upload_r2(key, data)
        if public: return public, "r2"

    # Local fallback (served from /images/covers/)
    fp = IMG_LOCAL_DIR / f"{slug}-cover.jpg"
    fp.write_bytes(data)
    return f"/images/covers/{slug}-cover.jpg", "local"

def write_post(lang, slug, title, date_iso, body_md, source_url, tags_dict, featured_image):
    folder = CONTENT_EN if lang=="en" else CONTENT_EL
    path = folder / slug / "index.md"
    path.parent.mkdir(parents=True, exist_ok=True)

    fm = {
        "title": title,
        "date": date_iso,
        "categories": ["news"],
        "tags": sorted(set(sum(tags_dict.values(), []))) if tags_dict else [],
        "players": tags_dict.get("players", []),
        "teams": tags_dict.get("teams", []),
        "leagues": tags_dict.get("leagues", []),
        "countries": tags_dict.get("countries", []),
        "topics": tags_dict.get("topics", []),
        "source": source_url,
        "featured_image": featured_image or "",
        "draft": False,
    }
    post = frontmatter.Post(body_md, **fm)
    path.write_text(frontmatter.dumps(post), encoding="utf-8")
    return path

def main():
    CONTENT_EN.mkdir(parents=True, exist_ok=True)
    CONTENT_EL.mkdir(parents=True, exist_ok=True)

    seen = load_seen()
    feeds = read_feeds()
    created = 0

    for feed in feeds:
        d = feedparser.parse(feed["url"])
        for entry in d.entries[:6]:
            guid = entry.get("id") or entry.get("link")
            if not guid: 
                continue
            h = hashlib.sha1(guid.encode("utf-8")).hexdigest()
            if h in seen:
                continue

            title = clean_title(entry.get("title",""))
            link = entry.get("link","").strip()
            date_t = entry.get("published_parsed")
            dt = datetime.fromtimestamp(time.mktime(date_t), tz=timezone.utc) if date_t else datetime.now(timezone.utc)
            date_iso = dt.isoformat()

            # Fetch & extract full text + og:image
            html = http_get(link).text if http_get(link) else ""
            body_txt = extract_fulltext(html) if html else (entry.get("summary","") or "")
            lead = newsroom_rewrite(body_txt.split("\n\n")[0] if body_txt else "")
            og_img = get_og_image(html, link) if html else ""

            # Image handling
            slug = slugify(title)[:80] or slugify(link)[:80]
            featured_url, img_where = store_cover(slug, og_img)

            # Build EN body
            body_en = ""
            if lead: body_en += f"{lead}\n\n"
            if body_txt and lead and not body_txt.startswith(lead):
                body_en += body_txt
            elif body_txt and not lead:
                body_en += body_txt

            # Smart tags
            tags_dict = pick_tags(f"{title}\n{body_en}")

            # Write EN
            write_post("en", slug, title, date_iso, body_en.strip(), link, tags_dict, featured_url)

            # Translate to EL (optional)
            if DEEPL_API_KEY:
                el_title = deepl_translate(title, DEEPL_TARGET_LANG) or title
                el_body = deepl_translate(body_en, DEEPL_TARGET_LANG) or body_en
                write_post("el", slug, el_title, date_iso, el_body.strip(), link, tags_dict, featured_url)

            seen[h] = {"link": link, "slug": slug, "created": date_iso}
            created += 1

    save_seen(seen)
    print(f"[EUROZONE] Created/updated posts: {created}")

if __name__ == "__main__":
    main()
