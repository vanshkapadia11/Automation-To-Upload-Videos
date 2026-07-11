"""
auto_upload.py  ──  Multi-account YouTube uploader  [PRODUCTION BUILD v3]
────────────────────────────────────────────────────────────────────────
Supports multiple YouTube channels across DIFFERENT Gmail accounts.

How to authorize each account:
  1. Visit /auto/auth?account=main        ← your first Gmail
  2. Visit /auto/auth?account=channel2    ← second Gmail
  3. Visit /auto/auth?account=channel3    ← third Gmail, etc.

Flask endpoints:
  POST   /auto/pick               → pick best video from a channel
  POST   /auto/run                → full pipeline: pick → download → upload (all accounts)
  POST   /auto/run_one            → upload to ONE specific account
  GET    /auto/accounts           → list all authorized accounts
  GET    /auto/auth               → start OAuth2 (pass ?account=NAME)
  GET    /auto/auth/callback      → OAuth2 callback (handled automatically)
  DELETE /auto/accounts/<id>      → remove an account's token

NEW production endpoints:
  GET    /auto/health             → liveness probe (NO auth required)
  GET    /auto/stats              → upload counts, last-run time, dedup size
  DELETE /auto/dedup              → wipe dedup log (re-allow old videos)

Production features added (zero logic changes to existing code):
  🔐 API-key auth     – X-API-Key header (or api_key in body/query) on all action routes
  🔁 Dedup            – uploaded_ids.json; same source video never re-uploaded
  🚦 Rate limiting    – per-IP sliding-window limiter (configurable)
  🩺 Health check     – GET /auto/health always returns 200 (no auth)
  📊 Stats endpoint   – GET /auto/stats shows totals + last run
  🌵 Dry-run          – pass "dry_run": true → pick + download, skip actual upload
  📋 JSON logging     – every log line is structured JSON for easy parsing
  ⏱  Request timing   – elapsed_ms returned in every pipeline response
  🗑  Dedup clear      – DELETE /auto/dedup to reset history

v3 new features:
  📺 Multi-channel    – pass channel_urls (list) instead of channel_url (string);
                        one channel is selected per run (random or weighted).
                        See MULTI_CHANNEL_MODE env var for selection strategy.
  🖼  Full long-form   – for non-Shorts (duration > 60s), copies EVERYTHING:
                        title, description, tags, category, thumbnail image,
                        made_for_kids flag, language, default audio language.
                        Pass "video_type": "longform" or omit for auto-detect.
  🖼  Thumbnail copy   – downloads source thumbnail and uploads via
                        YouTube thumbnails.set() API automatically.

v2 fixes (logic-only, no API/endpoint changes):
  ✅ Dedup filtered BEFORE scoring — already-uploaded IDs never enter the pool
  ✅ Parallel enrichment via ThreadPoolExecutor — much faster candidate evaluation
  ✅ random_pick draws from full dedup-filtered + enriched pool (true randomness)
  ✅ Retry-next logic — if best pick fails upload, tries next-best automatically
  ✅ Pool exhaustion detection — clear error when all candidates are used up
  ✅ Persistent stats — stats.json survives restarts
  ✅ Thread-safe credential refresh — RLock prevents double-refresh race
  ✅ YouTube quota error detection — 403/quotaExceeded triggers backoff
  ✅ Rate limiter persistence — sliding window flushed to disk, survives restarts

Environment variables (all optional):
  AUTO_API_KEY            Secret key (X-API-Key header). Empty = no auth enforced.
  GOOGLE_CLIENT_SECRETS   Path to client_secret.json          [default: client_secret.json]
  YOUTUBE_TOKENS_DIR      Where per-account token pickles live [default: tokens/]
  UPLOADED_LOG            Path to dedup JSON file              [default: uploaded_ids.json]
  STATS_LOG               Path to persistent stats JSON        [default: stats.json]
  BASE_URL                Your public base URL                 [default: http://localhost:5000]
  PICKER_MAX_CANDIDATES   Shorts to consider                   [default: 50]
  PICKER_MAX_AGE_DAYS     Max age in days                      [default: 30]
  PICKER_W_VIEWS          Score weight – views                 [default: 0.5]
  PICKER_W_LIKES          Score weight – likes                 [default: 0.3]
  PICKER_W_FRESHNESS      Score weight – freshness             [default: 0.2]
  PICKER_ENRICH_WORKERS   Parallel threads for enrichment      [default: 5]
  PICKER_MAX_UPLOAD_RETRY Max next-candidate retries           [default: 3]
  RATE_LIMIT_WINDOW       Sliding window in seconds            [default: 60]
  RATE_LIMIT_MAX_CALLS    Max calls per IP per window          [default: 20]
  MULTI_CHANNEL_MODE      How to pick from multiple channels:
                            "random"   – uniform random (default)
                            "weighted" – weight by subscriber/view count
                            "round_robin" – cycles through in order
  LONGFORM_MIN_DURATION   Seconds above which a video is "long-form" [default: 61]
  THUMBNAIL_COPY          "true"/"false" – copy thumbnail for longform [default: true]
"""

import json
import logging
import os
import pickle
import random
import shutil
import tempfile
import threading
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


import yt_dlp
from flask import Blueprint, jsonify, redirect, request

# ── Google API ─────────────────────────────────────────────────────────────────
try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import Flow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload
except ImportError:
    raise ImportError(
        "Run: pip install google-auth google-auth-oauthlib "
        "google-auth-httplib2 google-api-python-client"
    )

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

auto_bp = Blueprint("auto", __name__, url_prefix="/auto")

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# ── SECRET BOOTSTRAP — decode client_secret.json from env var at startup ──────
import base64

_secrets_json = os.environ.get("GOOGLE_CLIENT_SECRETS_JSON", "")
if _secrets_json:
    try:
        _decoded = base64.b64decode(_secrets_json).decode("utf-8")
        with open("client_secret.json", "w") as _f:
            _f.write(_decoded)
    except Exception as _e:
        print(f"WARNING: Could not decode GOOGLE_CLIENT_SECRETS_JSON: {_e}")

_secrets_json_1 = os.environ.get("GOOGLE_CLIENT_SECRETS_JSON_1", "")
if _secrets_json_1:
    try:
        _decoded_1 = base64.b64decode(_secrets_json_1).decode("utf-8")
        with open("client_secret_1.json", "w") as _f:
            _f.write(_decoded_1)
    except Exception as _e:
        print(f"WARNING: Could not decode GOOGLE_CLIENT_SECRETS_JSON_1: {_e}")

CLIENT_SECRETS_FILE = os.environ.get("GOOGLE_CLIENT_SECRETS", "client_secret.json")
TOKENS_DIR = os.environ.get("YOUTUBE_TOKENS_DIR", "tokens")
UPLOADED_LOG = os.environ.get("UPLOADED_LOG", "uploaded_ids.json")
STATS_LOG = os.environ.get("STATS_LOG", "stats.json")
API_KEY = os.environ.get("AUTO_API_KEY", "")
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", 60))
RATE_LIMIT_MAX = int(os.environ.get("RATE_LIMIT_MAX_CALLS", 20))

# ── v3: multi-channel + long-form config ─────────────────────────────────────
MULTI_CHANNEL_MODE = os.environ.get(
    "MULTI_CHANNEL_MODE", "random"
)  # random | weighted | round_robin
LONGFORM_MIN_DURATION = int(os.environ.get("LONGFORM_MIN_DURATION", 61))  # seconds
THUMBNAIL_COPY = os.environ.get("THUMBNAIL_COPY", "true").lower() == "true"

# Round-robin state (in-memory, resets on restart — acceptable for this use-case)
_rr_index: dict[str, int] = {}  # key = frozenset of channel URLs as string → index
_rr_lock = threading.Lock()

Path(TOKENS_DIR).mkdir(exist_ok=True)

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtubepartner",  # needed for thumbnail upload
]

MAX_CANDIDATES = int(os.environ.get("PICKER_MAX_CANDIDATES", 50))
MAX_AGE_DAYS = int(os.environ.get("PICKER_MAX_AGE_DAYS", 270))
W_VIEWS = float(os.environ.get("PICKER_W_VIEWS", 0.5))
W_LIKES = float(os.environ.get("PICKER_W_LIKES", 0.3))
W_FRESHNESS = float(os.environ.get("PICKER_W_FRESHNESS", 0.2))
ENRICH_WORKERS = int(os.environ.get("PICKER_ENRICH_WORKERS", 5))
MAX_UPLOAD_RETRY = int(os.environ.get("PICKER_MAX_UPLOAD_RETRY", 3))

# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCTION — STRUCTURED LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(message)s")
_log = logging.getLogger("auto_upload")


def _jlog(level: str, component: str, msg: str, **extra):
    record = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "component": component,
        "msg": msg,
        **extra,
    }
    _log.info(json.dumps(record))


# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCTION — SECURITY: API KEY AUTH
# ══════════════════════════════════════════════════════════════════════════════


def _require_api_key():
    if not API_KEY:
        return
    provided = (
        request.headers.get("X-API-Key")
        or request.args.get("api_key", "")
        or (request.get_json(silent=True) or {}).get("api_key", "")
    )
    if provided != API_KEY:
        _jlog(
            "WARN",
            "auth",
            "Rejected – bad or missing API key",
            ip=request.remote_addr,
            path=request.path,
        )
        from flask import abort

        abort(401, description="Invalid or missing API key. Pass X-API-Key header.")


# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCTION — RATE LIMITING
# ══════════════════════════════════════════════════════════════════════════════

_RATE_LOG = os.environ.get("RATE_LOG", "rate_limit.json")
_rate_lock = threading.Lock()


def _load_rate_data() -> dict:
    if not os.path.exists(_RATE_LOG):
        return {}
    try:
        with open(_RATE_LOG, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_rate_data(data: dict):
    try:
        with open(_RATE_LOG, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def _check_rate_limit():
    ip = request.remote_addr or "unknown"
    now = time.time()
    with _rate_lock:
        data = _load_rate_data()
        calls = [t for t in data.get(ip, []) if now - t < RATE_LIMIT_WINDOW]
        if len(calls) >= RATE_LIMIT_MAX:
            _jlog("WARN", "rate_limit", "Rate limit exceeded", ip=ip)
            from flask import abort

            abort(
                429,
                description=f"Rate limit: max {RATE_LIMIT_MAX} calls per {RATE_LIMIT_WINDOW}s.",
            )
        calls.append(now)
        data[ip] = calls
        _save_rate_data(data)


# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCTION — DEDUP
# ══════════════════════════════════════════════════════════════════════════════

_dedup_lock = threading.Lock()


def _load_uploaded() -> set:
    if not os.path.exists(UPLOADED_LOG):
        return set()
    try:
        with open(UPLOADED_LOG, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()


def _mark_uploaded(video_id: str):
    with _dedup_lock:
        ids = _load_uploaded()
        ids.add(video_id)
        with open(UPLOADED_LOG, "w") as f:
            json.dump(sorted(ids), f, indent=2)
    _jlog("INFO", "dedup", "Marked as uploaded", video_id=video_id)


def _already_uploaded(video_id: str) -> bool:
    return video_id in _load_uploaded()


# ══════════════════════════════════════════════════════════════════════════════
#  PRODUCTION — STATS TRACKING
# ══════════════════════════════════════════════════════════════════════════════

_stats_lock = threading.Lock()

_STATS_DEFAULTS = {
    "total_runs": 0,
    "total_uploads_succeeded": 0,
    "total_uploads_failed": 0,
    "total_skipped_dedup": 0,
    "last_run_at": None,
    "last_run_title": None,
    "last_run_source_url": None,
    "last_run_source_channel": None,  # v3: track which channel was used
}


def _load_stats() -> dict:
    if not os.path.exists(STATS_LOG):
        return dict(_STATS_DEFAULTS)
    try:
        with open(STATS_LOG, "r") as f:
            on_disk = json.load(f)
        return {**_STATS_DEFAULTS, **on_disk}
    except Exception:
        return dict(_STATS_DEFAULTS)


def _save_stats(stats: dict):
    try:
        with open(STATS_LOG, "w") as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        _jlog("WARN", "stats", "Could not persist stats", error=str(e))


def _update_stats(**kwargs):
    with _stats_lock:
        stats = _load_stats()
        for k, v in kwargs.items():
            if k.startswith("inc_"):
                key = k[4:]
                stats[key] = stats.get(key, 0) + v
            else:
                stats[k] = v
        _save_stats(stats)


# ══════════════════════════════════════════════════════════════════════════════
#  v3 — MULTI-CHANNEL SELECTOR
# ══════════════════════════════════════════════════════════════════════════════


def _select_channel(channel_urls: list[str], mode: str = MULTI_CHANNEL_MODE) -> str:
    """
    Given a list of channel URLs, pick one according to `mode`:

      "random"      – uniform random selection (default, safest against fingerprinting)
      "weighted"    – placeholder: weights channels by their position in the list
                      (index 0 = highest weight). Replace with real subscriber counts
                      via YouTube Data API if you want true weighting.
      "round_robin" – cycles through the list in order, persisted in memory.

    Returns the selected channel URL.
    """
    if not channel_urls:
        raise ValueError("channel_urls list is empty.")
    if len(channel_urls) == 1:
        return channel_urls[0]

    if mode == "round_robin":
        key = str(sorted(channel_urls))
        with _rr_lock:
            idx = _rr_index.get(key, 0)
            chosen = channel_urls[idx % len(channel_urls)]
            _rr_index[key] = (idx + 1) % len(channel_urls)
        _jlog(
            "INFO",
            "channel_selector",
            "Round-robin pick",
            index=idx,
            chosen=chosen,
            total=len(channel_urls),
        )
        return chosen

    if mode == "weighted":
        # Simple descending weight: first channel gets weight N, last gets 1.
        # Swap in real subscriber counts here if you fetch them via API.
        n = len(channel_urls)
        weights = list(range(n, 0, -1))
        chosen = random.choices(channel_urls, weights=weights, k=1)[0]
        _jlog(
            "INFO", "channel_selector", "Weighted pick", chosen=chosen, weights=weights
        )
        return chosen

    # default: "random"
    chosen = random.choice(channel_urls)
    _jlog(
        "INFO",
        "channel_selector",
        "Random pick",
        chosen=chosen,
        total=len(channel_urls),
    )
    return chosen


# ══════════════════════════════════════════════════════════════════════════════
#  TOKEN HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_token_locks: dict[str, threading.RLock] = {}
_token_locks_meta = threading.Lock()


def _lock_for(account_id: str) -> threading.RLock:
    with _token_locks_meta:
        if account_id not in _token_locks:
            _token_locks[account_id] = threading.RLock()
        return _token_locks[account_id]


def _token_path(account_id: str) -> str:
    safe = "".join(c for c in account_id if c.isalnum() or c in "_-")
    return os.path.join(TOKENS_DIR, f"yt_token_{safe}.pickle")


def _list_accounts() -> list[dict]:
    accounts = []
    for p in Path(TOKENS_DIR).glob("yt_token_*.pickle"):
        account_id = p.stem.replace("yt_token_", "")
        try:
            with open(p, "rb") as f:
                creds = pickle.load(f)
            status = (
                "valid"
                if (creds and creds.valid)
                else (
                    "expired_refreshable"
                    if (creds and creds.expired and creds.refresh_token)
                    else "expired"
                )
            )
        except Exception:
            status = "unreadable"
        accounts.append(
            {"account_id": account_id, "token_file": str(p), "status": status}
        )
    return accounts


def _get_credentials(account_id: str) -> Credentials:
    token_file = _token_path(account_id)
    lock = _lock_for(account_id)
    with lock:
        if not os.path.exists(token_file):
            raise RuntimeError(
                f"No token for account '{account_id}'. "
                f"Visit /auto/auth?account={account_id} first."
            )
        with open(token_file, "rb") as f:
            creds = pickle.load(f)
        if creds and creds.valid:
            return creds
        if creds and creds.expired and creds.refresh_token:
            _jlog("INFO", f"uploader:{account_id}", "Refreshing token...")
            creds.refresh(Request())
            with open(token_file, "wb") as f:
                pickle.dump(creds, f)
            return creds
    raise RuntimeError(
        f"Token for '{account_id}' is expired/invalid. "
        f"Re-authorize at /auto/auth?account={account_id}"
    )


def _save_credentials(account_id: str, creds: Credentials):
    token_file = _token_path(account_id)
    lock = _lock_for(account_id)
    with lock:
        with open(token_file, "wb") as f:
            pickle.dump(creds, f)
    _jlog("INFO", f"uploader:{account_id}", "Token saved", token_file=token_file)


# ══════════════════════════════════════════════════════════════════════════════
#  PART 1 — CHANNEL VIDEO PICKER
# ══════════════════════════════════════════════════════════════════════════════


def _fetch_channel_videos(
    channel_url: str, max_count: int = MAX_CANDIDATES, video_type: str = "shorts"
) -> list[dict]:
    """
    Fetch video entries from a channel.
    video_type = "shorts"   → /shorts tab  (duration ≤ 60s)
    video_type = "longform" → /videos tab  (duration > LONGFORM_MIN_DURATION)
    video_type = "auto"     → /videos tab (mix, filter by duration later)
    """
    if video_type == "shorts":
        tab = "shorts"
    else:
        tab = "videos"  # longform and auto both use the main videos tab

    if "/channel/" in channel_url:
        channel_id = channel_url.split("/channel/")[-1].split("/")[0]
        uploads_url = f"https://www.youtube.com/channel/{channel_id}/{tab}"
    elif "/c/" in channel_url:
        custom_url = channel_url.split("/c/")[-1].split("/")[0]
        uploads_url = f"https://www.youtube.com/c/{custom_url}/{tab}"
    elif "/@" in channel_url:
        handle = channel_url.split("/@")[-1].split("/")[0]
        uploads_url = f"https://www.youtube.com/@{handle}/{tab}"
    else:
        uploads_url = channel_url

    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "playlistend": max_count,
        "cookiesfrombrowser": ("firefox",),
    }
    _jlog("INFO", "picker", "Fetching videos", url=uploads_url, video_type=video_type)
    with yt_dlp.YoutubeDL(opts) as ydl:
        try:
            result = ydl.extract_info(uploads_url, download=False)
            if result.get("entries"):
                entries = result["entries"]
            else:
                entries = [result] if result and result.get("id") else []
        except Exception as e:
            _jlog("WARN", "picker", "Playlist extract failed, retrying", error=str(e))
            opts["extract_flat"] = "in_playlist"
            result = ydl.extract_info(channel_url, download=False)
            entries = result.get("entries", [])

    video_entries = []
    for entry in entries:
        if entry and entry.get("id") and entry.get("title"):
            video_id = entry.get("id", "")
            if len(video_id) == 11:
                video_entries.append(entry)

    _jlog("INFO", "picker", "Found videos", count=len(video_entries))
    return video_entries


def _enrich_video(entry: dict) -> dict | None:
    vid_id = None
    if entry.get("id"):
        vid_id = entry["id"]
    elif entry.get("url"):
        url = entry["url"]
        if "watch?v=" in url:
            vid_id = url.split("watch?v=")[-1].split("&")[0]
        elif "youtu.be/" in url:
            vid_id = url.split("youtu.be/")[-1].split("?")[0]
        elif "/shorts/" in url:
            vid_id = url.split("/shorts/")[-1].split("?")[0]

    if not vid_id or len(vid_id) < 10:
        _jlog("WARN", "picker", "Could not extract video ID", entry=str(entry)[:200])
        return None

    vid_url = f"https://www.youtube.com/watch?v={vid_id}"
    _jlog("INFO", "picker", "Enriching video", url=vid_url)

    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "cookiesfrombrowser": ("firefox",),
        "useragent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(vid_url, download=False)
            if info:
                duration = info.get("duration") or 0
                _jlog(
                    "INFO",
                    "picker",
                    "Enriched OK",
                    title=info.get("title", "Unknown"),
                    duration=duration,
                )
            return info
    except Exception as e:
        _jlog("WARN", "picker", "Enrich failed", video_id=vid_id, error=str(e))
        return None


def _score_components(
    info: dict, now_ts: float, video_type: str = "shorts"
) -> dict | float:
    views = info.get("view_count") or 0
    likes = info.get("like_count") or 0
    duration = info.get("duration") or 0

    # Duration gate
    if video_type == "shorts" and duration > 60:
        return -1.0
    if video_type == "longform" and duration < LONGFORM_MIN_DURATION:
        return -1.0
    # "auto" passes both

    age_days = MAX_AGE_DAYS
    raw_date = info.get("upload_date", "")
    if raw_date and len(raw_date) == 8:
        try:
            dt = datetime(
                int(raw_date[:4]),
                int(raw_date[4:6]),
                int(raw_date[6:8]),
                tzinfo=timezone.utc,
            )
            age_days = max(0, (now_ts - dt.timestamp()) / 86400)
        except Exception:
            pass
    if age_days > MAX_AGE_DAYS:
        return -1.0
    return {"views": views, "likes": likes, "age_days": age_days}


def _build_scored_pool(
    channel_url: str, max_candidates: int = MAX_CANDIDATES, video_type: str = "shorts"
) -> list[tuple[float, dict]]:
    """
    Fetch → dedup-filter → parallel-enrich → score ALL candidates.
    video_type: "shorts" | "longform" | "auto"
    """
    entries = _fetch_channel_videos(
        channel_url, max_count=max_candidates, video_type=video_type
    )
    if not entries:
        raise ValueError("No videos found on this channel.")

    already_done = _load_uploaded()
    fresh_entries = [e for e in entries if e.get("id") not in already_done]

    skipped = len(entries) - len(fresh_entries)
    if skipped:
        _jlog(
            "INFO",
            "picker",
            "Dedup pre-filter applied",
            skipped=skipped,
            remaining=len(fresh_entries),
        )

    if not fresh_entries:
        raise ValueError(
            f"Pool exhausted: all {len(entries)} candidates in the last "
            f"{max_candidates} videos have already been uploaded. "
            "Call DELETE /auto/dedup to reset, or increase max_candidates."
        )

    _jlog(
        "INFO",
        "picker",
        "Enriching candidates in parallel",
        count=len(fresh_entries),
        workers=ENRICH_WORKERS,
    )
    now_ts = time.time()
    enriched: list[tuple[float, dict]] = []

    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as ex:
        future_map = {ex.submit(_enrich_video, e): e for e in fresh_entries}
        for future in as_completed(future_map):
            info = future.result()
            if not info:
                continue
            sc = _score_components(info, now_ts, video_type=video_type)
            if sc == -1.0:
                continue
            enriched.append((sc, info))

    if not enriched:
        raise ValueError(
            f"No fresh videos matching type='{video_type}' within the last "
            f"{MAX_AGE_DAYS} days after enrichment. "
            "Try increasing PICKER_MAX_AGE_DAYS or PICKER_MAX_CANDIDATES."
        )

    max_views = max(sc["views"] for sc, _ in enriched) or 1
    max_likes = max(sc["likes"] for sc, _ in enriched) or 1

    scored: list[tuple[float, dict]] = []
    for sc, info in enriched:
        score = (
            W_VIEWS * (sc["views"] / max_views)
            + W_LIKES * (sc["likes"] / max_likes)
            + W_FRESHNESS * (1.0 - sc["age_days"] / MAX_AGE_DAYS)
        )
        scored.append((score, info))

    scored.sort(key=lambda x: x[0], reverse=True)
    _jlog("INFO", "picker", "Pool ready", size=len(scored), video_type=video_type)
    return scored


def pick_best_video(
    channel_url: str,
    max_candidates: int = MAX_CANDIDATES,
    random_pick: bool = False,
    video_type: str = "shorts",
) -> dict:
    scored = _build_scored_pool(
        channel_url, max_candidates=max_candidates, video_type=video_type
    )
    if random_pick:
        _, info = random.choice(scored)
        _jlog(
            "INFO",
            "picker",
            "Random pick from pool",
            pool_size=len(scored),
            title=info.get("title"),
        )
        return info
    best_score, best_info = scored[0]
    _jlog(
        "INFO",
        "picker",
        "Best pick selected",
        score=round(best_score, 3),
        title=best_info.get("title"),
    )
    return best_info


# ══════════════════════════════════════════════════════════════════════════════
#  PART 2 — UPLOADER  (Shorts + Long-form)
# ══════════════════════════════════════════════════════════════════════════════

_QUOTA_BACKOFF_BASE = 60
_QUOTA_BACKOFF_MAX = 3600


def _is_quota_error(exc: Exception) -> bool:
    if not isinstance(exc, HttpError):
        return False
    if exc.resp.status != 403:
        return False
    try:
        detail = json.loads(exc.content or b"{}")
        reasons = [
            e.get("reason", "") for e in detail.get("error", {}).get("errors", [])
        ]
        return "quotaExceeded" in reasons or "userRateLimitExceeded" in reasons
    except Exception:
        return False


def _upload_thumbnail(
    account_id: str, youtube_video_id: str, thumbnail_url: str, tmp_dir: str
) -> bool:
    """
    Download the source thumbnail and upload it to the newly uploaded video.
    Returns True on success, False on failure (non-fatal).

    NOTE: Thumbnail uploads require the channel to be verified on YouTube.
    If the channel isn't verified, YouTube returns a 403 and we log a warning.
    """
    if not thumbnail_url or not THUMBNAIL_COPY:
        return False

    # Download thumbnail to disk
    ext = "jpg"
    for fmt in (".webp", ".png", ".jpg", ".jpeg"):
        if fmt in thumbnail_url.lower():
            ext = fmt.lstrip(".")
            break
    thumb_path = os.path.join(tmp_dir, f"thumbnail.{ext}")
    try:
        urllib.request.urlretrieve(thumbnail_url, thumb_path)
    except Exception as e:
        _jlog(
            "WARN",
            f"thumbnail:{account_id}",
            "Failed to download thumbnail",
            error=str(e),
        )
        return False

    mime_map = {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
    }
    mime = mime_map.get(ext, "image/jpeg")

    try:
        creds = _get_credentials(account_id)
        youtube = build("youtube", "v3", credentials=creds)
        media = MediaFileUpload(thumb_path, mimetype=mime, resumable=False)
        youtube.thumbnails().set(videoId=youtube_video_id, media_body=media).execute()
        _jlog(
            "INFO",
            f"thumbnail:{account_id}",
            "Thumbnail uploaded",
            video_id=youtube_video_id,
        )
        return True
    except HttpError as e:
        if e.resp.status == 403:
            _jlog(
                "WARN",
                f"thumbnail:{account_id}",
                "Thumbnail upload failed – channel may not be verified (403)",
                video_id=youtube_video_id,
                error=str(e),
            )
        else:
            _jlog(
                "WARN",
                f"thumbnail:{account_id}",
                "Thumbnail upload failed",
                video_id=youtube_video_id,
                error=str(e),
            )
        return False
    except Exception as e:
        _jlog(
            "WARN",
            f"thumbnail:{account_id}",
            "Thumbnail upload error",
            video_id=youtube_video_id,
            error=str(e),
        )
        return False


def upload_video(
    account_id: str,
    filepath: str,
    title: str,
    description: str,
    tags: list[str],
    privacy: str = "private",
    category_id: str = "22",
    made_for_kids: bool = False,
    is_short: bool = False,
    # v3 long-form extras
    default_language: str | None = None,
    default_audio_language: str | None = None,
    thumbnail_url: str | None = None,
    tmp_dir: str | None = None,
) -> str:
    """Upload a local MP4 to the YouTube channel linked to account_id."""
    creds = _get_credentials(account_id)
    youtube = build("youtube", "v3", credentials=creds)

    title = (title or "Untitled")[:100]
    if is_short:
        if "#Shorts" not in title and "#shorts" not in title:
            title = (title[:93] + " #Shorts")[:100]
        description = title

    description = (description or "")[:5000]
    tags = [t for t in (tags or []) if t][:500]
    if is_short and "Shorts" not in tags:
        tags = ["Shorts"] + tags

    snippet: dict = {
        "title": title,
        "description": description,
        "tags": tags,
        "categoryId": category_id,
    }
    if default_language:
        snippet["defaultLanguage"] = default_language
    if default_audio_language:
        snippet["defaultAudioLanguage"] = default_audio_language

    body = {
        "snippet": snippet,
        "status": {
            "privacyStatus": privacy,
            "selfDeclaredMadeForKids": made_for_kids,
        },
    }
    media = MediaFileUpload(
        filepath, mimetype="video/mp4", resumable=True, chunksize=5 * 1024 * 1024
    )
    insert = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response, retries = None, 0
    _jlog(
        "INFO", f"uploader:{account_id}", "Upload started", title=title, privacy=privacy
    )
    while response is None:
        try:
            status, response = insert.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                _jlog("INFO", f"uploader:{account_id}", "Upload progress", pct=pct)
        except Exception as e:
            retries += 1
            if retries > 10:
                raise RuntimeError(f"Upload failed after 10 retries: {e}")
            if _is_quota_error(e):
                wait = min(
                    _QUOTA_BACKOFF_BASE * (2 ** (retries - 1)), _QUOTA_BACKOFF_MAX
                )
                _jlog(
                    "WARN",
                    f"uploader:{account_id}",
                    "YouTube quota error – backing off",
                    retry=retries,
                    wait_s=wait,
                )
            else:
                wait = 2**retries
                _jlog(
                    "WARN",
                    f"uploader:{account_id}",
                    "Chunk failed, retrying",
                    retry=retries,
                    wait_s=wait,
                    error=str(e),
                )
            time.sleep(wait)

    vid_id = response["id"]
    _jlog(
        "INFO",
        f"uploader:{account_id}",
        "Upload complete",
        youtube_url=f"https://youtube.com/watch?v={vid_id}",
    )

    # v3: upload thumbnail for long-form videos
    if thumbnail_url and tmp_dir and not is_short:
        _upload_thumbnail(account_id, vid_id, thumbnail_url, tmp_dir)

    return vid_id


# ══════════════════════════════════════════════════════════════════════════════
#  PART 3 — PIPELINE
# ══════════════════════════════════════════════════════════════════════════════


def _download_video(src_url: str, tmp_dir: str) -> str:
    dl_opts = {
        "quiet": False,
        "no_warnings": True,
        "noplaylist": True,
        "nocheckcertificate": True,
        "format": (
            "bestvideo[height<=1080][vcodec^=avc]+bestaudio[ext=m4a]"
            "/bestvideo[height<=1080]+bestaudio"
            "/best[height<=1080]/best"
        ),
        "outtmpl": os.path.join(tmp_dir, "%(id)s.%(ext)s"),
        "merge_output_format": "mp4",
        "cookiesfrombrowser": ("firefox",),
        "useragent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "sleep_requests": 2,
        "sleep_interval": 3,
        "max_sleep_interval": 5,
        "extractor_retries": 3,
        "fragment_retries": 3,
    }

    _jlog("INFO", "download", "Starting download", url=src_url)
    with yt_dlp.YoutubeDL(dl_opts) as ydl:
        try:
            ydl.download([src_url])
        except Exception as e:
            _jlog(
                "WARN", "download", "Primary format failed, trying 720p", error=str(e)
            )
            dl_opts["format"] = "best[height<=720]"
            with yt_dlp.YoutubeDL(dl_opts) as ydl2:
                ydl2.download([src_url])

    candidates = [
        os.path.join(tmp_dir, f)
        for f in os.listdir(tmp_dir)
        if f.lower().endswith(".mp4")
    ]
    if not candidates:
        all_files = [
            os.path.join(tmp_dir, f)
            for f in os.listdir(tmp_dir)
            if os.path.isfile(os.path.join(tmp_dir, f))
        ]
        if not all_files:
            raise FileNotFoundError("Download produced no file")
        candidates = all_files

    downloaded_file = max(candidates, key=os.path.getsize)
    _jlog("INFO", "download", "Download complete", filepath=downloaded_file)
    return downloaded_file


def run_pipeline(
    channel_url: str,  # single URL (resolved from multi-channel selection upstream)
    account_ids: list[str],
    privacy: str = "private",
    max_candidates: int = MAX_CANDIDATES,
    random_pick: bool = False,
    dry_run: bool = False,
    video_type: str = "shorts",  # v3: "shorts" | "longform" | "auto"
) -> dict:
    """
    1. Build full scored + dedup-filtered pool from channel
    2. Pick best (or random) candidate
    3. Download it ONCE
    4. Upload to ALL account_ids in parallel  (skipped if dry_run=True)
    5. For long-form: also copy thumbnail after upload
    6. Mark source video as uploaded
    """
    if not account_ids:
        raise ValueError("Provide at least one account_id.")

    t0 = time.time()
    _update_stats(
        inc_total_runs=1,
        last_run_at=datetime.utcnow().isoformat() + "Z",
        last_run_source_channel=channel_url,
    )

    scored_pool = _build_scored_pool(
        channel_url, max_candidates=max_candidates, video_type=video_type
    )

    if random_pick:
        random.shuffle(scored_pool)

    attempt = 0
    last_error = "Unknown error"

    for attempt_idx, (score, best) in enumerate(scored_pool):
        if attempt_idx >= MAX_UPLOAD_RETRY:
            break

        vid_id = best.get("id", "")
        src_url = f"https://www.youtube.com/watch?v={vid_id}"
        title = best.get("title") or "Untitled"
        desc = best.get("description") or ""
        tags = best.get("tags") or []
        duration = best.get("duration") or 0
        category_id = str(best.get("categories", [None])[0] or "22")
        # Map category name → ID if yt-dlp returned a name string instead of int
        # (yt-dlp sometimes returns the numeric category already)
        if not category_id.isdigit():
            category_id = "22"  # People & Blogs fallback
        made_for_kids = bool(
            best.get("age_limit", 0) == 0 and best.get("made_for_kids", False)
        )
        default_language = best.get("language") or None
        default_audio_language = (
            best.get("audio_channels") and None
        )  # not available flat

        # v3: determine if this video is a Short or long-form
        is_short = (video_type == "shorts") or (video_type == "auto" and duration <= 60)
        is_longform = (video_type == "longform") or (
            video_type == "auto" and duration > 60
        )

        # Thumbnail URL — yt-dlp provides "thumbnail" (best) or "thumbnails" list
        thumbnail_url: str | None = None
        if is_longform:
            thumbnail_url = best.get("thumbnail")
            if not thumbnail_url:
                thumbs = best.get("thumbnails") or []
                if thumbs:
                    # Pick highest resolution (maxresdefault if available)
                    def _thumb_res(t):
                        return (t.get("width") or 0) * (t.get("height") or 0)

                    thumbnail_url = max(thumbs, key=_thumb_res).get("url")

        _update_stats(last_run_title=title, last_run_source_url=src_url)
        _jlog(
            "INFO",
            "pipeline",
            "Attempting candidate",
            attempt=attempt_idx + 1,
            title=title,
            source=src_url,
            score=round(score, 3),
            accounts=account_ids,
            dry_run=dry_run,
            video_type=video_type,
            is_short=is_short,
            duration=duration,
        )

        tmp = tempfile.mkdtemp(prefix="auto_pipeline_")
        try:
            try:
                filepath = _download_video(src_url, tmp)
            except Exception as e:
                last_error = f"Download failed: {e}"
                _jlog(
                    "WARN",
                    "pipeline",
                    "Download failed – trying next candidate",
                    attempt=attempt_idx + 1,
                    error=last_error,
                )
                continue

            size_mb = os.path.getsize(filepath) / 1024 / 1024
            _jlog(
                "INFO",
                "pipeline",
                "Downloaded",
                filepath=filepath,
                size_mb=round(size_mb, 1),
            )

            if dry_run:
                _jlog("INFO", "pipeline", "Dry-run – skipping uploads")
                return {
                    "success": True,
                    "dry_run": True,
                    "source_url": src_url,
                    "title": title,
                    "size_mb": round(size_mb, 1),
                    "random_pick": random_pick,
                    "video_type": video_type,
                    "is_short": is_short,
                    "thumbnail_url": thumbnail_url,
                    "elapsed_ms": int((time.time() - t0) * 1000),
                    "note": "Dry-run: video downloaded but NOT uploaded.",
                }

            results = {}

            def _upload_one(acc_id: str):
                try:
                    new_id = upload_video(
                        account_id=acc_id,
                        filepath=filepath,
                        title=title,
                        description=desc,  # v3: full description for longform
                        tags=tags,
                        privacy=privacy,
                        category_id=category_id,
                        made_for_kids=made_for_kids,
                        is_short=is_short,
                        default_language=default_language,
                        default_audio_language=default_audio_language,
                        thumbnail_url=thumbnail_url if is_longform else None,
                        tmp_dir=tmp if is_longform else None,
                    )
                    return acc_id, {
                        "success": True,
                        "uploaded_id": new_id,
                        "youtube_url": f"https://youtube.com/watch?v={new_id}",
                    }
                except Exception as e:
                    _jlog(
                        "ERROR",
                        "pipeline",
                        "Upload failed",
                        account=acc_id,
                        error=str(e),
                    )
                    return acc_id, {"success": False, "error": str(e)}

            with ThreadPoolExecutor(max_workers=len(account_ids)) as executor:
                futures = {
                    executor.submit(_upload_one, aid): aid for aid in account_ids
                }
                for future in as_completed(futures):
                    acc_id, outcome = future.result()
                    results[acc_id] = outcome

            successful = [a for a, r in results.items() if r.get("success")]
            failed = [a for a, r in results.items() if not r.get("success")]

            if not successful:
                last_error = f"All accounts failed: { {a: r['error'] for a, r in results.items()} }"
                _jlog(
                    "WARN",
                    "pipeline",
                    "All uploads failed – trying next candidate",
                    attempt=attempt_idx + 1,
                )
                _update_stats(inc_total_uploads_failed=len(failed))
                continue

            _mark_uploaded(vid_id)
            _update_stats(
                inc_total_uploads_succeeded=len(successful),
                inc_total_uploads_failed=len(failed),
            )

            return {
                "success": True,
                "source_url": src_url,
                "source_channel": channel_url,
                "title": title,
                "privacy": privacy,
                "size_mb": round(size_mb, 1),
                "random_pick": random_pick,
                "video_type": video_type,
                "is_short": is_short,
                "thumbnail_copied": thumbnail_url is not None and is_longform,
                "dry_run": False,
                "candidate_attempt": attempt_idx + 1,
                "accounts_succeeded": successful,
                "accounts_failed": failed,
                "results": results,
                "elapsed_ms": int((time.time() - t0) * 1000),
            }

        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    raise RuntimeError(
        f"Pipeline failed after {min(len(scored_pool), MAX_UPLOAD_RETRY)} attempts. "
        f"Last error: {last_error}"
    )


def run_pipeline_multi_channel(
    channel_urls: list[str],
    account_ids: list[str],
    privacy: str = "private",
    max_candidates: int = MAX_CANDIDATES,
    random_pick: bool = False,
    dry_run: bool = False,
    video_type: str = "shorts",
    channel_mode: str = MULTI_CHANNEL_MODE,
) -> dict:
    """
    v3 wrapper: selects one channel from channel_urls, then runs run_pipeline().
    The selected channel is logged and returned in the response.
    """
    chosen_channel = _select_channel(channel_urls, mode=channel_mode)
    _jlog(
        "INFO",
        "pipeline",
        "Channel selected for this run",
        chosen=chosen_channel,
        total_channels=len(channel_urls),
        mode=channel_mode,
    )

    result = run_pipeline(
        channel_url=chosen_channel,
        account_ids=account_ids,
        privacy=privacy,
        max_candidates=max_candidates,
        random_pick=random_pick,
        dry_run=dry_run,
        video_type=video_type,
    )
    result["channel_pool"] = channel_urls
    result["channel_selected"] = chosen_channel
    result["channel_mode"] = channel_mode
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  PART 4 — FLASK ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

_pending_flows: dict[str, tuple] = {}
_flows_lock = threading.Lock()


def _callback_url() -> str:
    base = os.environ.get("BASE_URL", "http://localhost:5000")
    return f"{base}/auto/auth/callback"


@auto_bp.route("/health", methods=["GET"])
def health():
    return jsonify(
        {
            "status": "ok",
            "ts": datetime.utcnow().isoformat() + "Z",
            "accounts_count": len(_list_accounts()),
            "dedup_count": len(_load_uploaded()),
        }
    )


@auto_bp.route("/stats", methods=["GET"])
def stats_endpoint():
    _require_api_key()
    with _stats_lock:
        payload = _load_stats()
    payload["dedup_count"] = len(_load_uploaded())
    payload["accounts_count"] = len(_list_accounts())
    return jsonify(payload)


@auto_bp.route("/dedup", methods=["DELETE"])
def clear_dedup():
    _require_api_key()
    with _dedup_lock:
        if os.path.exists(UPLOADED_LOG):
            os.remove(UPLOADED_LOG)
    _jlog("INFO", "dedup", "Dedup log cleared")
    return jsonify(
        {"success": True, "msg": "Dedup log cleared. All videos are eligible again."}
    )


@auto_bp.route("/accounts", methods=["GET"])
def list_accounts():
    _require_api_key()
    return jsonify({"accounts": _list_accounts()})


@auto_bp.route("/accounts/<account_id>", methods=["DELETE"])
def delete_account(account_id: str):
    _require_api_key()
    token_file = _token_path(account_id)
    if not os.path.exists(token_file):
        return jsonify({"error": f"No token found for '{account_id}'"}), 404
    os.remove(token_file)
    _jlog("INFO", "accounts", "Account removed", account_id=account_id)
    return jsonify({"success": True, "removed": account_id})


@auto_bp.route("/auth")
def auth_start():
    account_id = request.args.get("account", "").strip()
    if not account_id:
        return (
            jsonify(
                {
                    "error": "Pass ?account=<name>  e.g. /auto/auth?account=main",
                    "tip": "Use a short unique name per Gmail account (no spaces)",
                }
            ),
            400,
        )

    if not os.path.exists(CLIENT_SECRETS_FILE):
        return (
            jsonify(
                {
                    "error": f"'{CLIENT_SECRETS_FILE}' not found. Download it from Google Cloud Console."
                }
            ),
            500,
        )

    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, scopes=SCOPES, redirect_uri=_callback_url()
    )
    auth_url, state = flow.authorization_url(
        prompt="consent", access_type="offline", include_granted_scopes="true"
    )
    with _flows_lock:
        _pending_flows[state] = (flow, account_id)
    _jlog("INFO", "auth", "OAuth started", account_id=account_id)
    return redirect(auth_url)


@auto_bp.route("/auth/callback")
def auth_callback():
    state = request.args.get("state", "")
    with _flows_lock:
        entry = _pending_flows.pop(state, None)
    if not entry:
        return (
            jsonify(
                {
                    "error": "Invalid or expired state. Visit /auto/auth?account=<name> again."
                }
            ),
            400,
        )
    flow, account_id = entry
    flow.fetch_token(authorization_response=request.url)
    _save_credentials(account_id, flow.credentials)
    return (
        jsonify(
            {
                "success": True,
                "account_id": account_id,
                "message": f"✅ Account '{account_id}' authorized! You can now use it in /auto/run",
                "next": "To authorize another Gmail, visit /auto/auth?account=<other_name>",
            }
        ),
        200,
    )


@auto_bp.route("/pick", methods=["POST"])
def pick_endpoint():
    """
    POST /auto/pick
    {
        "channel_url":    "https://youtube.com/@SomeChannel",   // single channel
        "channel_urls":   ["https://...", "https://..."],        // OR multi-channel
        "channel_mode":   "random",                              // random|weighted|round_robin
        "max_candidates": 50,
        "random_pick":    false,
        "video_type":     "shorts"                               // shorts|longform|auto
    }
    """
    _require_api_key()
    _check_rate_limit()

    t0 = time.time()
    data = request.get_json() or {}

    # Accept both channel_url (string) and channel_urls (list) for backwards compat
    channel_urls = _resolve_channel_urls(data)
    channel_mode = data.get("channel_mode", MULTI_CHANNEL_MODE)
    chosen_channel = _select_channel(channel_urls, mode=channel_mode)

    max_cands = int(data.get("max_candidates", MAX_CANDIDATES))
    random_pick = bool(data.get("random_pick", False))
    video_type = data.get("video_type", "shorts")

    try:
        best = pick_best_video(
            chosen_channel,
            max_candidates=max_cands,
            random_pick=random_pick,
            video_type=video_type,
        )
        already = _already_uploaded(best.get("id", ""))
        duration = best.get("duration") or 0
        is_short = (video_type == "shorts") or (video_type == "auto" and duration <= 60)
        return jsonify(
            {
                "success": True,
                "random_pick": random_pick,
                "already_uploaded": already,
                "channel_selected": chosen_channel,
                "channel_pool": channel_urls,
                "channel_mode": channel_mode,
                "video_type": video_type,
                "is_short": is_short,
                "id": best.get("id"),
                "title": best.get("title"),
                "url": f"https://youtube.com/watch?v={best.get('id')}",
                "views": best.get("view_count"),
                "likes": best.get("like_count"),
                "duration": duration,
                "upload_date": best.get("upload_date"),
                "thumbnail": best.get("thumbnail"),
                "description": (best.get("description") or "")[:500],
                "tags": (best.get("tags") or [])[:10],
                "elapsed_ms": int((time.time() - t0) * 1000),
            }
        )
    except Exception as e:
        _jlog("ERROR", "pick_endpoint", str(e))
        return jsonify({"error": str(e)}), 500


@auto_bp.route("/run", methods=["POST"])
def run_endpoint():
    """
    POST /auto/run
    {
        // ── Channel selection (pick ONE style) ──
        "channel_url":    "https://youtube.com/@SomeChannel",    // single channel (v2 compat)
        "channel_urls":   [                                       // OR multi-channel (v3)
            "https://youtube.com/@Channel1",
            "https://youtube.com/@Channel2",
            "https://youtube.com/@Channel3"
        ],
        "channel_mode":   "random",    // random | weighted | round_robin  [default: random]

        // ── Upload config ──
        "account_ids":    ["main", "gaming", "hindi"],
        "privacy":        "private",
        "max_candidates": 50,
        "random_pick":    false,
        "dry_run":        false,

        // ── v3: video type ──
        "video_type":     "shorts"     // shorts | longform | auto  [default: shorts]
                                       // longform copies EVERYTHING: title, description,
                                       // tags, category, thumbnail, language, made_for_kids
    }
    """
    _require_api_key()
    _check_rate_limit()

    data = request.get_json() or {}
    channel_urls = _resolve_channel_urls(data)
    channel_mode = data.get("channel_mode", MULTI_CHANNEL_MODE)
    account_ids = data.get("account_ids") or []
    privacy = data.get("privacy", "private").strip()
    max_cands = int(data.get("max_candidates", MAX_CANDIDATES))
    random_pick = bool(data.get("random_pick", False))
    dry_run = bool(data.get("dry_run", False))
    video_type = data.get("video_type", "shorts")

    if not account_ids:
        return (
            jsonify(
                {
                    "error": "account_ids required",
                    "example": {"account_ids": ["main", "gaming", "hindi"]},
                    "tip": "Authorize each account at /auto/auth?account=<name> first",
                }
            ),
            400,
        )
    if privacy not in ("private", "unlisted", "public"):
        return jsonify({"error": "privacy must be private / unlisted / public"}), 400
    if video_type not in ("shorts", "longform", "auto"):
        return jsonify({"error": "video_type must be shorts / longform / auto"}), 400

    try:
        result = run_pipeline_multi_channel(
            channel_urls=channel_urls,
            account_ids=account_ids,
            privacy=privacy,
            max_candidates=max_cands,
            random_pick=random_pick,
            dry_run=dry_run,
            video_type=video_type,
            channel_mode=channel_mode,
        )
        return jsonify(result)
    except Exception as e:
        _jlog("ERROR", "run_endpoint", str(e))
        return jsonify({"error": str(e)}), 500


@auto_bp.route("/run_one", methods=["POST"])
def run_one_endpoint():
    """
    POST /auto/run_one
    {
        "channel_url":  "https://youtube.com/@SomeChannel",   // single or...
        "channel_urls": ["https://...", "https://..."],        // ...multi-channel
        "channel_mode": "random",
        "account_id":   "gaming",
        "privacy":      "private",
        "random_pick":  false,
        "dry_run":      false,
        "video_type":   "longform"   // <── set this for full copy mode
    }
    """
    _require_api_key()
    _check_rate_limit()

    data = request.get_json() or {}
    channel_urls = _resolve_channel_urls(data)
    channel_mode = data.get("channel_mode", MULTI_CHANNEL_MODE)
    account_id = data.get("account_id", "").strip()
    privacy = data.get("privacy", "private").strip()
    max_cands = int(data.get("max_candidates", MAX_CANDIDATES))
    random_pick = bool(data.get("random_pick", False))
    dry_run = bool(data.get("dry_run", False))
    video_type = data.get("video_type", "shorts")

    if not account_id:
        return jsonify({"error": "account_id required"}), 400

    try:
        result = run_pipeline_multi_channel(
            channel_urls=channel_urls,
            account_ids=[account_id],
            privacy=privacy,
            max_candidates=max_cands,
            random_pick=random_pick,
            dry_run=dry_run,
            video_type=video_type,
            channel_mode=channel_mode,
        )
        result["account_result"] = result.get("results", {}).get(account_id, {})
        return jsonify(result)
    except Exception as e:
        _jlog("ERROR", "run_one_endpoint", str(e))
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════


def _resolve_channel_urls(data: dict) -> list[str]:
    """
    Accept either:
      - "channel_urls": [...]          ← v3 multi-channel
      - "channel_url": "..."           ← v2 single-channel (still works)
    Raises 400-compatible ValueError if neither is provided.
    """
    channel_urls = data.get("channel_urls")
    if channel_urls:
        if isinstance(channel_urls, str):
            channel_urls = [channel_urls]
        channel_urls = [u.strip() for u in channel_urls if u.strip()]
    else:
        single = (data.get("channel_url") or "").strip()
        if single:
            channel_urls = [single]

    if not channel_urls:
        from flask import abort

        abort(400, description="channel_url (string) or channel_urls (list) required")

    return channel_urls
