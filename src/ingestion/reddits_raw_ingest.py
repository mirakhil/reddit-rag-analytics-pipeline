"""
src/ingestion/reddits_raw_ingest.py

Main ingestion script for Reddit data.
Pulls posts and comments for one or more subreddits incrementally.

Designed to be run:
  - By Airflow on a schedule (all subreddits in .env)
  - By pipeline_trigger.py on-demand (single subreddit, fewer posts)

Usage:
    # Run all subreddits from .env
    python -m src.ingestion.reddits_raw_ingest

    # Run a single subreddit (on-demand, fewer posts)
    python -m src.ingestion.reddits_raw_ingest --subreddit python --limit 25
"""

import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.utils.config import config
from src.utils.logger import get_logger
from src.ingestion.watermark import get_watermark, update_watermark, is_data_stale
from src.ingestion.comment_fetcher import fetch_comments_with_resume

log = get_logger(__name__)

BASE_URL   = config.base_url
USER_AGENT = config.user_agent


# ── Session ───────────────────────────────────────────────────────────────────

def _get_session() -> requests.Session:

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


# ── Subreddit validation ──────────────────────────────────────────────────────

def validate_subreddit(subreddit: str) -> bool:
    """
    Pings Reddit to check if a subreddit actually exists before
    we try to ingest it. Prevents confusing errors downstream.

    Returns True if valid, False if not found or private.
    """
    session = _get_session()
    url = f"{BASE_URL}/r/{subreddit}/about.json"

    try:
        res = session.get(url, timeout=10)

        if res.status_code == 404:
            log.warning(f"r/{subreddit} does not exist on Reddit")
            return False

        if res.status_code == 403:
            log.warning(f"r/{subreddit} is private or banned")
            return False

        data = res.json()

        # Reddit returns a "quarantined" flag for some subs
        if data.get("data", {}).get("quarantine"):
            log.warning(f"r/{subreddit} is quarantined")
            return False

        log.info(f"r/{subreddit} validated successfully")
        return True

    except Exception as e:
        log.error(f"Could not validate r/{subreddit}: {e}")
        return False


# ── Lock file (prevents concurrent runs) ─────────────────────────────────────

def _acquire_lock(subreddit: str) -> bool:
    """
    Creates a lock file for this subreddit.
    Returns True if lock acquired, False if another run is already active.
    """
    lock_path = config.lock_file(subreddit)

    if lock_path.exists():
        log.warning(
            f"r/{subreddit} pipeline is already running "
            f"(lock file exists: {lock_path}). Skipping."
        )
        return False

    lock_path.write_text(datetime.now(timezone.utc).isoformat())
    log.info(f"Lock acquired for r/{subreddit}")
    return True


def _release_lock(subreddit: str) -> None:
    """Removes the lock file after a run completes (success or failure)."""
    lock_path = config.lock_file(subreddit)
    try:
        if lock_path.exists():
            lock_path.unlink()
            log.info(f"Lock released for r/{subreddit}")
    except Exception as e:
        log.warning(f"Could not release lock for r/{subreddit}: {e}")


# ── Post fetching ─────────────────────────────────────────────────────────────

def _fetch_posts_page(
    session: requests.Session,
    subreddit: str,
    after: str = None
) -> dict:

    ##Fetches one page of posts (up to 100) from Reddit.

    url = f"{BASE_URL}/r/{subreddit}/{config.reddit_category}.json"
    params = {"limit": 100, "after": after}

    for attempt in range(config.retries):
        try:
            res = session.get(url, params=params, timeout=30)
            
            if res.status_code == 429:
                wait = 5 * (attempt + 1)
                log.warning(f"Rate limited fetching posts. Waiting {wait}s...")
                time.sleep(wait)
                continue

            res.raise_for_status()
            return res.json()

        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch posts for r/{subreddit} after {config.retries} attempts")


def _collect_posts(
    session: requests.Session,
    subreddit: str,
    max_posts: int,
    since: datetime = None
) -> list:


    ##Collects posts up to max_posts.
    ##If `since` is provided (watermark), only returns posts newer than that timestamp.
    ##Stops early if it hits posts older than the watermark — no need to paginate further.

    all_posts = []
    after     = None
    page      = 1

    while len(all_posts) < max_posts:
        log.info(f"Fetching page {page} for r/{subreddit}...")

        data     = _fetch_posts_page(session, subreddit, after)
        children = data["data"]["children"]
        after    = data["data"]["after"]

        if not children:
            break

        for post in children:
            p = post["data"]

            # Filter out posts with no text content
            if _is_low_quality(p):
                continue

            # Watermark filter — stop if we've hit old posts
            if since is not None:
                post_time = datetime.fromtimestamp(p["created_utc"], tz=timezone.utc)
                if post_time <= since:
                    log.info(f"Hit watermark at post {p['id']} — stopping pagination")
                    return all_posts

            all_posts.append(p)

            if len(all_posts) >= max_posts:
                break

        if not after:
            break

        page += 1
        time.sleep(config.request_delay)

    return all_posts


# ── Content quality filter ────────────────────────────────────────────────────

def _is_low_quality(post: dict) -> bool:
    """
    Returns True for posts we should skip before embedding:
    - Deleted or removed posts
    - Link-only posts (no selftext)
    - Posts with very short content
    """
    selftext = post.get("selftext", "").strip()

    if selftext in ("", "[deleted]", "[removed]"):
        return True

    if len(selftext) < 30:
        return True

    return False


# ── Save raw data ─────────────────────────────────────────────────────────────

def _save_raw(data: list, folder: Path, prefix: str, run_ts: str) -> Path:
    """Saves a list of records as a timestamped JSON file."""
    folder.mkdir(parents=True, exist_ok=True)
    file_path = folder / f"{prefix}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log.info(f"Saved {len(data)} {prefix} → {file_path}")
    return file_path


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_ingestion(subreddit: str, max_posts: int = None) -> bool:
    """
    Runs the full ingestion pipeline for a single subreddit.

    Steps:
        1. Validate subreddit exists on Reddit
        2. Acquire lock (prevent concurrent runs)
        3. Read watermark (last ingestion time)
        4. Fetch only new posts since watermark
        5. Fetch comments for new posts (with checkpoint/resume)
        6. Save raw JSON files
        7. Update watermark
        8. Release lock

    Args:
        subreddit: subreddit name without r/
        max_posts: override max posts (used by on-demand trigger for 25 posts)

    Returns:
        True if successful, False if failed or nothing new
    """
    run_ts   = datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")
    max_posts = max_posts or config.max_posts

    log.info(f"{'='*50}")
    log.info(f"Starting ingestion for r/{subreddit} | max_posts={max_posts}")
    log.info(f"{'='*50}")

    # Step 1 — Validate subreddit
    if not validate_subreddit(subreddit):
        return False

    # Step 2 — Ensure folders exist FIRST (moved up before lock)
    config.ensure_dirs(subreddit)

    # Step 3 — Acquire lock
    if not _acquire_lock(subreddit):
        return False

    try:
        # Step 3 — Ensure folders exist
        config.ensure_dirs(subreddit)

        # Step 4 — Read watermark
        last_ingested = get_watermark(subreddit)
        if last_ingested:
            log.info(f"Watermark: fetching posts newer than {last_ingested}")
        else:
            log.info("No watermark — first run, fetching all posts")

        # Step 5 — Fetch posts
        session   = _get_session()
        new_posts = _collect_posts(session, subreddit, max_posts, since=last_ingested)

        if not new_posts:
            log.info(f"No new posts for r/{subreddit} — skipping")
            return False

        log.info(f"Found {len(new_posts)} new posts for r/{subreddit}")

        # Step 6 — Fetch comments
        log.info("Fetching comments...")
        comments = fetch_comments_with_resume(session, new_posts, subreddit)

        # Step 7 — Save raw files
        _save_raw(new_posts, config.posts_path(subreddit),    "posts",    run_ts)
        _save_raw(comments,  config.comments_path(subreddit), "comments", run_ts)

        # Step 8 — Update watermark to most recent post time
        latest_ts = max(
            datetime.fromtimestamp(p["created_utc"], tz=timezone.utc)
            for p in new_posts
        )
        update_watermark(subreddit, latest_ts)

        log.info(f"Ingestion complete for r/{subreddit} ✓")
        return True

    except Exception as e:
        log.error(f"Ingestion failed for r/{subreddit}: {e}")
        return False

    finally:
        # Always release lock, even if something crashes
        _release_lock(subreddit)


def run_all() -> None:
    """
    Runs ingestion for all subreddits defined in .env SUBREDDITS list.
    Called by Airflow on schedule.
    """
    subreddits = config.subreddits

    if not subreddits:
        log.error("No subreddits configured in .env — nothing to ingest")
        return

    log.info(f"Running scheduled ingestion for {len(subreddits)} subreddit(s): {subreddits}")

    results = {}
    for subreddit in subreddits:
        success = run_ingestion(subreddit)
        results[subreddit] = "✓" if success else "✗"

    # Summary
    log.info("=" * 50)
    log.info("Ingestion Summary:")
    for sub, status in results.items():
        log.info(f"  {status} r/{sub}")
    log.info("=" * 50)


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reddit RAG ingestion pipeline")
    parser.add_argument("--subreddit", type=str, help="Single subreddit to ingest")
    parser.add_argument("--limit",     type=int, help="Max posts to fetch")
    args = parser.parse_args()

    if args.subreddit:
        # Single subreddit run (on-demand or manual test)
        run_ingestion(args.subreddit, max_posts=args.limit)
    else:
        # Full scheduled run — all subreddits in .env
        run_all()