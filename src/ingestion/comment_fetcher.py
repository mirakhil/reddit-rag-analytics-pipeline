"""
src/ingestion/comment_fetcher.py

Handles fetching comments for a list of posts.
Key features:
- Saves progress after every 10 posts (checkpoint)
- Resumes from checkpoint if interrupted by rate limit or crash
- Retries on 429 rate limit with a 60s wait
- Skips posts that already have comments fetched

Usage:
    from src.ingestion.comment_fetcher import fetch_comments_with_resume

    comments = fetch_comments_with_resume(session, posts, subreddit)
"""

import json
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from requests import Session

from src.utils.config import config
from src.utils.logger import get_logger

log = get_logger(__name__)

_CHECKPOINT_EVERY = 10   # Save progress after every N posts
_RATE_LIMIT_WAIT  = 60   # Seconds to wait after a 429


# ── Main entry point ──────────────────────────────────────────────────────────

def fetch_comments_with_resume(
    session: Session,
    posts: list,
    subreddit: str
) -> list:
    """
    Fetches comments for all posts, with checkpoint/resume support.

    If this run gets interrupted (rate limit, crash, etc), progress
    is saved to a checkpoint file. Next time this runs, it picks up
    from where it left off instead of starting over.

    Args:
        session:    requests.Session with User-Agent set
        posts:      list of post dicts from ingestion
        subreddit:  subreddit name (used for checkpoint file path)

    Returns:
        list of comment dicts, each with parent_post_id attached
    """
    checkpoint_path = config.comment_progress_file(subreddit)

    # Load existing checkpoint if one exists
    completed_ids, all_comments = _load_checkpoint(checkpoint_path)

    # Figure out which posts still need comments
    remaining_posts = [
        p for p in posts
        if p["id"] not in completed_ids
    ]

    log.info(
        f"Comment fetch for r/{subreddit}: "
        f"{len(completed_ids)} already done, "
        f"{len(remaining_posts)} remaining"
    )

    if not remaining_posts:
        log.info("All comments already fetched — nothing to do")
        _clear_checkpoint(checkpoint_path)
        return all_comments

    # Fetch comments post by post
    for i, post in enumerate(remaining_posts):
        post_id = post["id"]
        post_title = post.get("title", "")[:50]

        log.info(f"  [{i+1}/{len(remaining_posts)}] Fetching comments for: {post_title}...")

        comments = _fetch_with_retry(session, post_id, subreddit)

        # Attach parent post id to each comment
        for c in comments:
            c["parent_post_id"] = post_id

        all_comments.extend(comments)
        completed_ids.add(post_id)

        # Save checkpoint every N posts
        if (i + 1) % _CHECKPOINT_EVERY == 0:
            _save_checkpoint(checkpoint_path, completed_ids, all_comments)
            log.info(f"Checkpoint saved ({len(completed_ids)} posts done)")

        # Polite delay between requests
        time.sleep(random.uniform(config.request_delay, config.request_delay + 0.5))

    # All done — clear checkpoint
    _clear_checkpoint(checkpoint_path)
    log.info(f"Fetched {len(all_comments)} total comments for r/{subreddit}")

    return all_comments


# ── Fetch with retry ──────────────────────────────────────────────────────────

def _fetch_with_retry(session: Session, post_id: str, subreddit: str) -> list:
    """
    Fetches comments for a single post.
    Retries up to config.retries times.
    On 429 rate limit: waits 60s then retries.
    On persistent failure: logs warning and returns empty list
    (we never want one bad post to kill the whole run).
    """
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
    params = {"limit": config.comment_limit}

    for attempt in range(config.retries):
        try:
            res = session.get(url, params=params, timeout=30)

            # Rate limited — wait 60s and retry
            if res.status_code == 429:
                log.warning(f"Rate limited on post {post_id}. Waiting {_RATE_LIMIT_WAIT}s...")
                time.sleep(_RATE_LIMIT_WAIT)
                continue

            res.raise_for_status()
            data = res.json()

            return _parse_comments(data)

        except Exception as e:
            wait = 2 ** attempt  # exponential backoff: 1s, 2s, 4s
            log.warning(f"Attempt {attempt+1} failed for post {post_id}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    # All retries exhausted — skip this post's comments
    log.warning(f"Skipping comments for post {post_id} after {config.retries} failed attempts")
    return []


# ── Parse comments from API response ─────────────────────────────────────────

def _parse_comments(data: list) -> list:
    """
    Reddit's comment API returns a 2-element list:
    [0] = post data
    [1] = comment data

    We only want [1], and only nodes of kind "t1" (actual comments).
    Skips deleted/removed comments with no useful text.
    """
    comments = []

    try:
        comment_nodes = data[1]["data"]["children"]

        for node in comment_nodes:
            if node["kind"] != "t1":
                continue

            body = node["data"].get("body", "").strip()

            # Skip deleted, removed, or empty comments
            if not body or body in ("[deleted]", "[removed]"):
                continue

            comments.append(node["data"])

    except Exception as e:
        log.warning(f"Error parsing comments: {e}")

    return comments


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _load_checkpoint(path: Path) -> tuple[set, list]:
    """
    Loads existing checkpoint file if it exists.
    Returns (set of completed post IDs, list of already-fetched comments).
    Returns (empty set, empty list) if no checkpoint exists.
    """
    if not path.exists():
        return set(), []

    try:
        with open(path, "r") as f:
            data = json.load(f)

        completed_ids = set(data.get("completed_post_ids", []))
        comments = data.get("comments", [])

        log.info(f"Resuming from checkpoint: {len(completed_ids)} posts already done")
        return completed_ids, comments

    except Exception as e:
        log.warning(f"Could not load checkpoint — starting fresh: {e}")
        return set(), []


def _save_checkpoint(path: Path, completed_ids: set, comments: list) -> None:
    """Saves current progress to checkpoint file."""
    try:
        with open(path, "w") as f:
            json.dump({
                "completed_post_ids": list(completed_ids),
                "comments": comments,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "status": "in_progress"
            }, f)
    except Exception as e:
        log.error(f"Failed to save checkpoint: {e}")


def _clear_checkpoint(path: Path) -> None:
    """Deletes checkpoint file after a successful complete run."""
    try:
        if path.exists():
            path.unlink()
            log.info("Checkpoint cleared")
    except Exception as e:
        log.warning(f"Could not clear checkpoint: {e}")