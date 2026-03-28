"""
src/ingestion/watermark.py

Tracks the last successful ingestion timestamp per subreddit.
In dev mode: reads/writes a simple JSON file.
In prod mode: reads/writes a Snowflake table (to be implemented later).

Usage:
    from src.ingestion.watermark import get_watermark, update_watermark

    last_run = get_watermark("dataengineering")
    # Returns a datetime or None if first run

    update_watermark("dataengineering", datetime.utcnow())
"""

import json
from datetime import datetime, timezone
from src.utils.config import config
from src.utils.logger import get_logger

log = get_logger(__name__)

_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_watermark(subreddit: str) -> datetime | None:
    """
    Returns the last successful ingestion time for a subreddit.
    Returns None if this subreddit has never been ingested before.
    """
    if config.is_dev:
        return _read_local_watermark(subreddit)
    else:
        # Prod: Snowflake implementation goes here later
        raise NotImplementedError("Snowflake watermark not implemented yet")


def update_watermark(subreddit: str, timestamp: datetime) -> None:
    """
    Saves the latest ingestion timestamp for a subreddit.
    Called after a successful ingestion run.
    """
    if config.is_dev:
        _write_local_watermark(subreddit, timestamp)
    else:
        raise NotImplementedError("Snowflake watermark not implemented yet")


def is_data_stale(subreddit: str) -> bool:
    """
    Returns True if the subreddit data is older than DATA_TTL_HOURS
    or has never been ingested. Used by the UI to decide whether
    to trigger a refresh before answering queries.
    """
    last_run = get_watermark(subreddit)

    # Never ingested before — definitely stale
    if last_run is None:
        return True

    now = datetime.now(timezone.utc)

    # Make last_run timezone-aware if it isn't
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    hours_since = (now - last_run).total_seconds() / 3600

    stale = hours_since > config.data_ttl_hours
    if stale:
        log.info(f"r/{subreddit} data is stale ({hours_since:.1f}h old, TTL={config.data_ttl_hours}h)")

    return stale


def get_last_updated_str(subreddit: str) -> str:
    """
    Returns a human-readable string of when data was last fetched.
    Used in the Streamlit UI to show freshness.

    Examples:
        "Last updated: 2 hours ago"
        "Last updated: Never"
    """
    last_run = get_watermark(subreddit)

    if last_run is None:
        return "Never ingested"

    now = datetime.now(timezone.utc)
    if last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    diff = now - last_run
    total_seconds = int(diff.total_seconds())

    if total_seconds < 60:
        return "Last updated: just now"
    elif total_seconds < 3600:
        mins = total_seconds // 60
        return f"Last updated: {mins} minute{'s' if mins > 1 else ''} ago"
    elif total_seconds < 86400:
        hours = total_seconds // 3600
        return f"Last updated: {hours} hour{'s' if hours > 1 else ''} ago"
    else:
        days = total_seconds // 86400
        return f"Last updated: {days} day{'s' if days > 1 else ''} ago"


# ── Local (dev) helpers ───────────────────────────────────────────────────────

def _read_local_watermark(subreddit: str) -> datetime | None:
    path = config.watermark_file(subreddit)

    if not path.exists():
        log.info(f"No watermark found for r/{subreddit} — first run")
        return None

    try:
        with open(path, "r") as f:
            data = json.load(f)
        ts = datetime.strptime(data["last_ingested_utc"], _DATE_FORMAT)
        ts = ts.replace(tzinfo=timezone.utc)
        log.info(f"Watermark for r/{subreddit}: {ts}")
        return ts
    except Exception as e:
        log.warning(f"Could not read watermark for r/{subreddit}: {e}")
        return None


def _write_local_watermark(subreddit: str, timestamp: datetime) -> None:
    path = config.watermark_file(subreddit)

    try:
        with open(path, "w") as f:
            json.dump({
                "subreddit": subreddit,
                "last_ingested_utc": timestamp.strftime(_DATE_FORMAT),
                "updated_at": datetime.now(timezone.utc).strftime(_DATE_FORMAT)
            }, f, indent=2)
        log.info(f"Watermark updated for r/{subreddit} → {timestamp}")
    except Exception as e:
        log.error(f"Failed to write watermark for r/{subreddit}: {e}")