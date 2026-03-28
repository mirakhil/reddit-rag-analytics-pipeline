"""
src/utils/config.py
Central config loader — import `config` everywhere, never hardcode values.
"""
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parents[2]  

load_dotenv(dotenv_path=_PROJECT_ROOT / ".env")

def _str(key, default=""): return os.getenv(key, default).strip()
def _int(key, default=0):
    try: return int(os.getenv(key, str(default)))
    except ValueError: return default
def _float(key, default=0.0):
    try: return float(os.getenv(key, str(default)))
    except ValueError: return default
def _list(key, default=None):
    raw = os.getenv(key, "")
    return [i.strip() for i in raw.split(",") if i.strip()] if raw.strip() else (default or [])

@dataclass
class Config:
    # Environment
    env: str = field(default_factory=lambda: _str("ENV", "dev"))

    # Session
    base_url: str = field(default_factory=lambda: _str("BASE_URL", "https://www.reddit.com"))
    user_agent: str = field(default_factory=lambda: _str("USER_AGENT", "reddit-rag-pipeline/1.0 (personal project)"))

    # Reddit
    subreddits: List[str] = field(default_factory=lambda: _list("SUBREDDITS", ["dataengineering"]))
    max_posts: int = field(default_factory=lambda: _int("MAX_POSTS", 100))
    on_demand_max_posts: int = field(default_factory=lambda: _int("ON_DEMAND_MAX_POSTS", 25))
    comment_limit: int = field(default_factory=lambda: _int("COMMENT_LIMIT", 100))
    reddit_category: str = field(default_factory=lambda: _str("REDDIT_CATEGORY", "hot"))
    request_delay: int = field(default_factory=lambda: _int("REQUEST_DELAY", 1))
    retries: int = field(default_factory=lambda: _int("RETRIES", 3))
    max_workers: int = field(default_factory=lambda: _int("MAX_WORKERS", 2))
    data_ttl_hours: int = field(default_factory=lambda: _int("DATA_TTL_HOURS", 6))
    index_rolling_window_days: int = field(default_factory=lambda: _int("INDEX_ROLLING_WINDOW_DAYS", 30))

    # API Keys
    openai_api_key: str = field(default_factory=lambda: _str("OPENAI_API_KEY"))
    openai_embedding_model: str = field(default_factory=lambda: _str("OPENAI_EMBEDDING_MODEL", "text-embedding-ada-002"))
    groq_api_key: str = field(default_factory=lambda: _str("GROQ_API_KEY"))
    google_api_key: str = field(default_factory=lambda: _str("GOOGLE_API_KEY"))
    anthropic_api_key: str = field(default_factory=lambda: _str("ANTHROPIC_API_KEY"))

    # Snowflake
    snowflake_account: str = field(default_factory=lambda: _str("SNOWFLAKE_ACCOUNT"))
    snowflake_user: str = field(default_factory=lambda: _str("SNOWFLAKE_USER"))
    snowflake_password: str = field(default_factory=lambda: _str("SNOWFLAKE_PASSWORD"))
    snowflake_database: str = field(default_factory=lambda: _str("SNOWFLAKE_DATABASE"))
    snowflake_schema: str = field(default_factory=lambda: _str("SNOWFLAKE_SCHEMA"))
    snowflake_warehouse: str = field(default_factory=lambda: _str("SNOWFLAKE_WAREHOUSE"))

    # RAG
    rag_top_k: int = field(default_factory=lambda: _int("RAG_TOP_K", 5))
    chunk_size: int = field(default_factory=lambda: _int("CHUNK_SIZE", 500))
    chunk_overlap: int = field(default_factory=lambda: _int("CHUNK_OVERLAP", 50))
    memory_buffer_size: int = field(default_factory=lambda: _int("MEMORY_BUFFER_SIZE", 5))
    rag_min_relevance_score: float = field(default_factory=lambda: _float("RAG_MIN_RELEVANCE_SCORE", 0.75))

    # Paths
    raw_data_path: Path = field(default_factory=lambda: _PROJECT_ROOT / _str("RAW_DATA_PATH", "data/raw"))
    processed_data_path: Path = field(default_factory=lambda: _PROJECT_ROOT / _str("PROCESSED_DATA_PATH", "data/processed"))
    faiss_index_path: Path = field(default_factory=lambda: _PROJECT_ROOT / _str("FAISS_INDEX_PATH", "data/faiss_index"))
    state_path: Path = field(default_factory=lambda: _PROJECT_ROOT / _str("STATE_PATH", "data/raw/state"))

    # ── Helpers ──────────────────────────────────────────────
    @property
    def is_dev(self): return self.env.lower() == "dev"

    @property
    def is_prod(self): return self.env.lower() == "prod"

    def posts_path(self, subreddit: str) -> Path:
        return self.raw_data_path / "posts" / subreddit

    def comments_path(self, subreddit: str) -> Path:
        return self.raw_data_path / "comments" / subreddit

    def faiss_path_for(self, subreddit: str) -> Path:
        return self.faiss_index_path / subreddit

    def processed_path(self, subreddit: str) -> Path:
        return self.processed_data_path / subreddit

    def lock_file(self, subreddit: str) -> Path:
        """Prevents two pipeline runs for the same subreddit at the same time."""
        return self.state_path / f"{subreddit}.lock"

    def comment_progress_file(self, subreddit: str) -> Path:
        """Checkpoint file — saves progress so comment fetching can resume after rate limit."""
        return self.state_path / f"{subreddit}_comment_progress.json"

    def watermark_file(self, subreddit: str) -> Path:
        """Tracks last ingestion timestamp per subreddit (dev mode)."""
        return self.state_path / f"{subreddit}_watermark.json"

    def ensure_dirs(self, subreddit: str) -> None:
        """Creates all folders for a subreddit. Call before any ingestion run."""
        for path in [
            self.posts_path(subreddit),
            self.comments_path(subreddit),
            self.faiss_path_for(subreddit),
            self.processed_path(subreddit),
            self.state_path,
        ]:
            path.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        ##Call at startup — raises EnvironmentError if required keys are missing.
        
        errors = []
        if not self.openai_api_key or self.openai_api_key == "your_openai_api_key_here":
            errors.append("OPENAI_API_KEY is not set in .env")
        if self.is_prod:
            for name, val in [
                ("SNOWFLAKE_ACCOUNT", self.snowflake_account),
                ("SNOWFLAKE_USER", self.snowflake_user),
                ("SNOWFLAKE_PASSWORD", self.snowflake_password),
                ("SNOWFLAKE_DATABASE", self.snowflake_database),
                ("SNOWFLAKE_SCHEMA", self.snowflake_schema),
                ("SNOWFLAKE_WAREHOUSE", self.snowflake_warehouse),
            ]:
                if not val:
                    errors.append(f"{name} required in prod but not set")
        if errors:
            raise EnvironmentError("Fix these in .env:\n" + "\n".join(f"  ✗ {e}" for e in errors))


config = Config()