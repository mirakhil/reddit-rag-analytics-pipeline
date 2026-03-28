import pandas as pd
from src.utils.config import config
from src.utils.snowflake_client import get_snowflake_connection
from snowflake.connector.pandas_tools import write_pandas

from src.utils.logger import get_logger
log = get_logger(__name__)

def load_posts_to_snowflake(subreddit):
    log.info(f"Loading posts for r/{subreddit} to Snowflake")
    
    processed_posts_path = config.processed_path(subreddit) / "posts"
    df = pd.read_parquet(processed_posts_path)
    log.info(f"Read {len(df)} posts from parquet")

    with get_snowflake_connection() as conn:
        write_pandas(conn=conn, df=df, table_name="STG_POSTS",
                     database="REDDIT_RAG", schema="STAGING", quote_identifiers=False)
    
    log.info(f"Loaded {len(df)} posts to Snowflake")


def load_comments_to_snowflake(subreddit):
    log.info(f"Loading comments for r/{subreddit} to Snowflake")
    
    processed_comments_path = config.processed_path(subreddit) / "comments"
    df = pd.read_parquet(processed_comments_path)
    log.info(f"Read {len(df)} comments from parquet")

    with get_snowflake_connection() as conn:
        write_pandas(conn=conn, df=df, table_name="STG_COMMENTS",
                     database="REDDIT_RAG", schema="STAGING", quote_identifiers=False)
    
    log.info(f"Loaded {len(df)} comments to Snowflake")


if __name__ == "__main__":
    for subreddit in config.subreddits:
        load_posts_to_snowflake(subreddit)
        load_comments_to_snowflake(subreddit)    