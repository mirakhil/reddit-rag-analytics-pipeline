import sys
import os
import re
import html

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:/hadoop/bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from src.utils.config import config
from src.utils.logger import get_logger

log = get_logger(__name__)


def _get_spark():
    return SparkSession.builder \
        .appName("reddit rag pipeline") \
        .master("local[*]") \
        .getOrCreate()


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    text = re.sub(r"~~(.*?)~~", r"\1", text)
    text = re.sub(r"`(.*?)`", r"\1", text)
    text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
    text = re.sub(r'http\S+', '', text)
    text = text.replace("\n", " ")
    text = re.sub(r' +', ' ', text)
    text = text.strip()
    return text


clean_text_udf = udf(clean_text, StringType())


def transform_posts(subreddit: str) -> None:
    log.info(f"Transforming posts for r/{subreddit}")

    spark = _get_spark()

    # keep as Path object for the exists() check
    file_path = config.posts_path(subreddit) / "posts.json"

    if not file_path.exists():
        log.error(f"No raw data found for r/{subreddit} — run ingestion first")
        return

    # convert to string only when passing to spark
    file_path_str = str(file_path).replace("\\", "/")

    log.info(f"Reading posts from {file_path_str}")

    try:
        df = spark.read.option("multiline", "true").json(file_path_str)

        df_clean = df.select(
            "id", "title", "selftext", "author", "score",
            "upvote_ratio", "num_comments", "created_utc", "url",
            "permalink", "subreddit", "subreddit_subscribers",
            "over_18", "link_flair_text"
        ).dropDuplicates(["id"])

        df_clean = df_clean.withColumn("clean_selftext", clean_text_udf("selftext"))

        df_final = df_clean.select(
            "id", "title", "clean_selftext", "author", "score",
            "upvote_ratio", "num_comments", "created_utc", "url",
            "permalink", "subreddit", "subreddit_subscribers",
            "over_18", "link_flair_text"
        ).dropDuplicates(["id"])

        output_path = str(config.processed_path(subreddit) / "posts").replace("\\", "/")
        df_final.write.mode("overwrite").parquet(output_path)
        log.info(f"Posts saved to {output_path}")
    
    except Exception as e:
        log.error(f"Failed to transform posts for r/{subreddit}: {e}")
        raise


def transform_comments(subreddit: str) -> None:
    log.info(f"Transforming comments for r/{subreddit}")

    spark = _get_spark()

    # keep as Path object for the exists() check
    file_path = config.comments_path(subreddit) / "comments.json"

    if not file_path.exists():
        log.error(f"No raw data found for r/{subreddit} — run ingestion first")
        return

    # convert to string only when passing to spark
    file_path_str = str(file_path).replace("\\", "/")

    log.info(f"Reading comments from {file_path_str}")


    try:
        df = spark.read.option("multiline", "true").json(file_path_str)

        df_clean = df.select(
            "id", "body", "author", "score", "created_utc",
            "depth", "parent_id", "parent_post_id", "permalink", "subreddit"
        ).dropDuplicates(["id"])

        df_clean = df_clean.filter(col("depth") == 0) \
                        .withColumn("clean_body", clean_text_udf("body"))

        df_final = df_clean.select(
            "id", "clean_body", "author", "score", "created_utc",
            "depth", "parent_id", "parent_post_id", "permalink", "subreddit"
        ).dropDuplicates(["id"])

        output_path = str(config.processed_path(subreddit) / "comments").replace("\\", "/")
        df_final.write.mode("overwrite").parquet(output_path)
        log.info(f"Comments saved to {output_path}")

    except Exception as e:
        log.error(f"Failed to transform comments for r/{subreddit}: {e}")
        raise

if __name__ == "__main__":
    for subreddit in config.subreddits:
        transform_posts(subreddit)
        transform_comments(subreddit)