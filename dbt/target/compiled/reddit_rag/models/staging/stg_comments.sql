select
    ID as id,
    CLEAN_BODY as body,
    AUTHOR as author,
    SCORE as score,
    TO_TIMESTAMP(CAST(CREATED_UTC AS INTEGER), 0) as created_at,
    DEPTH as depth,
    PARENT_ID as parent_id,
    PARENT_POST_ID as parent_post_id,
    PERMALINK as permalink,
    SUBREDDIT as subreddit,
    INGESTED_AT as ingested_at
from REDDIT_RAG.STAGING.STG_COMMENTS
where CLEAN_BODY is not null
and CLEAN_BODY != ''