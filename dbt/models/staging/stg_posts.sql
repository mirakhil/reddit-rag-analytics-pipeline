select
    ID as id,
    TITLE as title,
    CLEAN_SELFTEXT as body,
    AUTHOR as author,
    SCORE as score,
    UPVOTE_RATIO as upvote_ratio,
    NUM_COMMENTS as num_comments,
    TO_TIMESTAMP(CAST(CREATED_UTC AS INTEGER), 0) as created_at,
    URL as url,
    PERMALINK as permalink,
    SUBREDDIT as subreddit,
    SUBREDDIT_SUBSCRIBERS as subreddit_subscribers,
    OVER_18 as over_18,
    LINK_FLAIR_TEXT as link_flair_text,
    INGESTED_AT as ingested_at
from {{ source('staging', 'STG_POSTS') }}
where CLEAN_SELFTEXT is not null
and CLEAN_SELFTEXT != ''