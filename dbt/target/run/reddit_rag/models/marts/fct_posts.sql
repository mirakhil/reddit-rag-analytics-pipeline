
  
    

create or replace transient table REDDIT_RAG.MARTS.fct_posts
    
    
    
    as (with posts as (
    select * from REDDIT_RAG.MARTS.stg_posts
),

comments as (
    select * from REDDIT_RAG.MARTS.stg_comments
),

final as (
    select
        -- post columns
        p.id as post_id,
        p.title as post_title,
        p.body as post_body,
        p.author as post_author,
        p.score as post_score,
        p.upvote_ratio,
        p.num_comments,
        p.created_at as post_created_at,
        p.url as post_url,
        p.permalink as post_permalink,
        p.subreddit,
        p.subreddit_subscribers,
        p.over_18,
        p.link_flair_text,

        -- comment columns
        c.id as comment_id,
        c.body as comment_body,
        c.author as comment_author,
        c.score as comment_score,
        c.created_at as comment_created_at,
        c.permalink as comment_permalink,
        c.parent_id as comment_parent_id

    from posts p
    left join comments c on p.id = c.parent_post_id
)

select * from final
    )
;


  