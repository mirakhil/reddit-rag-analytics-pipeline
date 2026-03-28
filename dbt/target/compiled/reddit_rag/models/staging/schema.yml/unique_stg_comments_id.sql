
    
    

select
    id as unique_field,
    count(*) as n_records

from REDDIT_RAG.MARTS.stg_comments
where id is not null
group by id
having count(*) > 1


