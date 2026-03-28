
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select post_title
from REDDIT_RAG.MARTS.fct_posts
where post_title is null



  
  
      
    ) dbt_internal_test