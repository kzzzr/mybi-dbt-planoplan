{{ 
    config(
          engine="MergeTree()"
        , order_by=['user_id_min']
        , partition_by=None
    )
}}

SELECT DISTINCT

      user_id_min
    , min(dt) over (partition by user_id_min) as first_session_dt
    , first_value(visitor_id) over (partition by user_id_min order by dt asc) as first_session_visitor_id

from {{ ref('fct_seances') }} as seances

settings max_memory_usage = 20000000000000