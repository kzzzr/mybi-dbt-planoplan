{{ 
    config(
          engine="MergeTree()"
        , order_by=['user_id_min']
        , partition_by=None
    )
}}

SELECT DISTINCT

      user_id_min
    , min(CASE WHEN user_type IN 'New Visitor' THEN dt ELSE NULL END) over (partition by user_id_min) as first_session_dt
    , first_value(visitor_id) over (partition by user_id_min order by dt asc) as first_session_visitor_id

from {{ ref('fct_seances') }} as seances

where user_id_min not in (0)

settings max_memory_usage = 20000000000000