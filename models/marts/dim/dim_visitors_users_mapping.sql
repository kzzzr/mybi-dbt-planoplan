{{ 
    config(
          engine="MergeTree()"
        , order_by=['visitor_id']
        , partition_by=None
    )
}}

WITH unioned AS (

    SELECT 

        user_id
        , visitor_id

    FROM {{ ref('int_ga_users') }}

    UNION DISTINCT

    SELECT 

        user_id
        , visitor_id

    FROM {{ ref('int_pg_users') }}

)

SELECT 

    user_id
    , visitor_id
    , min(user_id) OVER (PARTITION BY visitor_id) AS user_id_min

FROM unioned