{{ 
    config(
          engine="MergeTree()"
        , order_by=["visitor_id"]
        , partition_by=None
    )
}}

SELECT DISTINCT

      user_id
	, SUBSTRING(REPLACE(visitor_ids_array, '"', ''), 7, 256) as visitor_id
    --, REPLACE(visitor_ids_array, '"', '') as visitor_id_raw
    --,stat_cid_ga 
    --,LENGTH(stat_cid_ga_array_element)
    --,JSONExtractArrayRaw(stat_cid_ga) AS stat_cid_ga_array_raw
    --,JSONExtractRaw(stat_cid_ga) AS stat_cid_ga_array
    --,JSONLength(stat_cid_ga)
    --,toTypeName(stat_cid_ga_arr)
    --,JSONType(stat_cid_ga)

FROM {{ ref('stg_pg_planoplan_users') }}
    ARRAY JOIN JSONExtractArrayRaw(visitor_ids) AS visitor_ids_array
