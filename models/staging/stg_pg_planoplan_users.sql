{{ 
    config(
          engine="MergeTree()"
        , order_by=["user_id"]
        , partition_by=None
    )
}}

SELECT

      id AS user_id
	, stat_cid_ga AS visitor_ids

FROM {{ source('planoplan', 'users') }}
