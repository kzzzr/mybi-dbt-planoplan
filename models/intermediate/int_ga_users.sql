{{ 
    config(
          engine="MergeTree()"
        , order_by=['visitor_id']
        , partition_by=None
    )
}}

SELECT DISTINCT

	  assumeNotNull(toInt32(ga_dimension4)) as user_id
    , assumeNotNull(ga_dimension1) as visitor_id

FROM {{ ref('stg_users') }}
WHERE ga_dimension4 IS NOT NULL
    AND ga_dimension4 NOT IN ('undefined', 'none', 'new UserID from CMS', 0)
