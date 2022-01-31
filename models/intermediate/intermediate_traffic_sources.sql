-- 1.1. GA_Параметры источников трафика

WITH traffic_sources_utm AS (

    SELECT
        
          key_dt_session_id
        , dt
        , session_id

        , ga_channelgrouping
        , ga_source
        , ga_medium
        
        , ga_campaign
        , ga_adcontent	
        , ga_keyword	

        
    FROM {{ ref('stg_traffic_sources_utm') }}

)

, traffic_sources_full_path AS (

    SELECT

          key_dt_session_id
        , dt
        , session_id

        , ga_channelgrouping	  
        , ga_source
        , ga_medium

        , ga_fullreferrer
        , ga_sourcemedium

    FROM {{ ref('stg_traffic_sources_full_path') }}

)

SELECT
	
	  coalesce(a.dt, b.dt) as dt
    , coalesce(a.key_dt_session_id, b.key_dt_session_id) as key_dt_session_id
    , coalesce(a.session_id, b.session_id) as session_id

	, coalesce(a.ga_channelgrouping, b.ga_channelgrouping) as ga_channelgrouping
	, coalesce(a.ga_source, b.ga_source) as ga_source
	, coalesce(a.ga_medium, b.ga_medium) as ga_medium
	
	, a.ga_campaign as ga_campaign
	, a.ga_adcontent as ga_adcontent
	, a.ga_keyword as ga_keyword
	
	, b.ga_fullreferrer as ga_fullreferrer
	, b.ga_sourcemedium as ga_sourcemedium

FROM traffic_sources_utm AS a
	FULL OUTER JOIN traffic_sources_full_path AS b ON a.key_dt_session_id = b.key_dt_session_id
		AND a.ga_channelgrouping = b.ga_channelgrouping
		AND a.ga_source = b.ga_source
		AND a.ga_medium = b.ga_medium
