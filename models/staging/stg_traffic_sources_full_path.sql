SELECT


	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_medium
	, ga_source
	, ga_sessions
	, ga_dimension13 as session_id
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_channelgrouping

FROM {{ source('hist', 'traffic_sources_full_path') }}

UNION ALL 

SELECT


	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_medium
	, ga_source
	, ga_sessions
	, ga_dimension13 as session_id
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_channelgrouping

FROM {{ ref('flt_traffic_sources_full_path') }}