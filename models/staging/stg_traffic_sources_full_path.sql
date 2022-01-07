SELECT


	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_medium
	, ga_source
	, ga_sessions
	, ga_dimension13
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_channelgrouping

FROM {{ source('ga', 'traffic_sources_full_path') }}
