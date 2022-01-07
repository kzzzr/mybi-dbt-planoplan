SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_medium
	, ga_source
	, ga_keyword
	, ga_campaign
	, ga_sessions
	, ga_adcontent
	, ga_dimension13
	, ga_channelgrouping

FROM {{ source('ga', 'traffic_sources_utm') }}
