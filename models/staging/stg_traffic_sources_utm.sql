SELECT DISTINCT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_medium
	, ga_source
	, ga_keyword
	, ga_campaign
	, ga_sessions
	, ga_adcontent
	, ga_dimension13 as session_id
	, ga_channelgrouping

FROM {{ source('hist', 'traffic_sources_utm') }}

UNION ALL 

SELECT DISTINCT

	  assumeNotNull(concat(CAST(CAST(simple_date AS DATE) AS String), ':', ga_dimension13)) AS key_dt_session_id	  
	, CAST(simple_date AS DATE) AS dt

	, ga_medium
	, ga_source
	, ga_keyword
	, ga_campaign
	, ga_sessions
	, ga_adcontent
	, ga_dimension13 as session_id
	, ga_channelgrouping

FROM {{ source('mybi_dtftgmb', 'traffic_sources_utm') }}