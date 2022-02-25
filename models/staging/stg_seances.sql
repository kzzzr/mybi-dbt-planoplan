SELECT


	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_date
	, ga_bounces
	, ga_sessions
	, ga_usertype
	, ga_dimension1
	, ga_dimension13 as session_id
	, toFloat32OrZero(ga_sessionduration) as ga_sessionduration

FROM {{ source('hist', 'seances') }}

UNION ALL 

SELECT


	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_date
	, ga_bounces
	, ga_sessions
	, ga_usertype
	, ga_dimension1
	, ga_dimension13 as session_id
	, toFloat32OrZero(ga_sessionduration) as ga_sessionduration

FROM {{ ref('flt_seances') }}