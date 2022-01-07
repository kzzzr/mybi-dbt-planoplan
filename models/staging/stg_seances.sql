SELECT


	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_date
	, ga_bounces
	, ga_sessions
	, ga_usertype
	, ga_dimension1
	, ga_dimension13
	, toFloat32OrZero(ga_sessionduration) as ga_sessionduration

FROM {{ source('ga', 'seances') }}
