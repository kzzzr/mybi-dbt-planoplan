SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_sessions
	, ga_dimension1
	, ga_dimension4
	, ga_dimension13
	, ga_datehourminute

FROM {{ source('ga', 'users') }}
