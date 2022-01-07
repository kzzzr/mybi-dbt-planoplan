SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_sessions
	, ga_dimension4
	, ga_dimension13
	, ga_dimension16
	, ga_dimension17

FROM {{ source('ga', 'registration_polls') }}
