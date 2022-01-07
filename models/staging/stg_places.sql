SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_country
	, ga_region
	, ga_city
	, ga_sessions
	, ga_dimension13

FROM {{ source('ga', 'places') }}
