SELECT DISTINCT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_sessions
	, ga_dimension13 as session_id
	
	, ga_dimension8
	, multiIf(
		ilike(ga_dimension8, '%platform:web%'), 'Web',
		ilike(ga_dimension8, '%platform:standalone%'), 'Standalone',
		'Not set'
		) as platform
	
FROM {{ source('hist', 'platforms') }}

UNION ALL

SELECT DISTINCT

	  assumeNotNull(concat(CAST(CAST(simple_date AS DATE) AS String), ':', ga_dimension13)) AS key_dt_session_id	  
	, CAST(simple_date AS DATE) AS dt

	, ga_sessions
	, ga_dimension13 as session_id
	
	, ga_dimension8
	, multiIf(
		ilike(ga_dimension8, '%platform:web%'), 'Web',
		ilike(ga_dimension8, '%platform:standalone%'), 'Standalone',
		'Not set'
		) as platform
	
FROM {{ source('mybi_fhqjlqs', 'platforms') }}