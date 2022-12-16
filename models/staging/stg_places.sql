SELECT DISTINCT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_country
	, ga_region
	, ga_city
	, ga_sessions
	, ga_dimension13 as session_id

FROM {{ source('hist', 'places') }}

UNION ALL 

SELECT DISTINCT

	  assumeNotNull(concat(CAST(CAST(simple_date AS DATE) AS String), ':', ga_dimension13)) AS key_dt_session_id	  
	, CAST(simple_date AS DATE) AS dt

	, ga_country
	, ga_region
	, ga_city
	, ga_sessions
	, ga_dimension13 as session_id

FROM {{ source('mybi_qfjrlkq', 'places') }}
