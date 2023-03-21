SELECT DISTINCT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_sessions
    -- remove GA1.X prefix where applicable
    , CASE
        WHEN ga_dimension1 ILIKE '%GA%' THEN substring(ga_dimension1, 7, 256)
        ELSE ga_dimension1
      END AS ga_dimension1
	, ga_dimension4
	, ga_dimension13 as session_id
	, ga_datehourminute

FROM {{ source('hist', 'users') }}

UNION DISTINCT 

SELECT DISTINCT

	  assumeNotNull(concat(CAST(CAST(simple_date AS DATE) AS String), ':', ga_dimension13)) AS key_dt_session_id	  
	, CAST(simple_date AS DATE) AS dt

	, ga_sessions
    -- remove GA1.X prefix where applicable
    , CASE
        WHEN ga_dimension1 ILIKE '%GA%' THEN substring(ga_dimension1, 7, 256)
        ELSE ga_dimension1
      END AS ga_dimension1
	, ga_dimension4
	, ga_dimension13 as session_id
	, ga_datehourminute

FROM {{ source('mybi_dtftgmb', 'users') }}
