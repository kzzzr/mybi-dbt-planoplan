SELECT DISTINCT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_sessions
	, ga_dimension13 as session_id
	
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion

FROM {{ source('hist', 'devices') }}

UNION ALL 

SELECT DISTINCT

	  assumeNotNull(concat(CAST(CAST(simple_date AS DATE) AS String), ':', ga_dimension13)) AS key_dt_session_id	  
	, CAST(simple_date AS DATE) AS dt

	, ga_sessions
	, ga_dimension13 as session_id
	
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion

FROM {{ source('mybi_qfjrlkq', 'devices') }}
