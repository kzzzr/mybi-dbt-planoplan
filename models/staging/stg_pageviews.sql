SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt
	
	, ga_dimension13 as session_id
	
	, ga_pageviews
	, toFloat32OrZero(ga_timeonpage) as ga_timeonpage
	, ga_entrances
	, ga_exits
	, ga_pagepath
	, ga_pagetitle

FROM {{ source('hist', 'pageviews') }}

UNION ALL 

SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt
	
	, ga_dimension13 as session_id
	
	, ga_pageviews
	, toFloat32OrZero(ga_timeonpage) as ga_timeonpage
	, ga_entrances
	, ga_exits
	, ga_pagepath
	, ga_pagetitle

FROM {{ ref('flt_pageviews') }}