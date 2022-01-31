SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_dimension13 as session_id

	, ga_pagepath
	
	, ga_eventcategory
	, ga_eventaction
	, ga_eventlabel

	, ga_eventvalue
	, ga_totalevents
	, ga_uniqueevents

FROM {{ source('ga', 'events') }}