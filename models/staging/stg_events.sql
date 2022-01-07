SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_dimension13

	, ga_pagepath
	
	, ga_eventcategory
	, ga_eventaction
	, ga_eventlabel

	, ga_eventvalue
	, ga_totalevents
	, ga_uniqueevents

FROM {{ source('ga', 'events') }}