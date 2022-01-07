SELECT DISTINCT 

	  halfMD5(ga_eventcategory, ga_eventaction, ga_eventlabel)
	, ga_eventcategory
	, ga_eventaction
	, ga_eventlabel	

FROM {{ ref('stg_events') }}