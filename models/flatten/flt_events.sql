SELECT

	  JSONExtract(_airbyte_data, 'ga_pagepath', 'String') as ga_pagepath
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_eventlabel', 'String') as ga_eventlabel
	, JSONExtract(_airbyte_data, 'ga_eventvalue', 'Float32') as ga_eventvalue
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_eventaction', 'String') as ga_eventaction
	, JSONExtract(_airbyte_data, 'ga_totalevents', 'Float32') as ga_totalevents
	, JSONExtract(_airbyte_data, 'ga_uniqueevents', 'Float32') as ga_uniqueevents
	, JSONExtract(_airbyte_data, 'ga_eventcategory', 'String') as ga_eventcategory

FROM {{ source('mybi', 'events') }}