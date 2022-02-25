SELECT

	  JSONExtract(_airbyte_data, 'ga_exits', 'Float32') as ga_exits
	, JSONExtract(_airbyte_data, 'ga_pagepath', 'String') as ga_pagepath
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_entrances', 'Float32') as ga_entrances
	, JSONExtract(_airbyte_data, 'ga_pagetitle', 'String') as ga_pagetitle
	, JSONExtract(_airbyte_data, 'ga_pageviews', 'Float32') as ga_pageviews
	, JSONExtract(_airbyte_data, 'ga_timeonpage', 'String') as ga_timeonpage
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	
FROM {{ source('mybi', 'pageviews') }}