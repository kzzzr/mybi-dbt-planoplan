SELECT

	  JSONExtract(_airbyte_data, 'ga_date', 'String') as ga_date
	, JSONExtract(_airbyte_data, 'ga_bounces', 'Float32') as ga_bounces
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'ga_usertype', 'String') as ga_usertype
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension1', 'String') as ga_dimension1
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_sessionduration', 'String') as ga_sessionduration

FROM {{ source('mybi', 'seances') }}
