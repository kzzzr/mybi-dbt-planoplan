SELECT

	  JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension1', 'String') as ga_dimension1
	, JSONExtract(_airbyte_data, 'ga_dimension4', 'String') as ga_dimension4
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_datehourminute', 'String') as ga_datehourminute

FROM {{ source('mybi', 'users') }}
