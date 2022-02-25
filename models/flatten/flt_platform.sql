SELECT

	  JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension8', 'String') as ga_dimension8
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13	
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions

FROM {{ source('mybi', 'platforms') }}
