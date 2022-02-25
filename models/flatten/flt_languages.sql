SELECT

	  JSONExtract(_airbyte_data, 'ga_language', 'String') as ga_language
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	
FROM {{ source('mybi', 'languages') }}
