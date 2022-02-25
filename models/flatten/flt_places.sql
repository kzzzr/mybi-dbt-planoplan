SELECT

	  JSONExtract(_airbyte_data, 'ga_city', 'String') as ga_city
	, JSONExtract(_airbyte_data, 'ga_region', 'String') as ga_region
	, JSONExtract(_airbyte_data, 'ga_country', 'String') as ga_country
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	
FROM {{ source('mybi', 'places') }}
