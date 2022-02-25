SELECT

	  JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension4', 'String') as ga_dimension4
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_dimension16', 'String') as ga_dimension16
	, JSONExtract(_airbyte_data, 'ga_dimension17', 'String') as ga_dimension17

FROM {{ source('mybi', 'registration_polls') }}
