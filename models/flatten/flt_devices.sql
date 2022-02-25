SELECT

	  JSONExtract(_airbyte_data, 'ga_browser', 'String') as ga_browser
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_browserversion', 'String') as ga_browserversion
	, JSONExtract(_airbyte_data, 'ga_devicecategory', 'String') as ga_devicecategory
	, JSONExtract(_airbyte_data, 'ga_operatingsystem', 'String') as ga_operatingsystem
	, JSONExtract(_airbyte_data, 'ga_operatingsystemversion', 'String') as ga_operatingsystemversion

FROM {{ source('mybi', 'devices') }}
