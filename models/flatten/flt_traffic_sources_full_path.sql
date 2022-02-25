SELECT

	  JSONExtract(_airbyte_data, 'ga_medium', 'String') as ga_medium
	, JSONExtract(_airbyte_data, 'ga_source', 'String') as ga_source
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_fullreferrer', 'String') as ga_fullreferrer
	, JSONExtract(_airbyte_data, 'ga_sourcemedium', 'String') as ga_sourcemedium
	, JSONExtract(_airbyte_data, 'ga_channelgrouping', 'String') as ga_channelgrouping
	
FROM {{ source('mybi', 'traffic_sources_full_path') }}
