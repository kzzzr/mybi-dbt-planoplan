SELECT

	  JSONExtract(_airbyte_data, 'ga_medium', 'String') as ga_medium
	, JSONExtract(_airbyte_data, 'ga_source', 'String') as ga_source
	, JSONExtract(_airbyte_data, 'ga_keyword', 'String') as ga_keyword
	, JSONExtract(_airbyte_data, 'ga_campaign', 'String') as ga_campaign
	, JSONExtract(_airbyte_data, 'ga_sessions', 'Float32') as ga_sessions
	, JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_adcontent', 'String') as ga_adcontent
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_channelgrouping', 'String') as ga_channelgrouping

FROM {{ source('mybi', 'traffic_sources_utm') }}
