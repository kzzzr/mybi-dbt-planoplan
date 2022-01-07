SELECT DISTINCT

      halfMD5(ga_channelgrouping, ga_source, ga_medium) as traffic_source_id

	, ga_channelgrouping
	, ga_source
	, ga_medium
	
	, ga_campaign
	, ga_adcontent
	, ga_keyword
	
	, ga_fullreferrer
	, ga_sourcemedium

FROM {{ ref('intermediate_traffic_sources') }}
