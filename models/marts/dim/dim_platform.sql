SELECT DISTINCT 

	  halfMD5(platform) AS platform_id
	, platform

FROM {{ ref('stg_platform') }}