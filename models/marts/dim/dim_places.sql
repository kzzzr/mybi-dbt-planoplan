SELECT DISTINCT 

	  halfMD5(ga_country, ga_region, ga_city) as place_id
	, ga_country
	, ga_region
	, ga_city

FROM {{ ref('stg_places') }}
