SELECT DISTINCT 

	  halfMD5(ga_language) AS language_id
	, ga_language

FROM {{ ref('stg_languages') }}