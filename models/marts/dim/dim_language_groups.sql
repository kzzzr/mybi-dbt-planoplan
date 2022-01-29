SELECT DISTINCT 

	  halfMD5(ga_language_group) AS language_group_id
	, ga_language_group
    
FROM {{ ref('stg_languages') }}