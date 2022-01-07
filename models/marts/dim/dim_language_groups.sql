SELECT DISTINCT 

	  multiIf(
		ilike(ga_language, '%ru%'), 'RU',
		ilike(ga_language, '%de%'), 'DE',
		ilike(ga_language, '%en%'), 'EN',
		'Others'
	  ) AS language_group
	, halfMD5(language_group) AS language_group_id	  
    
FROM {{ ref('stg_languages') }}