SELECT DISTINCT 

	  halfMD5(ga_usertype) AS language_group_id
	, ga_usertype

FROM {{ ref('stg_seances') }}
