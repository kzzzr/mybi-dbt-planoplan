SELECT DISTINCT 

	  halfMD5(ga_usertype) AS usertype_id
	, ga_usertype

FROM {{ ref('stg_seances') }}
