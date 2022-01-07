SELECT DISTINCT 
	
    session_id

FROM {{ ref('stg_seances') }}
