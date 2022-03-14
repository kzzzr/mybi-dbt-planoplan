SELECT DISTINCT 
	
    key_dt_session_id as session_id

FROM {{ ref('stg_seances') }}
