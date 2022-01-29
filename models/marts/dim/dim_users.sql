SELECT DISTINCT 

	ga_dimension4 as user_id

FROM {{ ref('stg_users') }}