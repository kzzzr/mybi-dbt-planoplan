SELECT DISTINCT 

	ga_dimension1 as visitor_id

FROM {{ ref('stg_users') }}