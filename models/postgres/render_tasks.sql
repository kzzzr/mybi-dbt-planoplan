{{
    config (
        materialized='view'
    )
}}

SELECT

	  _airbyte_data

FROM {{ source('render', 'tasks') }}
