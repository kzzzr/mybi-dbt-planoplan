SELECT

	  _airbyte_data

FROM {{ source('planoplan', 'users') }}
