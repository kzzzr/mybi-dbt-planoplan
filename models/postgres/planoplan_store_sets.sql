SELECT

	  _airbyte_data

FROM {{ source('planoplan', 'store_sets') }}