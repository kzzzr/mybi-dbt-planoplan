SELECT

	  _airbyte_data

FROM {{ source('planoplan', 'tariffs_stat') }}
