SELECT

	  JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_transactions', 'Float32') as ga_transactions
	, JSONExtract(_airbyte_data, 'ga_transactionid', 'String') as ga_transactionid
	, JSONExtract(_airbyte_data, 'ga_transactionrevenue', 'String') as ga_transactionrevenue

FROM {{ source('mybi', 'transactions') }}
