SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt
	
	, ga_dimension13
	, ga_transactions
	, ga_transactionid
	, toFloat32OrZero(ga_transactionrevenue) as ga_transactionrevenue

FROM {{ source('ga', 'transactions') }}
