SELECT DISTINCT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS key_dt_session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt
	
	, ga_dimension13 as session_id
	, ga_transactions
	, ga_transactionid
	, toFloat32OrZero(ga_transactionrevenue) as ga_transactionrevenue

FROM {{ source('hist', 'transactions') }}

UNION DISTINCT 

SELECT DISTINCT

	  assumeNotNull(concat(CAST(CAST(simple_date AS DATE) AS String), ':', ga_dimension13)) AS key_dt_session_id	  
	, CAST(simple_date AS DATE) AS dt
	
	, ga_dimension13 as session_id
	, ga_transactions
	, ga_transactionid
	, toFloat32OrZero(ga_transactionrevenue) as ga_transactionrevenue

FROM {{ source('mybi_qfjrlkq', 'transactions') }}