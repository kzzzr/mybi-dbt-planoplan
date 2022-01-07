SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_language
	, multiIf(
		ilike(ga_language, '%ru%'), 'RU',
		ilike(ga_language, '%de%'), 'DE',
		ilike(ga_language, '%en%'), 'EN',
		'Others'
	  ) AS ga_language_group

	, ga_sessions
	, ga_dimension13

FROM {{ source('ga', 'languages') }}
