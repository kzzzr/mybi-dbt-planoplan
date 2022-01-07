SELECT

	  concat(CAST(CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS String), ':', ga_dimension13) AS session_id	  
	, CAST(parseDateTime32BestEffortOrNull(simple_date) AS DATE) AS dt

	, ga_dimension13
	, ga_goal1completions
	, ga_goal2completions
	, ga_goal3completions
	, ga_goal4completions
	, ga_goal5completions
	, ga_goal6completions
	, ga_goal7completions
	, ga_goal8completions
	, ga_goal9completions
	, ga_goal13completions

FROM {{ source('ga', 'goals') }}
