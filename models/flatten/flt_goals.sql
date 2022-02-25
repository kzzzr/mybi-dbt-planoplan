SELECT

	  JSONExtract(_airbyte_data, 'simple_date', 'String') as simple_date
	, JSONExtract(_airbyte_data, 'ga_dimension13', 'String') as ga_dimension13
	, JSONExtract(_airbyte_data, 'ga_goal1completions', 'String') as ga_goal1completions
	, JSONExtract(_airbyte_data, 'ga_goal2completions', 'String') as ga_goal2completions
	, JSONExtract(_airbyte_data, 'ga_goal3completions', 'String') as ga_goal3completions
	, JSONExtract(_airbyte_data, 'ga_goal4completions', 'String') as ga_goal4completions
	, JSONExtract(_airbyte_data, 'ga_goal5completions', 'String') as ga_goal5completions
	, JSONExtract(_airbyte_data, 'ga_goal6completions', 'String') as ga_goal6completions
	, JSONExtract(_airbyte_data, 'ga_goal7completions', 'String') as ga_goal7completions
	, JSONExtract(_airbyte_data, 'ga_goal8completions', 'String') as ga_goal8completions
	, JSONExtract(_airbyte_data, 'ga_goal9completions', 'String') as ga_goal9completions
	, JSONExtract(_airbyte_data, 'ga_goal13completions', 'String') as ga_goal13completions
	
FROM {{ source('mybi', 'goals') }}
