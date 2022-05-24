WITH goals AS (

    SELECT

          ga_goal1completions as ga_goal1
        , ga_goal2completions as ga_goal2
        , ga_goal3completions as ga_goal3
        , ga_goal4completions as ga_goal4
        , ga_goal5completions as ga_goal5
        , ga_goal6completions as ga_goal6
        , ga_goal7completions as ga_goal7
        , ga_goal8completions as ga_goal8
        , ga_goal9completions as ga_goal9
        , ga_goal13completions as ga_goal13

    FROM {{ ref('stg_goals') }}

)


SELECT DISTINCT 

      tpl.1 AS goal_id
    , CASE goal_id
    	WHEN '1' THEN 'Регистрация'
    	WHEN '2' THEN 'Подтверждение регистрации'
    	WHEN '3' THEN 'Посещение страницы Магазин'
    	WHEN '4' THEN 'Управление тарифом'
    	WHEN '5' THEN 'Скачан инсталлятор'
    	WHEN '6' THEN 'Работал в Web'
    	WHEN '7' THEN 'Работал в Standalone'
    	WHEN '8' THEN 'Посещение страницы проекта (ALL)'
    	WHEN '9' THEN 'Успешная оплата'
    	WHEN '10' THEN 'Улучшение тарифа (успешная оплата)'
    	ELSE ''
      END AS goal_name	 

FROM goals
    ARRAY JOIN tupleToNameValuePairs((ga_goal1, ga_goal2, ga_goal3, ga_goal4, ga_goal5, ga_goal6, ga_goal7, ga_goal8, ga_goal9, ga_goal13)) AS tpl
