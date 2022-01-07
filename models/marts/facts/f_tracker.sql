{{
    config (
        order_by=['_row_source', 'session_id'],
        partition_by=['toYYYYMM(dt)']
    )
}}

SELECT

	  session_id
	, dt
	, ga_dimension13
	, `_row_source`
	, ga_channelgrouping
	, ga_source
	, ga_medium
	, ga_campaign
	, ga_adcontent
	, ga_keyword
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_country
	, ga_region
	, ga_city
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion
	, ga_usertype
	, ga_language
	, ga_language_group
	, ga_dimension1
	, ga_dimension4

    -- events
	, ga_eventcategory
	, ga_eventaction
	, ga_eventlabel
	, ga_eventvalue
	, ga_totalevents
	, ga_uniqueevents

    -- goals
	, null as ga_goal_id
	, null as ga_goal_name
	, null as ga_goal_reaches

    -- pageviews
	, null as ga_pageviews
	, null as ga_timeonpage
	, null as ga_entrances
	, null as ga_exits
	, null as ga_pagepath
	, null as ga_pagetitle

    -- seances
	, null as ga_sessions
	, null as ga_bounces
	, null as ga_sessionduration

    -- transactions
	, null as ga_transactions
	, null as ga_transactionid
	, null as ga_transactionrevenue    

FROM {{ ref('f_events') }}

UNION ALL

SELECT

	  session_id
	, dt
	, ga_dimension13
	, `_row_source`
	, ga_channelgrouping
	, ga_source
	, ga_medium
	, ga_campaign
	, ga_adcontent
	, ga_keyword
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_country
	, ga_region
	, ga_city
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion
	, ga_usertype
	, ga_language
	, ga_language_group
	, ga_dimension1
	, ga_dimension4

    -- events
	, null as ga_eventcategory
	, null as ga_eventaction
	, null as ga_eventlabel
	, null as ga_eventvalue
	, null as ga_totalevents
	, null as ga_uniqueevents    

    -- goals
	, ga_goal_id
	, ga_goal_name
	, ga_goal_reaches

    -- pageviews
	, null as ga_pageviews
	, null as ga_timeonpage
	, null as ga_entrances
	, null as ga_exits
	, null as ga_pagepath
	, null as ga_pagetitle    

    -- seances
	, null as ga_sessions
	, null as ga_bounces
	, null as ga_sessionduration

    -- transactions
	, null as ga_transactions
	, null as ga_transactionid
	, null as ga_transactionrevenue    

FROM {{ ref('f_goals') }}

UNION ALL

SELECT

      session_id
	, dt
	, ga_dimension13
	, `_row_source`
	, ga_channelgrouping
	, ga_source
	, ga_medium
	, ga_campaign
	, ga_adcontent
	, ga_keyword
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_country
	, ga_region
	, ga_city
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion
	, ga_usertype
	, ga_language
	, ga_language_group
	, ga_dimension1
	, ga_dimension4

    -- events
	, null as ga_eventcategory
	, null as ga_eventaction
	, null as ga_eventlabel
	, null as ga_eventvalue
	, null as ga_totalevents
	, null as ga_uniqueevents  

    -- goals
	, null as ga_goal_id
	, null as ga_goal_name
	, null as ga_goal_reaches      

    -- pageviews
	, ga_pageviews
	, ga_timeonpage
	, ga_entrances
	, ga_exits
	, ga_pagepath
	, ga_pagetitle

    -- seances
	, null as ga_sessions
	, null as ga_bounces
	, null as ga_sessionduration

    -- transactions
	, null as ga_transactions
	, null as ga_transactionid
	, null as ga_transactionrevenue    

FROM {{ ref('f_pageviews') }}

UNION ALL

SELECT
	  session_id
	, dt
	, ga_dimension13
	, `_row_source`
	, ga_channelgrouping
	, ga_source
	, ga_medium
	, ga_campaign
	, ga_adcontent
	, ga_keyword
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_country
	, ga_region
	, ga_city
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion
	, ga_usertype
	, ga_language
	, ga_language_group
	, ga_dimension1
	, ga_dimension4

    -- events
	, null as ga_eventcategory
	, null as ga_eventaction
	, null as ga_eventlabel
	, null as ga_eventvalue
	, null as ga_totalevents
	, null as ga_uniqueevents    

    -- goals
	, null as ga_goal_id
	, null as ga_goal_name
	, null as ga_goal_reaches    

    -- pageviews
	, null as ga_pageviews
	, null as ga_timeonpage
	, null as ga_entrances
	, null as ga_exits
	, null as ga_pagepath
	, null as ga_pagetitle    

    -- seances
	, ga_sessions
	, ga_bounces
	, ga_sessionduration    

    -- transactions
	, null as ga_transactions
	, null as ga_transactionid
	, null as ga_transactionrevenue

FROM {{ ref('f_seances') }}

UNION ALL

SELECT

	  session_id
	, dt
	, ga_dimension13
	, `_row_source`
	, ga_channelgrouping
	, ga_source
	, ga_medium
	, ga_campaign
	, ga_adcontent
	, ga_keyword
	, ga_fullreferrer
	, ga_sourcemedium
	, ga_country
	, ga_region
	, ga_city
	, ga_browser
	, ga_devicecategory
	, ga_browserversion
	, ga_operatingsystem
	, ga_operatingsystemversion
	, ga_usertype
	, ga_language
	, ga_language_group
	, ga_dimension1
	, ga_dimension4

    -- events
	, null as ga_eventcategory
	, null as ga_eventaction
	, null as ga_eventlabel
	, null as ga_eventvalue
	, null as ga_totalevents
	, null as ga_uniqueevents    

    -- goals
	, null as ga_goal_id
	, null as ga_goal_name
	, null as ga_goal_reaches   

    -- pageviews
	, null as ga_pageviews
	, null as ga_timeonpage
	, null as ga_entrances
	, null as ga_exits
	, null as ga_pagepath
	, null as ga_pagetitle     

    -- seances
	, null as ga_sessions
	, null as ga_bounces
	, null as ga_sessionduration    
    
    -- transactions
	, ga_transactions
	, ga_transactionid
	, ga_transactionrevenue

FROM {{ ref('f_transactions') }}
