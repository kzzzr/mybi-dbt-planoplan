
SELECT

      s.dt as dt

    -- идентификатор сессии - Session
	, s.ga_sessions as ga_sessions
	, s.ga_bounces as ga_bounces
	, s.ga_sessionduration as ga_sessionduration

    -- идентификатор сессии - Session
    , s.key_dt_session_id as key_dt_session_id
	, s.session_id as session_id   
    -- , 'seances' as _row_source    

    -- идентификатор источника трафика - Traffic sources
    , s.traffic_source_id as traffic_source_id
    -- , ts.ga_channelgrouping as ga_channelgrouping
    -- , ts.ga_source as ga_source
    -- , ts.ga_medium as ga_medium
    -- , ts.ga_campaign as ga_campaign
    -- , ts.ga_adcontent as ga_adcontent
    -- , ts.ga_keyword as ga_keyword
    -- , ts.ga_fullreferrer as ga_fullreferrer
    -- , ts.ga_sourcemedium as ga_sourcemedium

    -- идентификатор местоположения - Places
    , halfMD5(pl.ga_country, pl.ga_region, pl.ga_city) as place_id
	-- , pl.ga_country as ga_country
	-- , pl.ga_region as ga_region
	-- , pl.ga_city as ga_city

    -- идентификатор типа устройства
    , halfMD5(dvc.ga_devicecategory, dvc.ga_browserversion, dvc.ga_operatingsystem, dvc.ga_operatingsystemversion) AS device_id
	-- , dvc.ga_browser as ga_browser
	-- , dvc.ga_devicecategory as ga_devicecategory
	-- , dvc.ga_browserversion as ga_browserversion
	-- , dvc.ga_operatingsystem as ga_operatingsystem
	-- , dvc.ga_operatingsystemversion as ga_operatingsystemversion

    -- идентификатор типа посетителя
    , s.usertype_id as usertype_id
	-- , s.ga_usertype as ga_usertype

    -- идентификатор платформы
    , halfMD5(pf.platform) AS platform_id

    -- идентификатор языка
    , halfMD5(lg.ga_language) AS language_id
    , halfMD5(lg.ga_language_group) AS language_group_id
    -- , lg.ga_language as ga_language
	-- , lg.ga_language_group as ga_language_group
    
    -- идентификатор клиента
	, us.ga_dimension1 as visitor_id
	, us.ga_dimension4 as user_id

    -- идентификатор пользователя
	, dim_visitors_users_mapping.user_id as user_id_full
	, dim_visitors_users_mapping.user_id_min as user_id_min
    , FIRST_VALUE(s.dt) OVER (PARTITION BY user_id_min ORDER BY dt ASC) AS user_id_min_first_dt
    --, ROW_NUMBER() OVER (PARTITION BY user_id_min ORDER BY dt ASC) AS seance_number

from {{ ref('int_fct_seances') }} as s
    --left any join {{ ref('int_traffic_sources') }} as ts on s.session_id = ts.session_id
    left any join {{ ref('stg_places') }} as pl on s.session_id = pl.session_id
    left any join {{ ref('stg_devices') }} as dvc on s.session_id = dvc.session_id
    left any join {{ ref('stg_languages') }} as lg on s.session_id = lg.session_id
    left any join {{ ref('stg_users') }} as us on s.session_id = us.session_id
    left any join {{ ref('dim_visitors_users_mapping') }} as dim_visitors_users_mapping on dim_visitors_users_mapping.visitor_id = us.ga_dimension1
    left any join {{ ref('stg_platform') }} as pf on s.session_id = pf.session_id

settings max_memory_usage = 20000000000000