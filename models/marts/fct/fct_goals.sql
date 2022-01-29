
SELECT

    -- идентификатор сессии - Session
      gl.session_id as session_id
    , gl.dt as dt
	, gl.ga_dimension13 as ga_dimension13
    -- , 'goals' as _row_source    

    -- идентификатор источника трафика - Traffic sources
    , halfMD5(ts.ga_channelgrouping, ts.ga_source, ts.ga_medium) as traffic_source_id
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
    , halfMD5(s.ga_usertype) AS usertype_id
	--, s.ga_usertype as ga_usertype

    -- идентификатор платформы

    -- идентификатор языка
    , halfMD5(lg.ga_language) AS language_id
    , halfMD5(lg.ga_language_group) AS language_group_id    
    -- , lg.ga_language as ga_language
	-- , lg.ga_language_group as ga_language_group
    
    -- идентификатор клиента
	, us.ga_dimension1 as visitor_id

    -- идентификатор пользователя
	, us.ga_dimension4 as user_id

    -- идентификатор цели - Goals
	, gl.ga_goal_id as goal_id
	-- , gl.ga_goal_name as ga_goal_name
	
    , gl.ga_goal_reaches as ga_goal_reaches    

from {{ ref('intermediate_goals') }} as gl
    left any join {{ ref('stg_seances') }} as s on gl.session_id = s.session_id
    left any join {{ ref('intermediate_traffic_sources') }} as ts on gl.session_id = ts.session_id
    left any join {{ ref('stg_places') }} as pl on gl.session_id = pl.session_id
    left any join {{ ref('stg_devices') }} as dvc on gl.session_id = dvc.session_id
    left any join {{ ref('stg_languages') }} as lg on gl.session_id = lg.session_id
    left any join {{ ref('stg_users') }} as us on gl.session_id = us.session_id
