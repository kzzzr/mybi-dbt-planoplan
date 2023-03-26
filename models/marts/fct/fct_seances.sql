WITH first_seance AS (

    SELECT
          s.key_dt_session_id
        , ROW_NUMBER() OVER (PARTITION BY s.user_id_min, s.dt, s.usertype_id ORDER BY s.session_id ASC) AS group_row_number
    FROM {{ ref('int_fct_seances_windowed') }} AS s
    WHERE 1=1
        AND s.dt = s.user_id_min_first_dt
        AND s.visitor_id ILIKE '%.%'
        AND s.usertype_id IN (5908804507569195084)    

)

SELECT

      s.dt

    -- идентификатор сессии - Session
	, s.ga_sessions
	, s.ga_bounces
	, s.ga_sessionduration

    -- идентификатор сессии - Session
    , s.key_dt_session_id
	, s.session_id
    -- , 'seances' as _row_source    

    -- идентификатор источника трафика - Traffic sources
    , s.traffic_source_id
    -- , ts.ga_channelgrouping as ga_channelgrouping
    -- , ts.ga_source as ga_source
    -- , ts.ga_medium as ga_medium
    -- , ts.ga_campaign as ga_campaign
    -- , ts.ga_adcontent as ga_adcontent
    -- , ts.ga_keyword as ga_keyword
    -- , ts.ga_fullreferrer as ga_fullreferrer
    -- , ts.ga_sourcemedium as ga_sourcemedium

    -- идентификатор местоположения - Places
    , s.place_id
	-- , pl.ga_country as ga_country
	-- , pl.ga_region as ga_region
	-- , pl.ga_city as ga_city

    -- идентификатор типа устройства
    , s.device_id
	-- , dvc.ga_browser as ga_browser
	-- , dvc.ga_devicecategory as ga_devicecategory
	-- , dvc.ga_browserversion as ga_browserversion
	-- , dvc.ga_operatingsystem as ga_operatingsystem
	-- , dvc.ga_operatingsystemversion as ga_operatingsystemversion

    -- идентификатор типа посетителя
    , s.usertype_id
	-- , s.ga_usertype as ga_usertype

    -- идентификатор платформы
    , s.platform_id

    -- идентификатор языка
    , s.language_id
    , s.language_group_id
    -- , lg.ga_language as ga_language
	-- , lg.ga_language_group as ga_language_group
    
    -- идентификатор клиента
	, s.visitor_id
	, s.user_id

    -- идентификатор пользователя
	, s.user_id_full
	, s.user_id_min
    , s.user_id_min_first_dt
    , CASE
        WHEN 1=1
            AND s.dt = s.user_id_min_first_dt
            AND s.visitor_id ILIKE '%.%'
            AND s.usertype_id IN (5908804507569195084)
            AND w.group_row_number = 1
                THEN 'New Visitor'
        ELSE 'Returning Visitor'
      END AS user_type

from {{ ref('int_fct_seances_windowed') }} as s
    left any join first_seance as w using (key_dt_session_id)

settings max_memory_usage = 20000000000000