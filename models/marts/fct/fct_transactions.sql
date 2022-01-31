
SELECT

      tr.dt as dt

    -- идентификатор транзакции - Transactions
	, tr.ga_transactions as ga_transactions
	, tr.ga_transactionid as ga_transactionid
	, tr.ga_transactionrevenue as ga_transactionrevenue      

    -- идентификатор сессии - Session    
    , tr.key_dt_session_id as key_dt_session_id
	, tr.session_id as session_id   
    -- , 'transactions' as _row_source    

    -- идентификатор источника трафика - Traffic sources
    , halfMD5(ts.ga_channelgrouping, ts.ga_source, ts.ga_medium, ts.ga_campaign, ts.ga_adcontent, ts.ga_keyword, ts.ga_fullreferrer) as traffic_source_id
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
	-- , s.ga_usertype as ga_usertype

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

from {{ ref('stg_transactions') }} as tr
    left any join {{ ref('intermediate_traffic_sources') }} as ts on tr.session_id = ts.session_id
    left any join {{ ref('stg_places') }} as pl on tr.session_id = pl.session_id
    left any join {{ ref('stg_devices') }} as dvc on tr.session_id = dvc.session_id
    left any join {{ ref('stg_languages') }} as lg on tr.session_id = lg.session_id
    left any join {{ ref('stg_users') }} as us on tr.session_id = us.session_id
    left any join {{ ref('stg_seances') }} as s on tr.session_id = s.session_id

