
SELECT

    -- идентификатор сессии - Session
      pv.session_id as session_id
    , pv.dt as dt
	, pv.ga_dimension13 as ga_dimension13
    , 'pageviews' as _row_source    

    -- идентификатор источника трафика - Traffic sources
    , ts.ga_channelgrouping as ga_channelgrouping
    , ts.ga_source as ga_source
    , ts.ga_medium as ga_medium
    , ts.ga_campaign as ga_campaign
    , ts.ga_adcontent as ga_adcontent
    , ts.ga_keyword as ga_keyword
    , ts.ga_fullreferrer as ga_fullreferrer
    , ts.ga_sourcemedium as ga_sourcemedium

    -- идентификатор местоположения - Places
	, pl.ga_country as ga_country
	, pl.ga_region as ga_region
	, pl.ga_city as ga_city

    -- идентификатор типа устройства
	, dvc.ga_browser as ga_browser
	, dvc.ga_devicecategory as ga_devicecategory
	, dvc.ga_browserversion as ga_browserversion
	, dvc.ga_operatingsystem as ga_operatingsystem
	, dvc.ga_operatingsystemversion as ga_operatingsystemversion

    -- идентификатор типа посетителя
	, s.ga_usertype as ga_usertype

    -- идентификатор платформы

    -- идентификатор языка
    , lg.ga_language as ga_language
	, lg.ga_language_group as ga_language_group
    
    -- идентификатор клиента
	, us.ga_dimension1 as ga_dimension1

    -- идентификатор пользователя
	, us.ga_dimension4 as ga_dimension4

    -- идентификатор просмотра - Page Views
	, pv.ga_pageviews as ga_pageviews
	, pv.ga_timeonpage as ga_timeonpage
	, pv.ga_entrances as ga_entrances
	, pv.ga_exits as ga_exits
	, pv.ga_pagepath as ga_pagepath
	, pv.ga_pagetitle as ga_pagetitle

from {{ ref('stg_pageviews') }} as pv
    left any join {{ ref('stg_seances') }} as s on pv.session_id = s.session_id
    left any join {{ ref('int_traffic_sources') }} as ts on pv.session_id = ts.session_id
    left any join {{ ref('stg_places') }} as pl on pv.session_id = pl.session_id
    left any join {{ ref('stg_devices') }} as dvc on pv.session_id = dvc.session_id
    left any join {{ ref('stg_languages') }} as lg on pv.session_id = lg.session_id
    left any join {{ ref('stg_users') }} as us on pv.session_id = us.session_id

