-- DROP TABLE planoplan_postgres.render_tasks ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.render_tasks
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

      JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
    , JSONExtract(_airbyte_data, 'accesses_id', 'UInt32') as `accesses_id`
    , JSONExtract(_airbyte_data, 'action', 'String') as `action`
    
    -- , JSONExtract(_airbyte_data, 'data', 'String') as `data`
	, JSONExtract(JSONExtract(_airbyte_data, 'data', 'String'), 'data', 'String') as data_data
	, JSONExtract(JSONExtract(_airbyte_data, 'data', 'String'), 'panoCamId', 'String') as data_panoCamId
	, JSONExtract(JSONExtract(_airbyte_data, 'data', 'String'), 'camInfo', 'String') as data_camInfo
	, JSONExtract(JSONExtract(_airbyte_data, 'data', 'String'), 'unityXML', 'String') as data_unityXML
	, JSONExtract(JSONExtract(_airbyte_data, 'data', 'String'), 'unityAdditionalXML', 'String') as data_unityAdditionalXML
	, JSONExtract(JSONExtract(_airbyte_data, 'data', 'String'), 'lang', 'String') as data_lang

    , JSONExtract(_airbyte_data, 'callback', 'String') as `callback`
    , JSONExtract(_airbyte_data, 'callback_status', 'String') as `callback_status`
    , JSONExtract(_airbyte_data, 'priority', 'UInt32') as `priority`
    , JSONExtract(_airbyte_data, 'status', 'String') as `status`
    , JSONExtract(_airbyte_data, 'info', 'String') as `info`
    , JSONExtract(_airbyte_data, 'nodes_id', 'UInt32') as `nodes_id`
    , JSONExtract(_airbyte_data, 'attempts', 'UInt32') as `attempts`
    , JSONExtract(_airbyte_data, 'vip', 'String') as `vip`
    , JSONExtract(_airbyte_data, 'build_time', 'UInt32') as `build_time`
    , JSONExtract(_airbyte_data, 'deleted', 'String') as `deleted`
    , JSONExtract(_airbyte_data, 'result_urls', 'String') as `result_urls`
    , JSONExtract(_airbyte_data, 'filters_data', 'String') as `filters_data`
    , JSONExtract(_airbyte_data, 'archive', 'String') as `archive`
    , JSONExtract(_airbyte_data, 'filters_data_user_id', 'UInt32') as `filters_data_user_id`
    , JSONExtract(_airbyte_data, 'filters_data_project_id', 'UInt32') as `filters_data_project_id`
    , JSONExtract(_airbyte_data, 'filters_data_client_id', 'UInt32') as `filters_data_client_id`
    , JSONExtract(_airbyte_data, 'render_time', 'UInt32') as `render_time`
    , JSONExtract(_airbyte_data, 'filters_data_team_id', 'UInt32') as `filters_data_team_id`
    , JSONExtract(_airbyte_data, 'max_memory_usage', 'UInt32') as `max_memory_usage`
    , JSONExtract(_airbyte_data, 'max_gpu_memory_usage', 'UInt32') as `max_gpu_memory_usage`
    , JSONExtract(_airbyte_data, 'logfile', 'String') as `logfile`

    , toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String'))) as `created`
    , toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'time_started', 'String'))) as `time_started`
    , toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'time_done', 'String'))) as `time_done`

FROM postgres._airbyte_raw_tasks
;


select count(*) from planoplan_postgres.render_tasks ;
select count(*) from postgres._airbyte_raw_tasks ;

SELECT * FROM planoplan_postgres.render_tasks ;
select * from postgres._airbyte_raw_tasks limit 200 ;
