-- DROP TABLE planoplan_postgres.planoplan_clients ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_clients
ENGINE = ReplacingMergeTree(modified)
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String')) as created
	, parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'modified', 'String')) as modified
	, JSONExtract(_airbyte_data, 'name', 'String') as `name`
	, JSONExtract(_airbyte_data, 'alias', 'String') as `alias`
	, JSONExtract(_airbyte_data, 'active', 'String') as `active`
	, JSONExtract(_airbyte_data, 'free_project_count', 'UInt32') as `free_project_count`
	, JSONExtract(_airbyte_data, 'render_count', 'UInt32') as `render_count`
	, JSONExtract(_airbyte_data, 'free_render_count', 'UInt32') as `free_render_count`
	, JSONExtract(_airbyte_data, 'render_priority', 'UInt32') as `render_priority`
	, JSONExtract(_airbyte_data, 'use_in_main_db', 'String') as `use_in_main_db`
	, JSONExtract(_airbyte_data, 'allow_use_main_db', 'String') as `allow_use_main_db`
	, JSONExtract(_airbyte_data, 'use_personal_layout', 'String') as `use_personal_layout`
	, JSONExtract(_airbyte_data, 'free_texture_count', 'UInt32') as `free_texture_count`
	, JSONExtract(_airbyte_data, 'default_language', 'String') as `default_language`
	, JSONExtract(_airbyte_data, 'mail_subject_prefix', 'String') as `mail_subject_prefix`
	, JSONExtract(_airbyte_data, 'host', 'String') as `host`
	, JSONExtract(_airbyte_data, 'redirect_uri', 'String') as `redirect_uri`
	, JSONExtract(_airbyte_data, 'libraries', 'String') as `libraries`
	, JSONExtract(_airbyte_data, 'notifications', 'String') as `notifications`
	, JSONExtract(_airbyte_data, 'config', 'String') as `config`
	, JSONExtract(_airbyte_data, 'variables', 'String') as `variables`
	, JSONExtract(_airbyte_data, 'balance', 'UInt32') as `balance`
	, JSONExtract(_airbyte_data, 'catalog_access_keys', 'String') as `catalog_access_keys`
	, JSONExtract(_airbyte_data, 'account', 'String') as `account`
	, JSONExtract(_airbyte_data, 'secret', 'String') as `secret`
	, JSONExtract(_airbyte_data, 'task_callback_status_url', 'String') as `task_callback_status_url`
	, JSONExtract(_airbyte_data, 'tariffs_id', 'UInt32') as `tariffs_id`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'tariff_expire', 'String'))) as tariff_expire
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'deleted_at', 'String'))) as deleted_at
	, JSONExtract(_airbyte_data, 'textures', 'String') as `textures`
	, JSONExtract(_airbyte_data, 'lot_groups', 'String') as `lot_groups`
	, JSONExtract(_airbyte_data, 'disk_space', 'UInt32') as `disk_space`
	, JSONExtract(_airbyte_data, 'models', 'String') as `models`
	, JSONExtract(_airbyte_data, 'catalog_disk_usage', 'UInt32') as `catalog_disk_usage`
	, JSONExtract(_airbyte_data, 'currency', 'String') as `currency`
	, JSONExtract(_airbyte_data, 'max_api_requests_per_month', 'UInt32') as `max_api_requests_per_month`
	, JSONExtract(_airbyte_data, 'max_api_requests_per_period', 'UInt32') as `max_api_requests_per_period`
	, JSONExtract(_airbyte_data, 'designs', 'String') as `designs`
	, JSONExtract(_airbyte_data, 'disk_usage', 'UInt32') as `disk_usage`

FROM postgres._airbyte_raw_clients
;

SELECT * FROM planoplan_postgres.planoplan_clients ;
select count(*) from planoplan_postgres.planoplan_clients ;
select * from postgres._airbyte_raw_clients limit 200 ;
select count(*) from postgres._airbyte_raw_clients ;
