-- DROP TABLE planoplan_postgres.planoplan_teams ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_teams
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'clients_id', 'UInt32') as `clients_id`
	, JSONExtract(_airbyte_data, 'users_id', 'UInt32') as `users_id`
	, JSONExtract(_airbyte_data, 'license_count', 'UInt32') as `license_count`
	, JSONExtract(_airbyte_data, 'members', 'String') as `members`
	, JSONExtract(_airbyte_data, 'tariffs_id', 'UInt32') as `tariffs_id`
	, JSONExtract(_airbyte_data, 'caption', 'String') as `caption`
	, JSONExtract(_airbyte_data, 'account', 'String') as `account`
	, JSONExtract(_airbyte_data, 'block_members', 'String') as `block_members`
	, JSONExtract(_airbyte_data, 'email', 'String') as `email`
	, JSONExtract(_airbyte_data, 'site', 'String') as `site`
	, JSONExtract(_airbyte_data, 'about', 'String') as `about`
	, JSONExtract(_airbyte_data, 'social_links', 'String') as `social_links`
	, JSONExtract(_airbyte_data, 'logo', 'String') as `logo`
	, JSONExtract(_airbyte_data, 'disk_space', 'UInt32') as `disk_space`
	, JSONExtract(_airbyte_data, 'libraries', 'String') as `libraries`
	, JSONExtract(_airbyte_data, 'textures', 'String') as `textures`
	, JSONExtract(_airbyte_data, 'lot_groups', 'String') as `lot_groups`
	, JSONExtract(_airbyte_data, 'models', 'String') as `models`
	, JSONExtract(_airbyte_data, 'catalog_disk_usage', 'UInt32') as `catalog_disk_usage`
	, JSONExtract(_airbyte_data, 'api_key', 'String') as `api_key`
	, JSONExtract(_airbyte_data, 'api_secret', 'String') as `api_secret`
	, JSONExtract(_airbyte_data, 'autopayment', 'String') as `autopayment`
	, JSONExtract(_airbyte_data, 'tariff_log', 'String') as `tariff_log`
	, JSONExtract(_airbyte_data, 'disk_space_log', 'String') as `disk_space_log`
	, JSONExtract(_airbyte_data, 'currency', 'String') as `currency`
	, JSONExtract(_airbyte_data, 'unity_settings', 'String') as `unity_settings`
	, JSONExtract(_airbyte_data, 'max_api_requests_per_month', 'UInt32') as `max_api_requests_per_month`
	, JSONExtract(_airbyte_data, 'max_api_requests_per_period', 'UInt32') as `max_api_requests_per_period`
	, JSONExtract(_airbyte_data, 'designs', 'String') as `designs`
	, JSONExtract(_airbyte_data, 'admin_members', 'String') as `admin_members`
	, JSONExtract(_airbyte_data, 'render_priority', 'UInt32') as `render_priority`
	, JSONExtract(_airbyte_data, 'disk_usage', 'UInt32') as `disk_usage`
	, JSONExtract(_airbyte_data, 'stripe_customer', 'String') as `stripe_customer`
	, JSONExtract(_airbyte_data, 'catalog_access_keys', 'String') as `catalog_access_keys`
	
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'license_expire', 'String'))) as `license_expire`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String'))) as `created`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'modified', 'String'))) as `modified`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'deleted_at', 'String'))) as `deleted_at`

FROM postgres._airbyte_raw_teams
;


select count(*) from planoplan_postgres.planoplan_teams ;
select count(*) from postgres._airbyte_raw_teams ;

SELECT * FROM planoplan_postgres.planoplan_teams ;
select * from postgres._airbyte_raw_teams limit 200 ;
