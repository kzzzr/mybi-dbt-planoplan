-- DROP TABLE planoplan_postgres.planoplan_users ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_users
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'email', 'String') as `email`
	, JSONExtract(_airbyte_data, 'password', 'String') as `password`
	, JSONExtract(_airbyte_data, 'name', 'String') as `name`
	, JSONExtract(_airbyte_data, 'type', 'UInt32') as `type`
	, JSONExtract(_airbyte_data, 'status', 'UInt32') as `status`
	, JSONExtract(_airbyte_data, 'unity_settings', 'String') as `unity_settings`
	, JSONExtract(_airbyte_data, 'source_of_signup', 'String') as `source_of_signup`
	, JSONExtract(_airbyte_data, 'foreign_id', 'String') as `foreign_id`
	, JSONExtract(_airbyte_data, 'social_info', 'String') as `social_info`
	, JSONExtract(_airbyte_data, 'project_purchased', 'UInt32') as `project_purchased`
	, JSONExtract(_airbyte_data, 'render_purchased', 'UInt32') as `render_purchased`
	, JSONExtract(_airbyte_data, 'texture_purchased', 'UInt32') as `texture_purchased`
	, JSONExtract(_airbyte_data, 'lang', 'String') as `lang`
	, JSONExtract(_airbyte_data, 'settings_notification', 'String') as `settings_notification`
	, JSONExtract(_airbyte_data, 'email_change', 'String') as `email_change`
	, JSONExtract(_airbyte_data, 'balance', 'UInt32') as `balance`
	, JSONExtract(_airbyte_data, 'render_priority', 'UInt32') as `render_priority`
	, JSONExtract(_airbyte_data, 'notifications_off', 'String') as `notifications_off`
	, JSONExtract(_airbyte_data, 'libraries', 'String') as `libraries`
	, JSONExtract(_airbyte_data, 'vkontakte_id', 'String') as `vkontakte_id`
	, JSONExtract(_airbyte_data, 'facebook_id', 'String') as `facebook_id`
	, JSONExtract(_airbyte_data, 'vkontakte_social_info', 'String') as `vkontakte_social_info`
	, JSONExtract(_airbyte_data, 'facebook_social_info', 'String') as `facebook_social_info`
	, JSONExtract(_airbyte_data, 'google_id', 'String') as `google_id`
	, JSONExtract(_airbyte_data, 'google_social_info', 'String') as `google_social_info`
	, JSONExtract(_airbyte_data, 'manage_clients_id', 'UInt32') as `manage_clients_id`
	, JSONExtract(_airbyte_data, 'manage_libraries', 'String') as `manage_libraries`
	, JSONExtract(_airbyte_data, 'translator_languages', 'String') as `translator_languages`
	, JSONExtract(_airbyte_data, 'catalog_access_keys', 'String') as `catalog_access_keys`
	, JSONExtract(_airbyte_data, 'acl', 'String') as `acl`
	, JSONExtract(_airbyte_data, 'api_public_key', 'String') as `api_public_key`
	, JSONExtract(_airbyte_data, 'api_private_key', 'String') as `api_private_key`
	, JSONExtract(_airbyte_data, 'clients_id', 'UInt32') as `clients_id`
	, JSONExtract(_airbyte_data, 'account', 'String') as `account`
	, JSONExtract(_airbyte_data, 'bonuses', 'UInt32') as `bonuses`
	, JSONExtract(_airbyte_data, 'pool', 'String') as `pool`
	, JSONExtract(_airbyte_data, 'deleted', 'String') as `deleted`
	, JSONExtract(_airbyte_data, 'restore_data', 'String') as `restore_data`
	, JSONExtract(_airbyte_data, 'widget_version', 'UInt32') as `widget_version`
	, JSONExtract(_airbyte_data, 'tariffs_id', 'UInt32') as `tariffs_id`
	, JSONExtract(_airbyte_data, 'textures', 'String') as `textures`
	, JSONExtract(_airbyte_data, 'lot_groups', 'String') as `lot_groups`
	, JSONExtract(_airbyte_data, 'about', 'String') as `about`
	, JSONExtract(_airbyte_data, 'avatar', 'String') as `avatar`
	, JSONExtract(_airbyte_data, 'disk_space', 'UInt32') as `disk_space`
	, JSONExtract(_airbyte_data, 'account_backup', 'String') as `account_backup`
	, JSONExtract(_airbyte_data, 'models', 'String') as `models`
	, JSONExtract(_airbyte_data, 'catalog_disk_usage', 'UInt32') as `catalog_disk_usage`
	, JSONExtract(_airbyte_data, 'hide_client_libraries', 'String') as `hide_client_libraries`
	, JSONExtract(_airbyte_data, 'stat_cid_ga', 'String') as `stat_cid_ga`
	, JSONExtract(_airbyte_data, 'stat_cid_ym', 'String') as `stat_cid_ym`
	, JSONExtract(_airbyte_data, 'autopayment', 'String') as `autopayment`
	, JSONExtract(_airbyte_data, 'tariff_log', 'String') as `tariff_log`
	, JSONExtract(_airbyte_data, 'disk_space_log', 'String') as `disk_space_log`
	, JSONExtract(_airbyte_data, 'currency', 'String') as `currency`
	, JSONExtract(_airbyte_data, 'designs', 'String') as `designs`
	, JSONExtract(_airbyte_data, 'last_ip', 'String') as `last_ip`
	, JSONExtract(_airbyte_data, 'spending_rub', 'Float32') as `spending_rub`
	, JSONExtract(_airbyte_data, 'disk_usage', 'UInt32') as `disk_usage`
	, JSONExtract(_airbyte_data, 'stripe_customer', 'String') as `stripe_customer`
	, JSONExtract(_airbyte_data, 'survey', 'String') as `survey`
	, JSONExtract(_airbyte_data, 'logo', 'String') as `logo`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String'))) as `created`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'modified', 'String'))) as `modified`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'last_login', 'String'))) as `last_login`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'tariff_expire', 'String'))) as `tariff_expire`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'clear_session_before', 'String'))) as `clear_session_before`

FROM postgres._airbyte_raw_users
;


select count(*) from planoplan_postgres.planoplan_users ;
select count(*) from postgres._airbyte_raw_users ;

SELECT * FROM planoplan_postgres.planoplan_users ;
select * from postgres._airbyte_raw_users limit 200 ;
