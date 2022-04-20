-- DROP TABLE planoplan_postgres.planoplan_events ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_events
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'users_id', 'UInt32') as `users_id`
	, JSONExtract(_airbyte_data, 'type', 'UInt32') as `type`
	, JSONExtract(_airbyte_data, 'data', 'String') as `data`
	, JSONExtract(_airbyte_data, 'ip', 'String') as `ip`
	, JSONExtract(_airbyte_data, 'user_agent', 'String') as `user_agent`
	, JSONExtract(_airbyte_data, 'teams_id', 'UInt32') as `teams_id`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String'))) as `created`

FROM postgres._airbyte_raw_events
;

SELECT * FROM planoplan_postgres.planoplan_events ;
select count(*) from planoplan_postgres.planoplan_events ;
select * from postgres._airbyte_raw_clients limit 200 ;
select count(*) from postgres._airbyte_raw_events ;