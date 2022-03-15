-- DROP TABLE planoplan_postgres.planoplan_promocodes ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_promocodes
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	 JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'alias', 'String') as `alias`
	, JSONExtract(_airbyte_data, 'type', 'String') as `type`
	, JSONExtract(_airbyte_data, 'discount', 'UInt32') as `discount`
	, JSONExtract(_airbyte_data, 'goods', 'String') as `goods`
	, JSONExtract(_airbyte_data, 'archive', 'String') as `archive`
	, JSONExtract(_airbyte_data, 'limitations_email', 'String') as `limitations_email`

	, parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'active_from', 'String')) as `active_from`
	, parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'active_to', 'String')) as `active_to`

FROM postgres._airbyte_raw_promocodes
;


select count(*) from planoplan_postgres.planoplan_promocodes ;
select count(*) from postgres._airbyte_raw_payments ;

SELECT * FROM planoplan_postgres.planoplan_promocodes ;
select * from postgres._airbyte_raw_promocodes limit 200 ;
