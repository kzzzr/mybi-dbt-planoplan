-- DROP TABLE planoplan_postgres.planoplan_payments ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_payments
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'users_id', 'UInt32') as `users_id`
	, JSONExtract(_airbyte_data, 'client_id', 'UInt32') as `client_id`
	, JSONExtract(_airbyte_data, 'price', 'Float32') as `price`
	, JSONExtract(_airbyte_data, 'currency', 'String') as `currency`
	, JSONExtract(_airbyte_data, 'type', 'UInt32') as `type`
	, JSONExtract(_airbyte_data, 'amount', 'UInt32') as `amount`
	, JSONExtract(_airbyte_data, 'mode_old', 'UInt32') as `mode_old`
	, JSONExtract(_airbyte_data, 'ps_id', 'String') as `ps_id`
	, JSONExtract(_airbyte_data, 'paid', 'String') as `paid`
	, JSONExtract(_airbyte_data, 'paid_error', 'String') as `paid_error`
	, JSONExtract(_airbyte_data, 'reported_error', 'String') as `reported_error`
	, JSONExtract(_airbyte_data, 'type_area', 'String') as `type_area`
	, JSONExtract(_airbyte_data, 'price_internal', 'Float32') as `price_internal`
	, JSONExtract(_airbyte_data, 'order_id', 'String') as `order_id`
	, JSONExtract(_airbyte_data, 'stat_id', 'String') as `stat_id`
	, JSONExtract(_airbyte_data, 'mode', 'String') as `mode`
	, JSONExtract(_airbyte_data, 'price_rub', 'Float32') as `price_rub`
	, JSONExtract(_airbyte_data, 'teams_id', 'UInt32') as `teams_id`
	, JSONExtract(_airbyte_data, 'params', 'String') as `params`
	, JSONExtract(_airbyte_data, 'promocode', 'String') as `promocode`
	, JSONExtract(_airbyte_data, 'autopayment', 'String') as `autopayment`
	, JSONExtract(_airbyte_data, 'binding_id', 'UInt32') as `binding_id`
	, JSONExtract(_airbyte_data, 'vat', 'String') as `vat`
	, JSONExtract(_airbyte_data, 'country', 'String') as `country`

	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String'), 'UTC')) as `created`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'modified', 'String'), 'UTC')) as `modified`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'paid_date', 'String'), 'UTC')) as `paid_date`
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'deleted_at', 'String'), 'UTC')) as `deleted_at`

FROM postgres._airbyte_raw_payments
;


select count(*) from planoplan_postgres.planoplan_payments ;
select count(*) from postgres._airbyte_raw_payments ;

SELECT * FROM planoplan_postgres.planoplan_payments ;
select * from postgres._airbyte_raw_payments limit 200 ;

