-- DROP TABLE planoplan_postgres.planoplan_tariff_stat ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_tariff_stat
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'clients_id', 'UInt32') as `clients_id`
	, JSONExtract(_airbyte_data, 'tariffs_id', 'UInt32') as `tariffs_id`
	, JSONExtract(_airbyte_data, 'cnt', 'UInt32') as `cnt`
	
	, toString(parseDateTimeBestEffortOrZero(JSONExtract(_airbyte_data, 'created', 'String'))) as `created`

FROM postgres._airbyte_raw_tariffs_stat
;


select count(*) from planoplan_postgres.planoplan_tariff_stat ;
select count(*) from postgres._airbyte_raw_tariffs_stat ;

SELECT * FROM planoplan_postgres.planoplan_tariff_stat ;
select * from postgres._airbyte_raw_tariffs_stat limit 200 ;
