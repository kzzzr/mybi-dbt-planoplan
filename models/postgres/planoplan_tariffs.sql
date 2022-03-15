-- DROP TABLE planoplan_postgres.planoplan_tariffs ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_tariffs
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'name', 'String') as `name`
	, JSONExtract(_airbyte_data, 'variables', 'String') as `variables`
	, JSONExtract(_airbyte_data, 'archive', 'String') as `archive`
	, JSONExtract(_airbyte_data, 'is_default', 'String') as `is_default`
	, JSONExtract(_airbyte_data, 'prices', 'String') as `prices`
	, JSONExtract(_airbyte_data, 'alias', 'String') as `alias`
	, JSONExtract(_airbyte_data, 'hidden', 'String') as `hidden`
	, JSONExtract(_airbyte_data, 'catalog_access_keys', 'String') as `catalog_access_keys`

FROM postgres._airbyte_raw_tariffs
;


select count(*) from planoplan_postgres.planoplan_tariffs ;
select count(*) from postgres._airbyte_raw_tariffs ;

SELECT * FROM planoplan_postgres.planoplan_tariffs ;
select * from postgres._airbyte_raw_tariffs limit 200 ;
