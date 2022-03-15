-- DROP TABLE planoplan_postgres.planoplan_store_sets ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_store_sets
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'discount', 'Float32') as `discount`
	, JSONExtract(_airbyte_data, 'goods', 'String') as `goods`
	, JSONExtract(_airbyte_data, 'sort', 'UInt32') as `sort`
	, JSONExtract(_airbyte_data, 'caption', 'String') as `caption`
	, JSONExtract(_airbyte_data, 'allow_pro_influence_for_goods', 'String') as `allow_pro_influence_for_goods`
	, JSONExtract(_airbyte_data, 'description_site', 'String') as `description_site`
	, JSONExtract(_airbyte_data, 'descriptor', 'String') as `descriptor`
	, JSONExtract(_airbyte_data, 'link_title', 'String') as `link_title`
	, JSONExtract(_airbyte_data, 'link', 'String') as `link`
	, JSONExtract(_airbyte_data, 'img_preview', 'String') as `img_preview`
	, JSONExtract(_airbyte_data, 'img_description', 'String') as `img_description`

FROM postgres._airbyte_raw_store_sets
;


select count(*) from planoplan_postgres.planoplan_store_sets ;
select count(*) from postgres._airbyte_raw_store_sets ;

SELECT * FROM planoplan_postgres.planoplan_store_sets ;
select * from postgres._airbyte_raw_store_sets limit 200 ;
