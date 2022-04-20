-- DROP TABLE planoplan_postgres.planoplan_store_goods ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_store_goods
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

	  JSONExtract(_airbyte_data, 'id', 'UInt32') as `id`
	, JSONExtract(_airbyte_data, 'alias', 'String') as `alias`
	, JSONExtract(_airbyte_data, 'caption', 'String') as `caption`
	, JSONExtract(_airbyte_data, 'params', 'String') as `params`
	, JSONExtract(_airbyte_data, 'store_groups_id', 'UInt32') as `store_groups_id`
	, JSONExtract(_airbyte_data, 'archive', 'String') as `archive`
	, JSONExtract(_airbyte_data, 'type', 'String') as `type`
	, JSONExtract(_airbyte_data, 'price', 'Float32') as `price`
	, JSONExtract(_airbyte_data, 'price_bonus', 'UInt32') as `price_bonus`
	, JSONExtract(_airbyte_data, 'duration', 'UInt32') as `duration`
	, JSONExtract(_airbyte_data, 'duration_unit', 'String') as `duration_unit`
	, JSONExtract(_airbyte_data, 'is_deposit', 'String') as `is_deposit`
	, JSONExtract(_airbyte_data, 'allow_use_in_sets', 'String') as `allow_use_in_sets`
	, JSONExtract(_airbyte_data, 'allow_pro_influence', 'String') as `allow_pro_influence`
	, JSONExtract(_airbyte_data, 'short_name', 'String') as `short_name`
	, JSONExtract(_airbyte_data, 'description', 'String') as `description`
	, JSONExtract(_airbyte_data, 'not_for_sale', 'String') as `not_for_sale`
	, JSONExtract(_airbyte_data, 'unlimited', 'String') as `unlimited`
	, JSONExtract(_airbyte_data, 'sort', 'UInt32') as `sort`
	, JSONExtract(_airbyte_data, 'descriptor', 'String') as `descriptor`
	, JSONExtract(_airbyte_data, 'description_site', 'String') as `description_site`
	, JSONExtract(_airbyte_data, 'link', 'String') as `link`
	, JSONExtract(_airbyte_data, 'img_preview', 'String') as `img_preview`
	, JSONExtract(_airbyte_data, 'img_description', 'String') as `img_description`
	, JSONExtract(_airbyte_data, 'link_title', 'String') as `link_title`

FROM postgres._airbyte_raw_store_goods
;


select count(*) from planoplan_postgres.planoplan_store_goods ;
select count(*) from postgres._airbyte_raw_store_goods ;

SELECT * FROM planoplan_postgres.planoplan_store_goods ;
select * from postgres._airbyte_raw_store_goods limit 200 ;
