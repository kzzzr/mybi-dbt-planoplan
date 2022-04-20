-- DROP TABLE planoplan_postgres.planoplan_render_tasks ;
-- TRUNCATE TABLE planoplan_postgres.planoplan_render_tasks ;

CREATE MATERIALIZED VIEW IF NOT EXISTS planoplan_postgres.planoplan_render_tasks
ENGINE = ReplacingMergeTree()
ORDER BY id
PRIMARY KEY id
POPULATE AS
SELECT

      id

	, toString(parseDateTimeBestEffortOrZero(created)) as created
	, toString(parseDateTimeBestEffortOrZero(modified)) as modified
	, toString(parseDateTimeBestEffortOrZero(time_started)) as time_started
	, toString(parseDateTimeBestEffortOrZero(time_done)) as time_done

	, projects_id
	, render_servers_id

	--, `data`
	, data_data
	, data_panoCamId
	, data_camInfo
	, data_unityXML
	, data_unityAdditionalXML
	, data_lang

	, status
	, priority
	, attempts
	, image
	, u3d
	, clients_id
	, users_id
	, paid
	, build_time
	, properties
	, reason
	, `action`
	, task_id
	, floor
	, cam_id
	, preview
	, lot_id
	, goods_id
	, callback_status_url
	, teams_id
	, archive
	, tariff_fingerprint
	, u3d_path
	, preview_image
	, tariffs_id
	, stat

FROM postgres.render_tasks 
;

select count(*) from planoplan_postgres.planoplan_render_tasks ;
select count(*) frompostgres.render_tasks ;

SELECT min(created), max(created) FROM planoplan_postgres.planoplan_render_tasks ;

SELECT * FROM planoplan_postgres.planoplan_render_tasks ;
select * from postgres.render_tasks limit 200 ;
