{{
    config (
        materialized='view'
    )
}}

SELECT

      id
	, parseDateTimeBestEffortOrZero(created) as created
	, parseDateTimeBestEffortOrZero(modified) as modified
	, projects_id
	, render_servers_id
	, parseDateTimeBestEffortOrZero(time_started) as time_started
	, parseDateTimeBestEffortOrZero(time_done) as time_done
	, `data`
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

FROM {{ source('planoplan', 'render_tasks') }} FINAL
