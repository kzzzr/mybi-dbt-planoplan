# DEVELOPMENT

## Set ENV variables

```bash
export DBT_SOURCE_DATABASE=mybi
export DBT_SOURCE_SCHEMA=mybi
```

# Data Sync

## Performed by Airbyte

- [x] myBI (GAnalytics)

## `planoplan.public.render_tasks` synced via psql and clickhouse-client

- [x] create destination table
- [x] initial load
- [x] shell script for incremental load
- [x] filter fetched rows rows (>= max(id) from initial load)
- [x] schedule incremental load (crontab)
- [x] create dbt model with deduplicated rows (`select from final`)

## Explore

```bash
export PGPASSWORD=''
psql -h db.planoplan.com -p 5432 -U airbyte -d render
psql -h db.planoplan.com -p 5432 -U airbyte -d planoplan

# \COPY (SELECT * FROM public.render_tasks limit 3) TO 'render_tasks_binary' FORMAT binary 
# \COPY (SELECT * FROM public.render_tasks limit 3) TO 'render_tasks_binary' DELIMITER ',' CSV HEADER" > export.csv
# select count(1) from render_tasks;
# \d clients
# pg_dump -h db.planoplan.com -p 5432 -U airbyte -t 'public.tasks' --schema-only render

SELECT pid, age(clock_timestamp(), query_start), usename, query, state
FROM pg_stat_activity 
WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' 
    AND usename ILIKE ('%airbyte%')
ORDER BY query_start desc;

psql --dbname=planoplan --username=airbyte --host=db.planoplan.com \
	-c "COPY (SELECT * FROM public.render_tasks limit 3) TO stdout HEADER" > 'render_tasks_text'

psql --dbname=planoplan --username=airbyte --host=db.planoplan.com \
	-c "COPY (SELECT * FROM public.render_tasks limit 10) TO stdout DELIMITER ',' CSV HEADER" > 'render_tasks_csv'

clickhouse-client --host "" \
                  --secure \
                  --user "airbyte" \
                  --database "postgres" \
                  --port 9440 \
                  --password=""

cat render_tasks_csv | \
clickhouse-client \
--host="" \
--port=9440 \
--user="airbyte" \
--password="" \
--database="postgres" \
--secure \
--query="INSERT INTO postgres.render_tasks FORMAT CSVWithNames"


psql --dbname=planoplan --username=airbyte --host=db.planoplan.com \
-c "COPY (SELECT * FROM public.render_tasks WHERE created >= '2019-01-01' limit 100) TO stdout DELIMITER ',' CSV HEADER" | \
clickhouse-client \
--host="" \
--port=9440 \
--user="airbyte" \
--password="" \
--database="postgres" \
--secure \
--query="INSERT INTO postgres.render_tasks FORMAT CSVWithNames"

# no limit
psql --dbname=planoplan --username=airbyte --host=db.planoplan.com \
-c "COPY (SELECT * FROM public.render_tasks) TO stdout DELIMITER ',' CSV HEADER" | \
clickhouse-client \
--host="" \
--port=9440 \
--user="airbyte" \
--password="" \
--database="postgres" \
--secure \
--max_bytes_before_external_group_by=100000000000 \
--max_memory_usage=200000000000 \
--max_memory_usage_for_all_queries=200000000000 \
--max_memory_usage_for_user=200000000000 \
--max_insert_block_size=100000
--query="INSERT INTO postgres.render_tasks FORMAT CSVWithNames"
```

### Create destination table 

```sql
-- DROP TABLE postgres.render_tasks ;

CREATE TABLE postgres.render_tasks
(
id                    UInt64                     
, created             String
, modified            String
, projects_id         UInt64                     
, render_servers_id   UInt64                     
, time_started        String
, time_done           String

, data                String
, data_data String MATERIALIZED JSONExtract(`data`, 'data', 'String')
, data_panoCamId String MATERIALIZED JSONExtract(`data`, 'panoCamId', 'String')
, data_camInfo String MATERIALIZED JSONExtract(`data`, 'camInfo', 'String')
, data_unityXML String MATERIALIZED JSONExtract(`data`, 'unityXML', 'String')
, data_unityAdditionalXML String MATERIALIZED JSONExtract(`data`, 'unityAdditionalXML', 'String')
, data_lang String MATERIALIZED JSONExtract(`data`, 'lang', 'String')

, status              Int8                    
, priority            Int64                     
, attempts            Int8                    
, image               String
, u3d                 String                       
, clients_id          UInt64                     
, users_id            UInt64                     
, paid                String                     
, build_time          Int64
, properties          String
, reason              String
, action              String
, task_id             UInt64                     
, floor               Int64                     
, cam_id              UInt64                     
, preview             String                     
, lot_id              UInt64                     
, goods_id            UInt64                     
, callback_status_url String
, teams_id            UInt64                     
, archive             String                     
, tariff_fingerprint  String
, u3d_path            String
, preview_image       String
, tariffs_id          UInt64                     
, stat                String                        
) ENGINE = MergeTree()
ORDER BY id
PARTITION BY toYYYYMM(parseDateTimeBestEffortOrZero(created))
PRIMARY KEY id
SETTINGS 
index_granularity=100000
;

select * from postgres.render_tasks limit 500 ;

select * from postgres.render_tasks FINAL LIMIT 500 ;

select count(*) from postgres.render_tasks;
```

### Initial load + Incremental load script

```bash
#!/bin/bash

get_source_cursor() {
    psql \
        --dbname=planoplan \
        --username=airbyte \
        --host=db.planoplan.com \
        -qtA \
        -c "SELECT max(id) FROM public.render_tasks" \
        | cat
}

get_target_cursor() {
    clickhouse-client \
        --host="" \
        --port=9440 \
        --user="airbyte" \
        --password="" \
        --database="postgres" \
        --secure \
        --query="SELECT coalesce(nullif(max(id), 0), 1833569) FROM postgres.render_tasks" \
        | cat
}

load_rows() {
    echo "$(date '+%Y-%m-%d %T') BEGIN loading rows CURSOR between $1 and $2"

    psql \
        --dbname=planoplan \
        --username=airbyte \
        --host=db.planoplan.com \
        -c "COPY (SELECT * FROM public.render_tasks WHERE id between $1 and $2) TO stdout DELIMITER ',' CSV HEADER" \
    | clickhouse-client \
        --host="" \
        --port=9440 \
        --user="airbyte" \
        --password="" \
        --database="postgres" \
        --secure \
        --max_memory_usage=16000000000 \
        --max_bytes_before_external_group_by=16000000000 \
        --max_bytes_before_external_sort=16000000000 \
        --max_memory_usage_for_all_queries=16000000000 \
        --max_memory_usage_for_user=16000000000 \
        --max_insert_block_size=1000 \
        --input_format_parallel_parsing=0 \
        --query="INSERT INTO postgres.render_tasks FORMAT CSVWithNames"
    echo "$(date '+%Y-%m-%d %T') FINISH loading rows CURSOR between $1 and $2"
}

export PGPASSWORD=""
STEP=1000

echo "$(date '+%Y-%m-%d %T') GET cursor value from SOURCE"

CURSOR_SOURCE="$(get_source_cursor)"
printf "CURSOR_SOURCE=$CURSOR_SOURCE\n"

echo "$(date '+%Y-%m-%d %T') GET cursor value from TARGET"

CURSOR_TARGET="$(get_target_cursor)"
printf "CURSOR_TARGET=$CURSOR_TARGET\n"

# echo "$(date '+%Y-%m-%d %T') FETCHED to CSV file"

for (( i=CURSOR_TARGET ; i<=CURSOR_SOURCE ; i+=STEP ))
  do 
     load_rows $i $((i+STEP))
 done

echo -e "$(date '+%Y-%m-%d %T') FINISHED LOADING TO CLICKHOUSE \n"
```

### Put shell script on a schedule

```bash
# crontab
sudo nano /etc/crontab

# execute script
cd ~/render_tasks/ && bash render_tasks.sh 2>&1 | tee -a log

# cron entry
0 7 * * * airbyte cd ~/render_tasks/ && bash render_tasks.sh 2>&1 | tee -a log
```