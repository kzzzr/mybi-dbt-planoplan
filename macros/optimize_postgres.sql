{% macro optimize_postgres() %}

        {% set tables = [

              'planoplan_postgres.planoplan_clients'
            , 'planoplan_postgres.planoplan_events'
            , 'planoplan_postgres.planoplan_promocodes'
            , 'planoplan_postgres.planoplan_render_tasks'
            , 'planoplan_postgres.planoplan_store_goods'
            , 'planoplan_postgres.planoplan_store_sets'
            , 'planoplan_postgres.planoplan_tariffs'
            , 'planoplan_postgres.planoplan_tariff_stat'
            , 'planoplan_postgres.planoplan_teams'
            , 'planoplan_postgres.planoplan_users'
            , 'planoplan_postgres.render_tasks'
            
        ] %}    

        {% for table in tables %}

            {{ log('OPTIMIZE TABLE ' + table + ' FINAL DEDUPLICATE', info=True) }}
            {% do run_query('OPTIMIZE TABLE ' + table + ' FINAL DEDUPLICATE') %}            

        {% endfor %}

{% endmacro %}
