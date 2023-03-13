{% macro init_sources() -%}

    {% if target.name in ['prod'] %}

        {{ init_source_pg_planoplan() }}
        
    {% elif target.name in ['dev', 'ci'] %}
    {% endif %}

{%- endmacro %}


{% macro init_source_pg_planoplan() -%}

    {% set sql %}
        DROP DATABASE IF EXISTS {{ var('pg_planoplan') }} ;
    {% endset %}
    {% set result = run_query(sql) %}
    
    {% set sql %}
        CREATE DATABASE IF NOT EXISTS {{ var('pg_planoplan') }}
        ENGINE = PostgreSQL(
              '{{ var("pg_planoplan_host") }}'
            , '{{ var("pg_planoplan_database") }}'
            , '{{ env_var("PG_PLANOPLAN_USER") }}'
            , '{{ env_var("PG_PLANOPLAN_PASSWORD") }}'
        );
    {% endset %}
    {% set result = run_query(sql) %}

    {{ log('Initialized source database – ' + var('pg_planoplan'), info='True') }}

{%- endmacro %}