{% macro initialize_dwh() -%}

    {% set commands = [
          (init_sources, 'INITIALIZE DATA SOURCES')
    ] %}

    {% for command in commands %}
        {{ log('START - ' ~ command[1], info='True') }}
        {{ command[0]() }}
        {{ log('FINISH - ' ~ command[1], info='True') }}
    {% endfor %}
   
{%- endmacro %}


