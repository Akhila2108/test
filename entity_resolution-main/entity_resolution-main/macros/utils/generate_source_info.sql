{% macro get_source_info(source, filter_column='null', filter_value='null') -%}

    {% set site_array %}

        SELECT

            *

        FROM  {{ ref(source) }}

        {% if filter_column != 'null' %}
        WHERE {{ filter_column }} = '{{ filter_value }}'
        {% endif %}

    {% endset %}

    {% set results = run_query(site_array ) %}

    {% if execute %}
        {% set results_list = results %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}

{% endmacro %}