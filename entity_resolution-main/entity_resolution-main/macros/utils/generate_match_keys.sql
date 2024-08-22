{% macro get_entity_match_key(field_filter='NULL') %}

    {% set key_array %}

        SELECT
            DISTINCT
            {% if field_filter == 'NULL' %}
                column_name           AS column_name,
            {% endif %}
            
            match_weighting       AS match_weighting,
            match_diff_max        AS match_diff_max,
            match_value           AS match_type,
            match_method          AS match_method

        FROM {{ ref(var('meta_entity_match_key_values')) }}
        WHERE
            lower(match_type) = 'complex_match'

    {% endset %}

    {% set results = run_query(key_array) %}

    {% if execute %} --  ensure that the code runs during the parse stage of dbt

        {% set results_list = results %}

    {% else %}

        {% set results_list = [] %}

    {% endif %}

    {{ return(results_list) }}

{% endmacro %}