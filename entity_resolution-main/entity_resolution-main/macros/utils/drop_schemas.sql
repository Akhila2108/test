{% macro drop_schemas() -%}

    {%- set schemas_to_drop = ['AUDIT', 'CORE', 'GRAPH', 'MATCH', 'PREP', 'SURVIVOURSHIP', 'REFERENCE'] -%}

    {% for s in schemas_to_drop %}
        DROP SCHEMA IF EXISTS {{ s }};
    {% endfor %}

{%- endmacro %}