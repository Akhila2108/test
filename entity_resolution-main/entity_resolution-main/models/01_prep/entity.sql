{{
    config(
        tags=["prep"]
    )
}}

{% set sources = get_source_info(var('meta_source_entity')) %}

{% set columns = adapter.get_columns_in_relation(
    ref(var('meta_source_entity'))
) %}

{% set exclude_columns = ['RECORD_SOURCE', 'TABLE_SOURCE', 'SOURCE_PRIORITY'] %}  -- Source & Table to refer from sources.yml

-- Combine the sources from multiple sources
WITH
 
join_sources AS (

    {% for i in sources.rows %}

        SELECT

            {%- for c in exclude_columns if c not in ['SOURCE_PRIORITY'] %}  
                {% if i[c.name] != 'x' -%}          '{{ i[c] }}'            {%- else -%} NULL {% endif -%}::VARCHAR     AS {{ c | lower }} ,
            {%- endfor %}

            {%- for c in columns if c.name not in exclude_columns %}  
                {% if i[c.name] != 'x' -%}          {{ i[c.name] }}        {%- else -%} NULL {%- endif -%}::VARCHAR     AS {{ c.name | lower }}
                {%- if not loop.last %} , {% endif %}
            {%- endfor %}

        FROM {{ source(i['RECORD_SOURCE'].lower(), i['TABLE_SOURCE'].upper()) }}

        {% if not loop.last %} UNION ALL {% endif %}

    {% endfor %}

)

SELECT {{ generate_hash_id('record_source', 'table_source', 'entity_natural_key') }}  AS entity_hash_id, * FROM join_sources 