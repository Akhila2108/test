{% macro get_master_source_batch(match_type) -%}

{%  set quick_match_columns = get_source_info(var('meta_entity_match_key_values'), 'MATCH_TYPE', 'quick_match') %}

master_index AS (

    SELECT

        -- @TODO - Need to understand why we minus (-) for quick_match
        {% if match_type == 'quick_match' %}
        -
        {% endif %}
        ROW_NUMBER() OVER(PARTITION BY entity_hash_id ORDER BY entity_hash_id)   AS group_id    -- Generate group_id for each rows.

        , *

    FROM {{ ref('prep_entity') }} p

)

-- Retrive stage_source_records
, source_records_batch AS (
    
    SELECT 
        
        s.*

    FROM {{ ref('source_entity') }} s

    {% if match_type == 'complex_match' %}
        LEFT JOIN {{ ref('quick_match') }} m
        ON 
            s.entity_hash_id = m.entity_hash_id

        WHERE
            m.entity_hash_id IS NULL
    {% endif %}

)

{% endmacro %}

{% macro get_source_priority_case_block() -%}

{%  set source_priority     = get_source_info(var('meta_source_entity')) %}

{% for c in source_priority.rows %}
WHEN '{{ c['RECORD_SOURCE'] }}' THEN {{ c['SOURCE_PRIORITY'] }}
{% endfor %}
ELSE {{ source_priority | length + 1 }}

{% endmacro %}

{% macro read_entity_match_type_value(source_cte) %}

, read_{{ source_cte }} AS (

    SELECT

        m.entity_hash_id 
        , mn.match_type
        , mn.match_value
        , mn.match_key

    FROM {{ source_cte }} m

    INNER JOIN {{ ref('entity_match_node') }} mn 
    ON
        m.entity_hash_id = mn.entity_hash_id

)

{% endmacro %}