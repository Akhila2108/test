{{ 
    config(
        tags=["audit", "init_audit"],
        materialized="incremental"
) }}

{% set match_keys = get_entity_match_key('Remove_unwanted_filter') %}

-- Creates empty table required for dependencies to operate properly
-- Populated later in the process by macro: update_complex_match_audit

SELECT

    CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))                                 AS source_entity_hash_id
    , CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))                               AS master_entity_hash_id
    
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS source_record_source
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS source_table_source
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS master_record_source
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS master_table_source
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS source_entity_natural_key
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS master_entity_natural_key

    {% if var('data_loaded_at') != 'null' %}
        , TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())                           AS source_source_loaded_at
    {% endif %}
    
    {% for i in match_keys.rows %}
    {% if i['MATCH_METHOD'] != 'ALGORITHM' and i['MATCH_METHOD'] != 'algorithm' %}
    , '{{ var('default_entity_group_id') }}'                        AS is_{{ i['MATCH_TYPE'] | lower }}_match
    {% endif %}
    {% endfor %}

    {% for i in match_keys.rows %}
    {% if i['MATCH_METHOD'] == 'ALGORITHM' or i['MATCH_METHOD'] == 'algorithm' %}

    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS source_{{ i['MATCH_TYPE'] | lower }}_match_key
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS master_{{ i['MATCH_TYPE'] | lower }}_match_key
    
    , '{{ var('default_entity_group_id') }}'                         AS {{ i['MATCH_TYPE'] | lower }}_distance
    , '{{ var('default_entity_group_id') }}'                         AS {{ i['MATCH_TYPE'] | lower }}_length
    , '{{ var('default_entity_group_id') }}'                         AS {{ i['MATCH_TYPE'] | lower }}_distance_pc

    , '{{ var('default_entity_group_id') }}'                         AS {{ i['MATCH_TYPE'] | lower }}_match_score

    {% endif %}
    {% endfor %}

    {% for i in match_keys.rows -%}
    {% if i['MATCH_METHOD'] != 'ALGORITHM' and i['MATCH_METHOD'] != 'algorithm' %}
    , '{{ var('default_entity_group_id') }}'                         AS {{ i['MATCH_TYPE'] | lower }}_match_score
    {% endif %}
    {% endfor %}

    , '{{ var('default_entity_group_id') }}'                         AS total_match_score
     
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})        AS merge_decision
         
    , '{{ var('default_entity_group_id') }}'                         AS master_record_source_priority
    , TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())                           AS match_timestamp

FROM {{ ref('prep_entity') }}

WHERE 
    1 = 0