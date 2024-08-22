{{ 
    config(
        tags=["match", "complex_match"],
        materialized="table",
    ) 
}}

{% set pairs        = ['master', 'source'] %}  -- @TODO - use this pairs for dynamic columns
{% set match_keys   = get_entity_match_key() %}
{% set temp_match_keys   = get_entity_match_key('Remove_unwanted_filter') %}

WITH

{{ get_master_source_batch('complex_match') }}

{{ read_entity_match_type_value('master_index') }}

{{ read_entity_match_type_value('source_records_batch') }}

, get_all_match_combinations_existing_index AS (

    SELECT 

        DISTINCT
        sou.entity_hash_id                                                         AS source_entity_hash_id
        , mas.entity_hash_id                                                       AS master_entity_hash_id
        , mas.match_type
        , mas.match_value

    FROM read_source_records_batch sou

    INNER JOIN read_master_index mas
    ON
        sou.entity_hash_id != mas.entity_hash_id  -- Avoid same records join
        -- AND sou.match_key = mas.match_key -- Join match nodes together

)

-- Indentify the records which are matches between source and master using match type
, pivot_matches_existing_index AS (

    SELECT

        source_entity_hash_id
        , master_entity_hash_id

        {% for i in temp_match_keys.rows %}
        , SUM(CASE WHEN match_type = '{{ i['MATCH_TYPE'] }}' THEN 1 ELSE 0 END)         AS is_{{ i['MATCH_TYPE'] | lower }}_match 
        {% endfor %}

    FROM get_all_match_combinations_existing_index

    GROUP BY 
        master_entity_hash_id
        , source_entity_hash_id

)

-- To exclude ID's present in the do_not_merge
, get_do_not_merge_records AS (

    {{ do_not_merge('pivot_matches_existing_index', 'manual_do_not_merge') }}

)

-- Calculate match_score and name_distance for appropriate columns
, merge_decisions_existing_index AS (

    SELECT
    
        p.source_entity_hash_id
        , p.master_entity_hash_id
        
        , s.record_source                                                               AS source_record_source
        , s.table_source                                                                AS source_table_source

        , m.record_source                                                               AS master_record_source
        , m.table_source                                                                AS master_table_source

        , s.entity_natural_key                                                          AS source_entity_natural_key
        , m.entity_natural_key                                                          AS master_entity_natural_key
        
        {% if var('data_loaded_at') != 'null' %}
        , s.source_loaded_at                                                            AS source_loaded_at
        {% endif %}

        {% for i in temp_match_keys.rows %}
        {% if i['MATCH_METHOD'] != 'ALGORITHM' and i['MATCH_METHOD'] != 'algorithm' %}
        , p.is_{{ i['MATCH_TYPE'] | lower }}_match
        {% endif %}
        {% endfor %}

        {% for i in match_keys.rows %}
        {% if i['MATCH_METHOD'] == 'ALGORITHM' or i['MATCH_METHOD'] == 'algorithm' %}

        , s.{{ i['COLUMN_NAME'] | lower }}                                                                       AS source_{{ i['MATCH_TYPE'] | lower }}_match_key
        , m.{{ i['COLUMN_NAME'] | lower }}                                                                   AS master_{{ i['MATCH_TYPE'] | lower }}_match_key

        , EDITDISTANCE(master_{{ i['MATCH_TYPE'] | lower }}_match_key, source_{{ i['MATCH_TYPE'] | lower }}_match_key)      AS {{ i['MATCH_TYPE'] | lower }}_distance
        , LEN(master_{{ i['MATCH_TYPE'] | lower }}_match_key) + LEN(source_{{ i['MATCH_TYPE'] | lower }}_match_key)         AS {{ i['MATCH_TYPE'] | lower }}_length
        , DIV0({{ i['MATCH_TYPE'] | lower }}_distance, {{ i['MATCH_TYPE'] | lower }}_length)                                AS {{ i['MATCH_TYPE'] | lower }}_distance_pc

        , IFF({{ i['MATCH_TYPE'] | lower }}_distance_pc > {{ i['MATCH_DIFF_MAX'] }}
            , 0, (1 - {{ i['MATCH_TYPE'] | lower }}_distance_pc) * {{ i['MATCH_WEIGHTING'] }})                               AS {{ i['MATCH_TYPE'] | lower }}_match_score

        {% endif %}
        {% endfor %}

        {% for i in temp_match_keys.rows -%}

        {% if i['MATCH_METHOD'] != 'ALGORITHM' and i['MATCH_METHOD'] != 'algorithm' %}
        , IFF(p.is_{{ i['MATCH_TYPE'] | lower }}_match = 1
            , {{ i['MATCH_WEIGHTING'] }}, 0)                                            AS {{ i['MATCH_TYPE'] | lower }}_match_score
        {% endif %}

        {% endfor %}

        {% if temp_match_keys.rows -%}
            , {% for i in temp_match_keys.rows -%}
            
                {{ i['MATCH_TYPE'] | lower }}_match_score
                {% if not loop.last %} + {% endif %}

            {% endfor %}
        {% endif %}                                                                    AS total_match_score
                                                                                                                            
        , CASE
            WHEN total_match_score >= {{ var('threshold_merge') }} THEN 'MERGE'
            WHEN total_match_score >= {{ var('threshold_possible_merge') }} THEN 'POSSIBLE MERGE'
            ELSE 'DO NOT MERGE'
        END                                                                            AS merge_decision
        
        , CASE master_record_source  
            {{ get_source_priority_case_block() }}
        END                                                                            AS master_record_source_priority
        
    FROM pivot_matches_existing_index p

    JOIN master_index m 
    ON 
        p.master_entity_hash_id = m.entity_hash_id

    JOIN source_records_batch s 
    ON 
        p.source_entity_hash_id = s.entity_hash_id

    -- Do not include merges on the do not merge list
    WHERE NOT EXISTS (

        SELECT 
        
            1

        FROM get_do_not_merge_records dnm

        WHERE 
            p.source_entity_hash_id = dnm.source_entity_hash_id

        )

)

-- Discard the records, When entity_total_match_score in less than 0.7
SELECT 

    *
    , CURRENT_TIMESTAMP()                                                              AS match_timestamp

FROM merge_decisions_existing_index

-- WHERE 
--     total_match_score >= ({{ var('threshold_merge') }} * var('threshold_total_match_score')) -- @TODO: Move to global variable