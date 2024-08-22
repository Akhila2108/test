{{ 
    config(
        tags=["match", "quick_match"],
        materialized="table",
        post_hook="{{ update_quick_match_audit(this) }}"
    )
}}

-- depends_on: {{ ref('config_table_sources') }}

-- Filter out the quick match columns to dynamically generete attributes from seed file
{%  set quick_match_columns = get_source_info(var('meta_entity_match_key_values'), 'MATCH_TYPE', 'quick_match') %}

{%  set manual_force_merge_columns = get_source_info('manual_force_merge') %}

WITH 

{{ get_master_source_batch('quick_match') }}


{% if manual_force_merge_columns.rows %}

-- Explicitly merge ID's that are present in force merge list
, force_merge AS (

    SELECT

        s.entity_hash_id                                                         AS source_entity_hash_id
        , m.entity_hash_id                                                       AS master_entity_hash_id

        , s.record_source
        , s.table_source

        {% if var('data_loaded_at') != 'null' %}
        , s.source_loaded_at
        {% endif %}
        
        , s.entity_natural_key
        , m.group_id

        , CASE m.record_source
            {{ get_source_priority_case_block() }}
        END AS master_record_source_priority

        {% if var('data_loaded_at') != 'null' %}
        ,  m.source_loaded_at                                                   AS master_source_loaded_at
        {% endif %}
        , 'FORCE MERGE'                                                         AS merge_reason
        -- , 1                                                                  AS merge_priority -- @TODO - We don't need it and comment it 

    FROM {{ ref('manual_force_merge') }} fm 

    INNER JOIN source_records_batch s 
    ON 
        TO_BINARY(fm.source_entity_hash_id) = s.entity_hash_id

    INNER JOIN master_index m 
    ON 
        TO_BINARY(fm.master_entity_hash_id) = m.entity_hash_id
    
)

{% endif %}

{% for c in quick_match_columns.rows %}

, {{ c['COLUMN_NAME'] }}_lookup AS (

    SELECT
        
        s.entity_hash_id                                                      AS source_entity_hash_id
        , m.entity_hash_id                                                    AS master_entity_hash_id

        , s.record_source
        , s.table_source
        {% if var('data_loaded_at') != 'null' %}
        , s.source_loaded_at
        {% endif %}
        , s.entity_natural_key
        , m.group_id
        
        , CASE m.record_source
            {{ get_source_priority_case_block() }}
        END AS master_record_source_priority

        {% if var('data_loaded_at') != 'null' %}
        , m.source_loaded_at                                                 AS master_source_loaded_at
        {% endif %}

        , '{{ c['COLUMN_NAME'] | upper }} LOOKUP'                            AS merge_reason

        -- , 2                                                               AS merge_priority -- @TODO - We don't need it and comment it 

    FROM source_records_batch s

    INNER JOIN master_index m
    ON 
        -- LTRIM(s.company_registration_number, 0) = LTRIM(m.company_registration_number, 0)            -- Remove leading 0
        s.{{ c['COLUMN_NAME'] }} = m.{{ c['COLUMN_NAME'] }}
        AND s.entity_hash_id != m.entity_hash_id

)

{% endfor %}

-- Combine all the matched records into one table
, combine_all_quick_matches AS (

    SELECT

        *
        , CURRENT_TIMESTAMP()                                              AS match_calulation_timestamp

    FROM (

        {% if manual_force_merge_columns.rows %}

            SELECT * FROM force_merge
            UNION ALL
        
        {% endif %}
        
        {% if quick_match_columns.rows %}

        {% for c in quick_match_columns.rows %}
            SELECT * FROM {{ c['COLUMN_NAME'] }}_lookup
            {%- if not loop.last %} UNION ALL {% endif %}
        {% endfor %}

        {% endif %}
    )

)

-- Exclude ID's present in the do not merge list
, get_do_not_merge_records AS (
    {{ do_not_merge('combine_all_quick_matches', 'manual_do_not_merge') }}
)

-- Exclude the records in do not merge category
, remove_do_not_merges AS (

    SELECT 
    
        *

    FROM combine_all_quick_matches a

    WHERE NOT EXISTS (

        SELECT 
        
            1 
        
        FROM get_do_not_merge_records dnm

        WHERE 
            a.source_entity_hash_id = dnm.source_entity_hash_id
                
    )  

)

, final AS ( 

    SELECT

        group_id
        , record_source
        , table_source

        , source_entity_hash_id                                           AS entity_hash_id
        , master_entity_hash_id

        , entity_natural_key
        , merge_reason

        {% if var('data_loaded_at') != 'null' %}
        , source_loaded_at
        {% endif %}
                    
    FROM remove_do_not_merges

)

SELECT * FROM final