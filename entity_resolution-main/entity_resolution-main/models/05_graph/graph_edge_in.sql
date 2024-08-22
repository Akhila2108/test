{{ 
    config(
        tags=["merge", "graph_input"],
        materialized="table",
    ) 
}}

{%  set manual_force_merge_columns = get_source_info('manual_force_merge') %}

WITH 

-- Align the left_node and right_node

matches AS (

    SELECT 

        DISTINCT
        CASE 
            WHEN entity_hash_id < master_entity_hash_id 
                THEN entity_hash_id 
            ELSE master_entity_hash_id 
        END                                                             AS left_node

        , CASE 
            WHEN entity_hash_id < master_entity_hash_id 
                THEN master_entity_hash_id 
            ELSE entity_hash_id 
        END                                                             AS right_node
   
    FROM {{ ref('quick_match') }} q

    WHERE 
        left_node IS NOT NULL

    UNION ALL

    SELECT 

        DISTINCT
        CASE 
            WHEN source_entity_hash_id < master_entity_hash_id 
                THEN source_entity_hash_id 
            ELSE master_entity_hash_id 
        END                                                              AS left_node

        , CASE 
            WHEN source_entity_hash_id < master_entity_hash_id 
                THEN master_entity_hash_id 
            ELSE source_entity_hash_id 
        END                                                              AS right_node

    FROM {{ ref('complex_match') }}

    WHERE 
        MERGE_DECISION = 'MERGE'

    {% if manual_force_merge_columns.rows %}

    UNION ALL 

        SELECT 

            DISTINCT 
            TO_BINARY(CASE 
                WHEN source_entity_hash_id < master_entity_hash_id 
                    THEN source_entity_hash_id 
                ELSE master_entity_hash_id 
            END)                                                         AS left_node

            , TO_BINARY(CASE 
                WHEN source_entity_hash_id < master_entity_hash_id 
                    THEN master_entity_hash_id 
                ELSE source_entity_hash_id 
            END)                                                         AS right_node

        FROM {{ ref('manual_force_merge') }}

    {% endif %}

    )
    
-- Get current highest group_id from master_entity

, new_group_id AS (

    SELECT 

        -- Set to 50 on full refresh to allow fixed ids to not be duplicated
        IFNULL(MAX(group_id), {{ var('default_entity_top_group_id') }}) AS top_group_id 

    FROM {{ ref('master_entity') }}
)

-- Get all records from master_entity

, entity_master_act AS (

    SELECT 
    
        * 
    
    FROM {{ ref('master_entity') }}

)

-- Generate Group Id

, final AS (

    SELECT 

        DISTINCT
        m.*
        , COALESCE(cm.group_id, DENSE_RANK() OVER (ORDER BY left_node) + top_group_id)         AS group_id                                                                               
        , group_id                                                                             AS old_group_id

    FROM matches m

    {% if manual_force_merge_columns.rows %}

    LEFT JOIN {{ ref('manual_force_merge') }} dnm 
    ON 
        (TO_BINARY(dnm.source_entity_hash_id) = m.left_node 
        AND TO_BINARY(dnm.master_entity_hash_id) = m.right_node)
        OR (TO_BINARY(dnm.source_entity_hash_id) = m.right_node 
        AND TO_BINARY(dnm.master_entity_hash_id) = m.left_node)

    {% endif %}

    LEFT JOIN entity_master_act cm 
    ON 
        m.left_node = cm.entity_hash_id 
        OR m.right_node = cm.entity_hash_id
 
    CROSS JOIN new_group_id group_id   

    {% if manual_force_merge_columns.rows %}

    WHERE 
        dnm.source_entity_hash_id IS NULL

    {% endif %}
)

SELECT * FROM final