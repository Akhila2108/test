{{ 
    config(
        tags=["audit", "init_audit"],
        materialized="incremental"
    ) 
}}

-- Creates empty table required for dependencies to operate properly
-- Populated later in the process by macro: update_graph_edge_audit

SELECT 

    CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))                  AS left_node
    , CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))                AS right_node

    , CAST('{{ var('default_entity_group_id') }}' AS NUMBER)                    AS group_id
    , CAST('{{ var('default_entity_group_id') }}' AS NUMBER)           AS old_group_id

    , TO_TIMESTAMP_LTZ(CURRENT_TIMESTAMP())                                     AS merge_timestamp

FROM {{ ref('prep_entity') }}

WHERE 
    1 = 0