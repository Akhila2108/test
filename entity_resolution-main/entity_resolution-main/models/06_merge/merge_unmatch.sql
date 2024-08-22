{{ 
    config(
        tags=["stage", "merge_unmatched"],
        materialized="table",
        post_hook= "{{ update_master_entity(this) }}"
    ) 
}}

--  Deduplication of nodes

-- depends_on: {{ ref('merge_match') }}

WITH

{{ node_dedupe('graph_edge_in') }} 

-- Get current highest group_company_id as max_id_value

, max_id AS (

    SELECT 

        MAX(group_id)                                                  AS max_id_value

    FROM {{ ref('master_entity') }}
)

-- Load the unmatched records to Company Master

, unmerged_records AS (

    SELECT

        ROW_NUMBER() OVER (ORDER BY c.entity_hash_id) + max_id_value  AS group_id

        , c.entity_hash_id	
        , c.record_source	
        , c.table_source	
        {% if var('data_loaded_at') != 'null' %}
        , c.source_loaded_at	
        {% endif %}
        , c.entity_natural_key	
        , 'UNMERGED'                                                  AS merge_reason	
        , CURRENT_TIMESTAMP()                                         AS merge_timestamp

    FROM {{ ref('source_entity') }} c

    LEFT JOIN vertices v 
    ON 
        v.entity_hash_id = c.entity_hash_id
        
    LEFT JOIN max_id m 
    ON 
        1 = 1

    WHERE 
        v.group_id IS NULL  

)

SELECT ur.* 
FROM unmerged_records ur

INNER JOIN {{ ref('source_entity') }} ssr
ON 
    ssr.entity_hash_id = ur.entity_hash_id