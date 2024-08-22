{{ 
    config(
        tags=["stage", "merge_matched"],
        materialized="table",
        post_hook= ["{{ update_master_entity (this)}}", "{{ update_merge_reason(this) }}"]
    ) 
}}

-- depends_on: {{ ref('merge_fix') }}
--  Deduplication of nodes from graph out

WITH

{{ node_dedupe('graph_edge_out') }} 

, entity_master_act AS (

    SELECT 
    
        * 
    
    FROM {{ ref('master_entity') }}

)

-- Load the outputs of Graph out to master_entity table

, merged_records AS (

    SELECT 
    
        DISTINCT
        v.group_id	
        , c.entity_hash_id	
        , c.record_source	
        , c.table_source	
        {% if var('data_loaded_at') != 'null' %}
        , c.source_loaded_at	
        {% endif %}
        , c.entity_natural_key	
        , 'GRAPH'                                           AS merge_reason	
        , CURRENT_TIMESTAMP()                               AS merge_timestamp

    FROM {{ ref('prep_entity') }} c

    INNER JOIN vertices v 
    ON 
        v.entity_hash_id = c.entity_hash_id

    LEFT JOIN entity_master_act cm 
    ON 
        c.entity_hash_id = cm.entity_hash_id

    WHERE  
        (cm.group_id != v.group_id 
        OR cm.group_id IS NULL) 

)

SELECT * FROM merged_records