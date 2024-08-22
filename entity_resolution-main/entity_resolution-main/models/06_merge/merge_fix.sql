{{ 
    config(
        tags=["stage", "merge_fixed"],
        materialized="table",
        post_hook= "{{ update_master_entity(this) }}"
    ) 
}}

-- They explicitly assinged certain companies to certain group ids and assigning those here.

WITH 

fixed_group_records AS (

    SELECT

        gi.*

    FROM {{ ref('graph_edge_in') }} gi

    LEFT JOIN {{ ref('graph_edge_out') }} g 
    ON 
        g.left_node = gi.left_node 
        AND g.right_node = gi.right_node  

    WHERE 
        g.left_node IS NULL

)

, left_vertices AS (

        SELECT 
        
            DISTINCT
            left_node      AS entity_hash_id

        FROM fixed_group_records 

)

, right_vertices AS (

        SELECT 
        
            DISTINCT
            right_node    AS entity_hash_id

        FROM fixed_group_records 

)

, verticies_ids_unfiltered AS (

    SELECT * FROM left_vertices

    UNION

    SELECT * FROM right_vertices

)

, vertices AS (

    SELECT 
    
        v.*

    FROM verticies_ids_unfiltered v

    LEFT JOIN {{ ref('source_entity') }} m 
    ON 
        m.entity_hash_id = v.entity_hash_id

    WHERE 
        m.entity_hash_id IS NULL

)

, entity_master_act AS (

    SELECT 
    
        * 
    
    FROM {{ ref('master_entity')}}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_hash_id ORDER BY merge_timestamp DESC) = 1
)

, fixed_records AS(

    SELECT 
    
        DISTINCT
        COALESCE(n.group_id, w.group_id, 49)    AS group_id      
        , c.entity_hash_id	
        , c.record_source	
        , c.table_source
        {% if var('data_loaded_at') != 'null' %}
        , c.source_loaded_at	
        {% endif %}
        , c.entity_natural_key	
        , 'FIXED'                               AS merge_reason	
        , CURRENT_TIMESTAMP()                   AS merge_timestamp

    FROM {{ ref('prep_entity') }} c

    INNER JOIN vertices v 
    ON 
        v.entity_hash_id = c.entity_hash_id

    LEFT JOIN entity_master_act cm 
    ON 
        c.entity_hash_id = cm.entity_hash_id

    LEFT JOIN {{ ref('manual_entity_names') }} n
    ON 
        c.entity_name_match_value = n.name

    LEFT JOIN {{ ref('manual_entity_wildcards') }} w 
    ON 
        c.entity_name_match_value LIKE w.wildcard

)

SELECT mr.*
FROM fixed_records mr

INNER JOIN {{ ref('source_entity') }} ssr 
ON 
    ssr.entity_hash_id = mr.entity_hash_id