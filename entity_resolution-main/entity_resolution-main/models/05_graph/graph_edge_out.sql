{{
    config(
        tags=["merge", "graph_output"],
        materialized="table",
        post_hook= "{{ call_proc_graph_network(this) }} {{ update_graph_edge_audit(this) }}"
    )
}}


WITH

pairs_to_resolve AS (

    SELECT 
    
        *
    
    FROM  {{ ref('graph_edge_in') }}

)

-- Exclusion of records based on the seed, Mentioned below.

, exclusions_applied AS (

    SELECT

        DISTINCT
        ptr.*

    FROM pairs_to_resolve ptr
    
    LEFT JOIN {{ ref('prep_entity')}} l 
    ON
        ptr.left_node = l.entity_hash_id

    LEFT JOIN {{ ref('prep_entity') }} r 
    ON
        ptr.right_node = r.entity_hash_id

    LEFT JOIN {{ ref('manual_entity_names') }} ln 
    ON
        l.entity_name_match_value = ln.name

    LEFT JOIN {{ ref('manual_entity_names') }} rn 
    ON
        r.entity_name_match_value = rn.name

    LEFT JOIN {{ ref('manual_entity_wildcards') }} lw 
    ON
        l.entity_name_match_value LIKE lw.wildcard

    LEFT JOIN {{ ref('manual_entity_wildcards') }} rw 
    ON
        r.entity_name_match_value LIKE rw.wildcard

    WHERE
        COALESCE(ln.group_id, lw.group_id) IS NULL 
        AND COALESCE(rn.group_id, rw.group_id) IS NULL

)

SELECT * FROM exclusions_applied