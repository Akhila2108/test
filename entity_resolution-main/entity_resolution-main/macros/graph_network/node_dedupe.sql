{% macro node_dedupe(table_name) %}

left_vertices AS (

    SELECT 
    
        DISTINCT
        left_node                AS entity_hash_id
        , group_id               

    FROM {{ ref(table_name) }}

)

, right_vertices AS (

    SELECT 
    
        DISTINCT
        right_node              AS entity_hash_id
        , group_id               

    FROM {{ ref(table_name) }}

)

--  Deduplication of nodes

, vertices AS (

    SELECT * FROM left_vertices

    UNION

    SELECT  * FROM right_vertices

)

{% endmacro %}