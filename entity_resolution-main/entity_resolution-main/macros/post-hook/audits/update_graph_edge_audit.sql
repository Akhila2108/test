{% macro update_graph_edge_audit(source_records) -%}

    INSERT INTO {{ ref('audit_graph_edge') }} (

        left_node
        , right_node
        , old_group_id
        , group_id
        , merge_timestamp

    )

    SELECT 

        geo.*
        , CURRENT_TIMESTAMP()    AS merge_timestamp

    FROM {{source_records}} geo

    LEFT JOIN  {{ ref('audit_graph_edge') }} gea 
    ON 
        geo.left_node = gea.left_node 
        AND geo.right_node = gea.right_node

    WHERE 
        geo.group_id != gea.group_id 
        OR gea.group_id IS NULL

{% endmacro %}