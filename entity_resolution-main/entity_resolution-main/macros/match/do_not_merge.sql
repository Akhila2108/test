{% macro do_not_merge(cte_name, table_name)%}

    SELECT 

        DISTINCT 
        m.source_entity_hash_id
        , m.master_entity_hash_id
        
    FROM {{ cte_name }} m

    INNER JOIN {{ ref(table_name) }} dnm
    ON 
        m.source_entity_hash_id = TO_BINARY(dnm.source_entity_hash_id)
        AND m.master_entity_hash_id = TO_BINARY(dnm.master_entity_hash_id)

    UNION DISTINCT

    SELECT 

        DISTINCT            
        m.master_entity_hash_id
        , m.source_entity_hash_id

    FROM {{ cte_name }} m

    INNER JOIN {{ ref(table_name) }} dnm
    ON 
        m.source_entity_hash_id = TO_BINARY(dnm.source_entity_hash_id)
        AND m.master_entity_hash_id = TO_BINARY(dnm.master_entity_hash_id)

{% endmacro %}