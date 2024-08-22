{% macro generate_default_skeleton_entity_model(model_name) -%}

    SELECT

        CAST('{{ var('default_entity_group_id') }}'  AS NUMBER)                    AS group_id
        , CAST('{{ var('default_entity_name') }}' AS VARCHAR(50))                  AS record_source
        , CAST('{{ var('default_entity_name') }}' AS VARCHAR(50))                  AS table_source

        , CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))               AS entity_hash_id

        {% if model_name == 'audit_match_quick' %}
        , CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))               AS master_entity_hash_id
        {% endif %}

        , CAST('{{ var('default_entity_name') }}' AS VARCHAR(100))                 AS entity_natural_key

        , CAST('{{ var('default_entity_name') }}' AS VARCHAR(100))                 AS merge_reason

        {% if model_name == 'master_entity' %}
        , TO_TIMESTAMP_LTZ(CURRENT_TIMESTAMP())                                    AS merge_timestamp
        {% endif %}

        {% if var('data_loaded_at') != 'null' %}
        , TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())                                     AS source_loaded_at
        {% endif %}
        
        {% if model_name == 'audit_match_quick' %}
        , TO_TIMESTAMP_LTZ(CURRENT_TIMESTAMP())                                    AS match_timestamp
        {% endif %}

    FROM {{ ref('prep_entity') }}

    WHERE 
        1 = 0

{% endmacro %}