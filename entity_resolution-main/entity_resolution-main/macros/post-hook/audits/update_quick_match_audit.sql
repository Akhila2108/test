{% macro update_quick_match_audit(source_records) -%}

    INSERT INTO {{ ref('audit_match_quick') }} (

        group_id
        , record_source
        , table_source
        , entity_hash_id
        , master_entity_hash_id
        , entity_natural_key
        , merge_reason
        {% if var('data_loaded_at') != 'null' %}
        , source_loaded_at
        {% endif %}
        , match_timestamp

    )

    SELECT 

        *
        , CURRENT_TIMESTAMP()       AS match_timestamp

    FROM {{ source_records }} qm

{% endmacro %}