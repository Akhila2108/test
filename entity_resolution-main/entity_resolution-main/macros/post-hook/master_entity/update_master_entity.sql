{% macro update_master_entity(source_records) %}

INSERT INTO {{ ref('master_entity') }} (    

    group_id
    , entity_hash_id
    , record_source
    , table_source
    {% if var('data_loaded_at') != 'null' %}
    , source_loaded_at
    {% endif %}
    , entity_natural_key
    , merge_reason
    , merge_timestamp

)

SELECT 

    group_id
    , entity_hash_id 
    , record_source 
    , table_source
    {% if var('data_loaded_at') != 'null' %}
    , source_loaded_at 
    {% endif %}
    , entity_natural_key
    , merge_reason
    , CURRENT_TIMESTAMP() AS merge_timestamp

FROM {{ source_records }}

{% endmacro %}