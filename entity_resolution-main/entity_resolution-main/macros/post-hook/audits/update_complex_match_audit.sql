{% macro update_complex_match_audit(source_records) -%}

    {% set match_keys = get_entity_match_key() %}

    INSERT INTO {{ ref('audit_match_complex') }} (

        source_entity_hash_id
        , master_entity_hash_id

        , source_record_source
        , source_table_source

        , master_record_source
        , master_table_source
        
        , source_entity_natural_key
        , master_entity_natural_key
        
        {% if var('data_loaded_at') != 'null' %}
        , source_source_loaded_at
        {% endif %}

        {% for i in match_keys.rows %}
        {% if i['MATCH_TYPE'] != 'ENTITY_NAME' %}
        , is_{{ i['MATCH_TYPE'] }}_match
        {% endif %}
        {% endfor %}

        {% for i in match_keys.rows %}
        {% if i['MATCH_TYPE'] == 'ENTITY_NAME' %}
        , source_{{ i['COLUMN_NAME'] | lower }}_match_key
        , master_{{ i['COLUMN_NAME'] | lower }}_match_key

        , {{ i['COLUMN_NAME'] | lower }}_distance
        , {{ i['COLUMN_NAME'] | lower }}_length
        , {{ i['COLUMN_NAME'] | lower }}_distance_pc
        , {{ i['COLUMN_NAME'] | lower }}_match_score
        {% endif %}
        {% endfor %}

        {% for i in match_keys.rows %}
        {% if i['MATCH_TYPE'] != 'ENTITY_NAME' %}
        , {{ i['MATCH_TYPE'] }}_match_score
        {% endif %}
        {% endfor %}

        , total_match_score

        , merge_decision
        , master_record_source_priority
        , match_timestamp 

    )

    SELECT

        cm.*

    FROM {{source_records}} cm

{% endmacro %}