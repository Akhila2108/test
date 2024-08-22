{{ 
    config(
        tags=["complex_match", "prep"]
    )
}}

-- depends_on: {{ ref('prep_entity') }}

{% set match_keys = get_entity_match_key() %}

-- Generate match_type and match_value records

{% if match_keys.rows  %}

WITH 

all_match_combinations AS (

    SELECT 

        DISTINCT 
        *
    
    FROM (

        {% for i in match_keys.rows  %} 
       
            SELECT 

                DISTINCT 
                entity_hash_id
                , entity_natural_key
                , record_source
                , table_source

                , '{{ i['MATCH_TYPE'] }}'                        AS match_type
                , {{ i['COLUMN_NAME'] }}                         AS match_value

                {% if var('data_loaded_at') != 'null' %}
                , source_loaded_at
                {% endif %}

            FROM {{ ref('prep_entity') }} 

            WHERE 
                NULLIF({{ i['COLUMN_NAME'] }}, '') IS NOT NULL
            
        {% if not loop.last %} UNION {% endif %}

       {% endfor %}

    )

)

-- Avoid high volume dummny values @TODO - To understand the why this is needed
, match_limiter AS (

    SELECT
        
        match_type
        , match_value
        , COUNT(*)                                              AS value_count
    
    FROM all_match_combinations

    GROUP BY
        match_type
        , match_value
        
    HAVING 
        value_count > {{ var('avoid_dummy_values_count_thresold') }}

)

-- Filter the records using match_type
, final AS (

    SELECT 

        MD5_BINARY(CONCAT(amc.match_type, amc.match_value))    AS match_key
        , amc.*
    
    FROM all_match_combinations amc

    LEFT JOIN match_limiter ml
    ON 
        amc.match_type = ml.match_type
        AND amc.match_value = ml.match_value
    
    WHERE
        ml.match_type IS NULL

)

SELECT * FROM final

{% endif %}

{% if not match_keys.rows  %}

SELECT
    MD5_BINARY('{{ var('default_entity_name') }}')                                              AS match_key
    , CAST('{{ var('default_entity_group_id') }}' AS BINARY(16))                                AS entity_hash_id
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})         AS entity_natural_key
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})         AS record_source
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})         AS table_source

    
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})         AS match_type
    , '{{ var('default_entity_name') }}' ::VARCHAR({{ var('default_varchar_length') }})         AS match_value

    {% if var('data_loaded_at') != 'null' %}
        , TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())                           AS source_loaded_at
    {% endif %}

FROM {{ ref('prep_entity') }} 

WHERE
    1 = 0

{% endif %}