{{ 
    config(
        tags=["core"],
        materialized="table"
    )
}}

WITH

-- Get latest load date for each company hash id
latest_records AS (

    SELECT
        
        entity_hash_id

        {% if var('data_loaded_at') != 'null' %}
        , MAX(source_loaded_at)            AS source_loaded_at
        {% endif %}

    FROM {{ ref('master_entity') }}

    GROUP BY
        entity_hash_id

)

-- Retrive latest records by comparing with (master_entity)
, source_records_batch AS ( 

    SELECT

        s.*

    FROM {{ ref('prep_entity') }} s

    LEFT JOIN latest_records m 
    ON 
        s.entity_hash_id = m.entity_hash_id

    {% if var('data_loaded_at') != 'null' %}
    WHERE 
        s.source_loaded_at > IFNULL(m.source_loaded_at, '{{var('min_data_loaded_at')}}')
    {% endif %}

)

SELECT * FROM source_records_batch 