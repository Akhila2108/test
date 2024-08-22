{{ 
    config(
        tags=["ssot"],
        materialized="table"
    ) 
}}

-- Retrieve latest matched records from master entity

{%  set sources = get_source_info(var('meta_ssot_columns')) %}

WITH 

company_master AS (

    SELECT

        *

    FROM {{ ref('master_entity') }}

)

-- Retrieve latest records from prep entity

, prep_records AS (

    SELECT

        *
    
    FROM {{ ref('prep_entity') }}

)

-- Join latest_company_master and prep_records CTE's to retrieve the required columns.

, collate_source_records AS (

    SELECT 

        m.*,
        {% for i in sources.rows %}
            p.{{ i['COLUMN_NAME'] }}
        {%- if not loop.last -%} , {% endif -%}
        {% endfor %}

    FROM company_master m

    LEFT JOIN prep_records p
    ON
        m.entity_hash_id = p.entity_hash_id

)

-- Apply survivorship rule to all the required columns

{% for i in sources.rows %}
    {% if i['SSOT_RULE'] == 'most_recent_record' %}

        {{ most_recent_record(i['COLUMN_NAME'], i['GROUP_BY_COLUMN'], i['ORDER_COLUMN']) }}

    {% elif i['SSOT_RULE'] == 'dominant_value' %}

        {{ dominant_value(i['COLUMN_NAME'], i['GROUP_BY_COLUMN'], i['ORDER_COLUMN'], 'record_source', i['FIRST_SOURCE']) }}

    {% elif i['SSOT_RULE'] == 'most_complete_record' %}

        {{most_complete_record(i['COLUMN_NAME'], i['GROUP_BY_COLUMN'], i['ORDER_COLUMN'], i['LENGTH_COLUMN'])}}

    {% elif i['SSOT_RULE'] == 'lowest_record' %}

        {{lowest_record(i['COLUMN_NAME'], i['GROUP_BY_COLUMN'])}}

    {% elif i['SSOT_RULE'] == 'highest_record' %}

        {{highest_record(i['COLUMN_NAME'], i['GROUP_BY_COLUMN'])}}

    {% endif%}
{% endfor %}
-- Retrieve the golden records

, golden_records AS (
    
    SELECT 
  
        DISTINCT
        csr.group_id 
        {% for i in sources.rows %}
            ,{{ i['COLUMN_NAME'] }}.use_value  AS {{ i['COLUMN_NAME'] }}
        {% endfor %}

    FROM collate_source_records csr

    {% for i in sources.rows %}
        LEFT JOIN group_{{ i['COLUMN_NAME'] }} {{ i['COLUMN_NAME'] }}
        ON
            {{ i['COLUMN_NAME'] }}.group_id = csr.group_id
    {% endfor %}
    
)

SELECT * FROM golden_records