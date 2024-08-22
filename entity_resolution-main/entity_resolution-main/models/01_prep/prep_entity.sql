{{
    config(
        tags=["prep"]
    )
}}

-- depends_on: {{ ref('exclude_column_values') }}

{%  set sources = get_source_info(var('meta_source_columns')) %}

WITH

select_columns AS (

    SELECT

        entity_hash_id,
        
        {% for i in sources.rows %}
            {{ i['COLUMN_NAME'] }}::{{ i['DATA_TYPE'] }}    AS {{ i['COLUMN_NAME'] }}
        {%- if not loop.last -%} , {% endif -%}
        {% endfor %}

        {% if var('data_loaded_at') != 'null' %}
        , source_loaded_at
        {% endif %}

    FROM {{ ref('entity') }}

)

-- Apply cleanse macros for the appropriate columns
, cleansing AS (

    SELECT

        entity_hash_id

        -- Clean rules
        {% for i in sources.rows %}

            {% if i['CLEANSE_RULE'].lower() == 'default' %}
                , {{ clean_default(i['COLUMN_NAME']) }}                         AS {{ i['COLUMN_NAME'] }}
            {% endif %}

            {% if i['CLEANSE_RULE'].lower() == 'remove_blankspace' %}
                , {{ remove_blankspaces(i['COLUMN_NAME']) }}                    AS {{ i['COLUMN_NAME'] }}
            {% endif %}

            {% if i['CLEANSE_RULE'].lower() == 'crn' %}
                , {{ clean_company_registration_number(i['COLUMN_NAME']) }}     AS {{ i['COLUMN_NAME'] }}
            {% endif %}

            {% if i['CLEANSE_RULE'].lower() == 'x' %}
                , {{ i['COLUMN_NAME'] }}                                        AS {{ i['COLUMN_NAME'] }}
            {% endif %}
            
            {% if i['CLEANSE_MATCH_RULE'] == 'email' %}
                , {{ extract_domain_from_email(i['COLUMN_NAME']) }}             AS {{ i['COLUMN_NAME'] }}_domain
            {% endif %}

        {% endfor %}

        -- To generate Match value node type
        {% for i in sources.rows %}

            {% if i['CLEANSE_MATCH_RULE'].lower() == 'company_name' %}
                , {{ clean_company_name(i['COLUMN_NAME']) }}                    AS {{ i['COLUMN_NAME'] }}_match_value
            {% endif %}

            {% if i['CLEANSE_MATCH_RULE'].lower() == 'hotel_name' %}
                , {{ clean_hotel_name(i['COLUMN_NAME']) }}                    AS {{ i['COLUMN_NAME'] }}_match_value
            {% endif %}

            {% if i['CLEANSE_MATCH_RULE'].lower() == 'crn' %}
                , {{ clean_company_registration_number(i['COLUMN_NAME']) }}     AS {{ i['COLUMN_NAME'] }}_match_value
            {% endif %}

            {% if i['CLEANSE_MATCH_RULE'].lower() == 'email' %}
                , {{ clean_email(i['COLUMN_NAME']) }}                           AS {{ i['COLUMN_NAME'] }}_match_value
                , {{ clean_website(i['COLUMN_NAME'] + '_domain') }}             AS {{ i['COLUMN_NAME'] }}_domain_match_value
            {% endif %}

            {% if i['CLEANSE_MATCH_RULE'].lower() == 'mobile_number' %}
                , {{ clean_telephone(i['COLUMN_NAME']) }}                       AS {{ i['COLUMN_NAME'] }}_match_value                  
            {% endif %}
            
            {% if i['CLEANSE_MATCH_RULE'].lower() == 'website' %}
                , {{ clean_website(i['COLUMN_NAME']) }}                         AS {{ i['COLUMN_NAME'] }}_match_value                     
            {% endif %}

            {% if i['CLEANSE_MATCH_RULE'].lower() == 'default' %}
                , {{ detault_cleanse_match_value(i['COLUMN_NAME']) }}           AS {{ i['COLUMN_NAME'] }}_match_value                     
            {% endif %}
            
        {% endfor %}

        {% if var('data_loaded_at') != 'null' %}
        , source_loaded_at
        {% endif %}
       
    FROM select_columns

)

, final AS (

    SELECT

        entity_hash_id

        {% for i in sources.rows %}

            {% if i['CLEANSE_MATCH_RULE'] == 'x' %}
                , {{ i['COLUMN_NAME'] }}
            {% endif %}

        {% endfor %}

        {% for i in sources.rows %}

            {% if i['CLEANSE_MATCH_RULE'].lower() != 'x' %}
                , {{ i['COLUMN_NAME'] }}
                , {{ i['COLUMN_NAME'] }}_match_value

                {% if i['CLEANSE_MATCH_RULE'] == 'email' %}
                    , {{ i['COLUMN_NAME'] }}_domain_match_value
                {% endif %}

            {% endif %}

        {% endfor %}

        {% if var('data_loaded_at') != 'null' %}
        , source_loaded_at
        {% endif %}
       
    FROM cleansing

)

SELECT * FROM final 