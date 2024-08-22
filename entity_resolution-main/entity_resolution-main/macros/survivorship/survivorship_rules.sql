-- Rule: To identify the most recent record using LUD(Last update date) 

{% macro most_recent_record(field, group_by_column, order_column) %}

,group_{{field}} AS (
    SELECT
        group_id
        ,FIRST_VALUE({{ field }}) OVER (
            PARTITION BY {{ group_by_column }} ORDER BY {{ order_column }} DESC NULLS LAST) AS Use_Value
    FROM
        collate_source_records
)

{% endmacro %}

-- Rule: To identify the most complete record using length 

{% macro most_complete_record(field, group_by_column, order_column, length_column) %}

,group_{{field}} AS (
    SELECT
        group_id
        ,FIRST_VALUE({{ field }}) OVER (
            PARTITION BY {{ group_by_column }} ORDER BY {{ length_column }} DESC, {{ order_column }} DESC NULLS LAST)   AS Use_Value
    FROM
        collate_source_records
)

{% endmacro %}

-- Rule: To identify the most recent record by record source

{% macro most_recent_by_record_source(field, group_by_column, order_column, list_of_sources) %}

,group_{{field}} AS (
    SELECT 
        group_id
        ,FIRST_VALUE({{ field }}) OVER (
            PARTITION BY {{ group_by_column }} ORDER BY 
                CASE RECORD_SOURCE
                    {% for record_source in list_of_sources %}
                       WHEN '{{ record_source }}' THEN {{ loop.index }}
                    {% endfor %}
                ELSE 999 END,
            {{ order_column }} DESC NULLS LAST )    AS Use_Value
    FROM
       collate_source_records 
)

{% endmacro %}

-- Rule: To identify the lowest record

{% macro lowest_record(field, group_by_column) %}

,group_{{ field }} AS (
    SELECT
        group_id
        ,FIRST_VALUE({{ field }}) OVER (
            PARTITION BY {{ group_by_column }} ORDER BY {{ field }} NULLS LAST)  AS Use_Value
    FROM 
        collate_source_records
)

{% endmacro %}

-- Rule: To identify the highest record

{% macro highest_record(field, group_by_column) %}

,group_{{field}} AS (
    SELECT
        group_id
        ,FIRST_VALUE({{ field }}) OVER (
            PARTITION BY {{ group_by_column }} ORDER BY {{ field }} DESC NULLS LAST) AS Use_Value
    FROM 
        collate_source_records
)

{% endmacro %}

-- Rule: To identify the dominant record using frequnetly occuring value and record source

{% macro dominant_value(field, group_by_column, date_order_column, source_column, First_Source) %}

, Child_{{ field }} AS (

    SELECT
    
        *
        , MAX(value_freq) OVER (PARTITION BY {{ group_by_column }})                                         AS dominant_value_count

    FROM (

        SELECT

            {{ group_by_column }}
            , {{ field }}                                                                                   AS Source_Value
            , {{ date_order_column }}                                                                       AS date_order
            , {{ source_column }}                                                                           AS Origin_Source
            , MAX(iff(Origin_Source='{{ First_Source }}', 1, 0)) OVER (PARTITION BY {{ group_by_column }})  AS has_1st_choice 
            , COUNT(DISTINCT Source_Value) OVER (PARTITION BY {{ group_by_column }})                        AS distinctCount
            , COUNT(Source_Value) OVER (PARTITION BY {{ group_by_column }}, Source_Value)                   AS value_freq
            , COUNT(*) OVER (PARTITION BY {{ group_by_column }})                                            AS childcount

        FROM collate_source_records

        WHERE 
            Source_Value IS NOT NULL 

    ) AS Subquery
    
)

, group_{{ field }} AS (

    SELECT

        {{ group_by_column }}
        , use_value

    FROM (

        SELECT

            {{ group_by_column }}
            , iff(Origin_Source = '{{ First_Source }}', 99, 0)                                             AS Source_Order
            , iff(dominant_value_count = value_freq, dominant_value_count, 0)                              AS dominant_value

            , CASE 
                WHEN has_1st_choice = 1 THEN FIRST_VALUE(Source_Value) OVER 
                    (PARTITION BY {{ group_by_column }} ORDER BY Source_Order DESC, date_order DESC NULLS LAST)
                WHEN div0((childcount - dominant_value_count), childcount) < 0.5 THEN FIRST_VALUE(Source_Value) OVER 
                    (PARTITION BY {{ group_by_column }} ORDER BY dominant_value DESC NULLS LAST) 
                WHEN (childcount - dominant_value_count)/childcount = 0.5 AND distinctCount > 2 THEN FIRST_VALUE(Source_Value) OVER 
                    (PARTITION BY {{ group_by_column }} ORDER BY date_order DESC NULLS LAST)
                ELSE FIRST_VALUE(Source_Value) OVER (PARTITION BY {{ group_by_column }} ORDER BY date_order DESC NULLS LAST)
            END                                                                                           AS Use_Value 

        FROM Child_{{ field }} 
        )

    GROUP BY 
        Use_Value
        , {{ group_by_column }}

)

{% endmacro %}