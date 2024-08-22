{% macro update_merge_reason(source_records) %}

{% set get_matched_records%}

    SELECT group_id FROM {{ ref('master_entity') }} 
    WHERE group_id IN (SELECT group_id FROM {{ ref('master_entity') }} WHERE merge_reason = 'UNMERGED')
    GROUP BY group_id 
    HAVING COUNT(group_id) > 1;

{% endset %}

{% if execute %}

    {% set result = run_query(get_matched_records).columns[0].values() %}

{% endif %}

{%- set chunk_list = [] -%}
{%- for i in result -%}
    {%- do chunk_list.append(i) -%}

    {%- if (chunk_list | length) == 10000 -%}

        {% set update_merge_reason %}
            UPDATE {{ ref('master_entity') }} SET merge_reason = 'GRAPH' WHERE group_id IN 
            ( {%- for group_id_value in chunk_list -%} {{ group_id_value }}
                {%- if not loop.last -%} , {% endif -%}
            {%- endfor -%} )
        {% endset %}

        {% do run_query(update_merge_reason) %}
        {%- do chunk_list.clear() -%}
    {%- endif -%}

{%- endfor -%}

{%- if (chunk_list | length) != 0 -%}

    {% set update_merge_reason %}
        UPDATE {{ ref('master_entity') }} SET merge_reason = 'GRAPH' WHERE group_id IN 
        ( {%- for group_id_value in chunk_list -%} {{ group_id_value }}
            {%- if not loop.last -%} , {% endif -%}
        {%- endfor -%} )
    {% endset %}

    {% do run_query(update_merge_reason) %}
    {%- do chunk_list.clear() -%}
{%- endif -%}

{% endmacro %}