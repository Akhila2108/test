{% macro generate_hash_id(record_source, table_source, natural_key) %}

    CAST(MD5_BINARY(CONCAT(
    IFNULL(NULLIF(UPPER(TRIM(CAST({{record_source}} AS VARCHAR))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST({{table_source}} AS VARCHAR))), ''), '^^'), '||',
    IFNULL(NULLIF(UPPER(TRIM(CAST({{natural_key}} AS VARCHAR))), ''), '^^') ))
         AS BINARY(16)) 

{% endmacro %}