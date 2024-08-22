{% macro create_proc_graph_network() -%}

{% set function_sql_string %}

CREATE SCHEMA IF NOT EXISTS MERGE;
CREATE OR REPLACE PROCEDURE MERGE.PROC_GRAPH_NETWORK(DB_SCHEMA_TABLE VARCHAR)
returns VARCHAR

LANGUAGE javascript
AS
$$

    var returned_value = 1;
    var update_stmt = snowflake.createStatement(
        {
        sqlText: `CREATE OR REPLACE TABLE ` + DB_SCHEMA_TABLE + ` AS (

            WITH 
          
            tg AS (

            SELECT 
          
                node
                , MIN(group_id)                     AS min_group

            FROM (
                SELECT 
              
                    DISTINCT 
                    left_node                       AS node
                    , group_id 

                FROM ` + DB_SCHEMA_TABLE + `

                UNION ALL

                SELECT 
              
                    DISTINCT 
                    right_node                     AS node
                    , group_id 
              
                FROM ` + DB_SCHEMA_TABLE + `
            )

            GROUP BY 
                node

            )

            SELECT 

                left_node
                , right_node
                , group_id                        AS old_group_id

                , CASE 
                  WHEN tl.min_group <= tr.min_group THEN tl.min_group
                  ELSE tr.min_group 
                END                             AS group_id

            FROM ` + DB_SCHEMA_TABLE + ` t0

            LEFT JOIN tg tl 
            ON 
              t0.left_node = tl.node

            LEFT JOIN tg tr
            ON 
              t0.right_node = tr.node

            )`
        }

        );
     var loop_stmt = snowflake.createStatement(
        {
        sqlText: `
          SELECT 

            1

          FROM ` + DB_SCHEMA_TABLE + `

          WHERE 
            old_group_id <> group_id`
        }
        );
     var create_audit_table = snowflake.createStatement(
        {
        sqlText: `
        CREATE OR REPLACE TABLE "MERGE"."GRAPH_AUDIT_TABLE" (
        rows_affected INT NOT NULL,
        run_time datetime NOT NULL)`
        }
        );
     var insert_audit_table = snowflake.createStatement(
        {
        sqlText: `
        INSERT INTO "MERGE"."GRAPH_AUDIT_TABLE" 
        (rows_affected, run_time) 
              SELECT

                COUNT(*)
                , GETDATE()                     AS run_time
              
              FROM ` + DB_SCHEMA_TABLE + `

              WHERE 
                old_group_id <> group_id`
        }
        );
    var res = update_stmt.execute();
    var res_b = loop_stmt.execute();
    var res_c = create_audit_table.execute();
    var res_d = insert_audit_table.execute();
    res_c = create_audit_table.execute();
    returned_value = res_b.getRowCount(1);
    while (returned_value > 0)
        {
        res_b = loop_stmt.execute();
        res_d = insert_audit_table.execute();
        returned_value = res_b.getRowCount(1);
        res = update_stmt.execute();
        }
    return 'Done';
   $$

{% endset %}

{% set results = run_query(function_sql_string) %}

{% if execute %}
    {% do log("Success", info=True) %}
{% else %}
    {% do log("Failure", info=True) %}
{% endif %}

{% endmacro %}

{% macro call_proc_graph_network(output_table) -%}

    COMMIT;
    CALL MERGE.PROC_GRAPH_NETWORK('{{output_table}}');
    COMMIT;

{% endmacro %}