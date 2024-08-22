/* 
Searches for repeated charachters - indicative of dummy/test values being entered - in a string
https://community.snowflake.com/s/question/0D50Z00009T5yVYSAZ/hi-everyone-i-need-your-help-with-a-regex-formula-in-snowflake-i-
looked-around-a-lot-but-i-couldnt-find-any-solution-im-looking-for-a-formula-that-matches-names-that-contain-letters-repeated-multiple-times-at-least-3-times
*/

{% macro create_rexp_function() %}
    
    {% set function_sql_string %}
        CREATE SCHEMA IF NOT EXISTS PREP;
        CREATE OR REPLACE FUNCTION PREP.REXP(subject TEXT, pattern TEXT, parameters TEXT)
            RETURNS BOOLEAN
            LANGUAGE JAVASCRIPT
            AS
            $$
                rx = new RegExp(PATTERN, PARAMETERS);
                return rx.test(SUBJECT);
            $$;
    {% endset %}

    {% set results = run_query(function_sql_string) %}

{% endmacro %}