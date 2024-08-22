{% macro clean_default(column) %}
   
    NULLIF(TRIM({{column}}), '')

{% endmacro %}

{% macro clean_hotel_name(hotel) %}

    NULLIF(
        IFF(
            RLIKE( {{ hotel }} , (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ hotel }}' ), 'i')
            , NULL
            , REGEXP_REPLACE(
                IFNULL(
                    NULLIF(LEFT(UPPER({{ hotel }}), POSITION('T/A', UPPER({{ hotel }}), 1)-1 ), '')
                    , UPPER({{ hotel }}) )             
                , '[^a-zA-Z0-9]+|HOTEL,|HOTELS|Z_|HOTEL CURIO'
                , '') )
        , '')

{% endmacro %}

/*This macro is used to cleanse the company name and also used to exclude the companies using exclude data*/
{% macro clean_company_name(company) %}

    NULLIF(
        IFF(
            RLIKE( {{ company }} , (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ company }}' ), 'i')
            , NULL
            , REGEXP_REPLACE(
                IFNULL(
                    NULLIF(LEFT(UPPER({{ company }}), POSITION('T/A', UPPER({{ company }}), 1)-1 ), '')
                    , UPPER({{ company }}) )             
                , '[^a-zA-Z0-9]+|LIMITED|LTD|PLC|LLP|CIC|COMMUNITY INTEREST COMPANY|P.L.C|P L C'
                , '') )
        , '')

{% endmacro %}

/*This macro is used to cleanse the website name and also used to exclude the website using exclude data*/
{% macro clean_website(website) %}

    NULLIF(
        IFF(
            RLIKE(SPLIT_PART(UPPER(REGEXP_REPLACE({{ website }}, 'www.|https|http|://', '')), '.', 0), (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ website }}'), 'i')
            , NULL
            , SUBSTRING(
                REGEXP_REPLACE(REPLACE(UPPER({{ website }}), ' ', ''), 'WWW.|HTTPS|HTTP|://', '')
                , 1
                , CHARINDEX('/', REGEXP_REPLACE(REPLACE(UPPER({{ website }}||'/'), ' ', ''), 'WWW.|HTTPS|HTTP|://', '')) -1) )
        , '') 
        
{% endmacro %}


/*This macro is used to cleanse the email address and also used to exclude the website using exclude data*/
{% macro clean_email(email) %}

    NULLIF(
        NULLIF(
            IFF(
                RLIKE(split_part({{ email }}, '@', 2), (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ email }}'), 'i')
                , NULL
                , REPLACE(UPPER({{ email }}), ' ', '') )
            , '')
        , 'GDPRANONYMISED')

{% endmacro %}


/*This macro is used to get the domain from email address*/
{% macro extract_domain_from_email(email) %}

    RIGHT(UPPER({{ email }}), LEN({{ email }}) - REGEXP_INSTR({{ email }}, '@'))

{% endmacro %}

/*This macro is used to cleanse the Telephone Number and also used to exclude the Numbers using exclude data*/
{% macro clean_telephone(telephone) %}

    NULLIF(
        IFF(
            RLIKE( REPLACE(ltrim({{ telephone }}, '0'), ' ', ''), (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ telephone }}'), 'i')
            , NULL
            , REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REPLACE(
                        IFF(
                            LEN({{ telephone }}) > 1 AND LEN(REPLACE({{ telephone }}, LEFT({{ telephone }}, 1), '')) = 0
                            , ''
                            , IFF(PREP.REXP({{ telephone }}, '^(.)\\1\\1\\1', 'i'), '', {{ telephone }}) )
                        , ' '
                        , '' )
                    , '[^0-9]'
                    , '' )
                , '^(0|\\+44|44|0044)'
                , '') )
        ,'')

{% endmacro %}

{% macro clean_address(address_line_1, postcode) %}

    NULLIF(
        IFF(
            RLIKE( REPLACE({{ postcode }}, ' ', ''), (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ address }}'), 'i')
            , NULL
            , REGEXP_REPLACE(
                REPLACE(LEFT(REPLACE( UPPER({{ address_line_1 }}),' ', ''), 20), 'C/O', '')||REPLACE(UPPER({{ postcode }}), ' ', '')
                , '[^0-9A-Z -]'
                , '') ) 
        , '')

{% endmacro %}


{% macro clean_postcode(postcode) %}

    NULLIF(
        IFF(
            RLIKE(REPLACE({{ postcode }}, ' ', ''), (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ postcode }}'), 'i')
            , NULL
            , REGEXP_REPLACE(REPLACE(UPPER({{ postcode }}), ' ', ''), '[^0-9A-Z -]', '') ) 
        ,'')
{% endmacro %}

{% macro clean_contact_name(full_name) %}

    NULLIF(
        IFF(
            RLIKE(UPPER(REGEXP_REPLACE(REPLACE(UPPER({{ full_name }}), ' ', ''), '[^A-Z]', '')), (SELECT LISTAGG(values_to_exclude, '|') FROM {{ ref('exclude_column_values') }} WHERE column_name = '{{ full_name }}'), 'i')
            , NULL
            , {%if soundexY=='y' %} SOUNDEX( {% endif%} 
                REGEXP_REPLACE(REPLACE(UPPER({{ full_name }}), ' ', ''), '[^A-Z]', '')
              {% if soundexY=='y' %} ) {% endif %} )
        , '')

{% endmacro %}

{% macro clean_company_registration_number(company_registration_number) %}
   
    NULLIF(
        REPLACE(
            LTRIM(company_registration_number, '0')
            , ' '
            , '')
        , '')  

{% endmacro %}

{% macro remove_blankspaces(column) %}
   
    REGEXP_REPLACE(REGEXP_REPLACE({{column}}, '[\\s]'), '')

{% endmacro %}


{% macro remove_special_characters(column) %}
   
    REGEXP_REPLACE(REGEXP_REPLACE({{column}}, '[^\\w]'), '')

{% endmacro %}

{% macro detault_cleanse_match_value(column) %}

    UPPER(REGEXP_REPLACE(REGEXP_REPLACE({{ column }}, '[^a-zA-Z0-9]'), ''))

{% endmacro %}