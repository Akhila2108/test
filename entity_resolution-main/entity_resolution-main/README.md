# Entity resolution
TBD

## Pre-requisites

* Update `sources.yml` to read the source data
* Update `dbt_project.yml` to update global variables
* Update `seeds` folder  to update `csv` files

### Global variables - `dbt_project.yml`

#### Default variables (optional to update)
| Variable name  | Datatype | Default value | Purpose  |
|---|---|---|---|
| default_entity_name | string | xxxxxxxxxxxxxxxxxxx  | Default entity name  |
| default_entity_group_id  | number | 1000000000000000  | Default entity group id |
| default_entity_top_group_id  | number | 50  | Default start index of entity group id  |
| default_varchar_length  | number | 200  | Default string column physical length  |

#### Source locations (required to update)
| Variable name  | Datatype | Default value | Purpose  |
|---|---|---|---|
| meta_source_entity | string | meta_source_entity  | Mapping file to update column mapping to create integrated table in prep layer  |
| meta_entity_match_key_values | string | meta_entity_match_key_values  | Meta data to define the data type, cleanse rule, match value macro to apply, weightage  |
| meta_source_columns | string | meta_source_columns  | Meta data to define data type, cleanse rule, match value **will be deprecated** in future release and merged with `meta_entity_match_key_values`   |
| min_data_loaded_at  | string | 1900-01-01  | (optional) Only applicable for incremental approach |
| data_loaded_at  | string | null  | (optional) Only applicable for incremental approach |

#### Default variables
| Variable name  | Datatype | Default value | Purpose  |
|---|---|---|---|
| threshold_merge  | number | 6  | Accepted threshold for merge  |
| threshold_possible_merge | number | 5  | Accepted threshold for possible merge  |
| threshold_total_match_score | number | 0.7  | Accepted threshold for total score  |
| avoid_dummy_values_count_thresold | number | 20000 | To avoid dummy record counts while matching |

### Seeds

#### config_table_sources.csv

To map the data from multiple source tables into unified structure

| Column             | Is required? | Data type | Purpose                                                                                                   |
| ------------------ | ------------ | --------- | --------------------------------------------------------------------------------------------------------- |
| record_source      | Y            | VARCHAR   | Source name from sources.yml to read the data in prep layer                                               |
| table_source       | Y            | VARCHAR   | Source table name in Snowflake to read the data in prep layer                                             |
| priority           | Y            | INTEGER   | Source to prioritise in survivorship rule                                                                 |
| entity_natural_key | Y            | VARCHAR   | Primary key column of source table                                                                        |
| entity_name        | Y            | VARCHAR   | Entity name column for fuzzy match                                                                        |
| data_loaded_at     | N            | VARCHAR   | Data column to load data incrementally for match & merge process. Only applicable for incremental process |

#### Best practices 
* CSV files supports SQL expression, if you want to concat, format, type cast - you can have SQL expression as column value
* If specific source does not appropriate column for unified view, please use `x` as value, to know more please review sample file at `seeds/mapping/mapping_sources_entity.csv`

#### config_required_fields_and_rules.csv

To define the cleanse rule to apply and data type for columns

| column              | Is required? | data_type | purpose                                                                                                    |
| ------------------- | ------------ | --------- | ---------------------------------------------------------------------------------------------------------- |
| column_name         | Y            | VARCHAR   | Physical column name                                                                                       |
| data_type           | Y            | VARCHAR   | Data type of column                                                                                        |
| cleanse_rule        | Y            | VARCHAR   | default, remove_blankspace, crn                                                                            |
| cleanse_match_value | Y            | VARCHAR   | Define what macro we should apply to get match_value. Ex: company_name, crn, website, email, mobile_number |

file location `seeds/config/config_required_field_and_rules.csv`

#### config_match_values.csv

To define the what match rule to apply, weightage score, match method for columns

| column              | Is required? | data_type | purpose                                                                                                    |
| ------------------- | ------------ | --------- | ---------------------------------------------------------------------------------------------------------- |
| match_type          | Y            | VARCHAR   | Define quick_match, complex_match                                                                          |
| column_name         | Y            | VARCHAR   | Physical column name                                                                                       |
| match_value         | Y            | VARCHAR   | its used to check 2 columns with one field (i.e, web address domain and email address domain)              |
| match_method        | Y            | INTEGER   | default weightage score                                                                                    |
| match_weighting     | Y            | INTEGER   | Field weightning                                                                                           |
| match_diff_max      | Y            | FLOAT     | maximum allowed difference between the matching                                                            |

Sample file `seeds/meta_data/config_match_values.csv`

## Sequence of execution

### Pre-requisites
````
dbt run-operation drop_schemas  -- To drop schema if exists to clean up (run when required)
dbt run-operation create_rexp_function  -- Schema should exist
dbt run-operation create_proc_graph_network -- Schema should exist
dbt seed --full-refresh
````

### Prep
````
dbt run -m 01_prep
````
or

````
dbt run -m entity
dbt run -m prep_entity
dbt run -m entity_match_node
````

### Core
````
dbt run -m 02_core
````
or
````
dbt run -m master_entity # Run it as one-off
dbt run -m source_entity
````

### Audit
````
dbt run -m 03_audit
````
or
````
dbt run -m audit_match_quick # Run it as one-off
dbt run -m audit_match_complex # Run it as one-off
dbt run -m audit_graph_edge
````

### Match
````
dbt run -m 04_match
````
or
````
dbt run -m quick_match
dbt run -m complex_match
````

### Graph
````
dbt run -m 05_graph
````
or
````
dbt run -m graph_edge_in
dbt run -m graph_edge_out
````

### Merge
````
dbt run -m 06_merge
````
or
````
dbt run -m merge_fix
dbt run -m merge_match
dbt run -m merge_unmatch
````

### Golden record
````
dbt run -m ssot
````

## Constraints

* Columns in source tables should not have whitespace between characters or double quotes, currently `mapping_sources_entity.csv` does not support for columns with whitespace between characters. Please rename at source tables in Snowflake before run `prep` layer
* `mapping_entity_match_key_values.csv` should have atleast 1 row for complex match

### Technical constraints

* Only support for Snowflake