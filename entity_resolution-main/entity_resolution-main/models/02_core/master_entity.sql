{{ 
    config(
        tags=["core", "master_entity"],
        materialized="incremental"
    )
}}

-- Creates empty table required for dependencies to operate properly
-- Populated later in the process by macro: update_master_entity()
{{ generate_default_skeleton_entity_model('master_entity') }}