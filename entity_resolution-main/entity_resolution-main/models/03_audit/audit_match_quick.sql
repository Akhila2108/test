{{ 
    config(
        tags=["audit", "init_audit"],
        materialized="incremental"
    ) 
}}

-- Creates empty table required for dependencies to operate properly
-- Populated later in the process by macro: update_quick_match_audit
{{ generate_default_skeleton_entity_model('audit_match_quick') }}