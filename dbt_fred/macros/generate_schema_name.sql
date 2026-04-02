/*
    macros/generate_schema_name.sql

    Override dbt's default schema naming behavior.
    
    Without this macro, dbt would create schemas like:
        STAGING_STAGING, STAGING_INTERMEDIATE, STAGING_MARTS
    
    With this macro, dbt writes directly to:
        STAGING, INTERMEDIATE, MARTS
    
    This is a standard dbt pattern in most production projects.
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}