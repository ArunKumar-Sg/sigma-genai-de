-- macros/generate_schema_name.sql
-- Override: always use the target schema from profiles.yml directly.
-- Without this, dbt appends the custom schema name → PUBLIC_DEV which doesn't exist.
-- With this, dbt writes all models into SIGMA_DE.PUBLIC where STUDENT_CORTEX has USAGE.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
