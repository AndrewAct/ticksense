-- Use the custom schema name as-is (no target.schema prefix).
-- This keeps Trino schemas clean: iceberg.staging.*, iceberg.marts.*
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
