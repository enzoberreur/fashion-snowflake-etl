{#
  Use the custom schema name as-is (STAGING, MARTS) instead of dbt's default
  "<target_schema>_<custom>" (which would be INGEST_STAGING and require
  CREATE SCHEMA on the database). The bare schemas are pre-created by
  sql/ddl/00_database_setup.sql and the INGEST role has CREATE TABLE/VIEW on them.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
