{# Use the schema named in dbt_project.yml exactly (silver, gold) instead of dbt's default
   "<target schema>_<custom schema>" pattern. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
