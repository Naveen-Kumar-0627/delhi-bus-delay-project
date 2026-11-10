{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}

    {# We add a check to make sure 'node' exists before looking at 'node.path' #}
    {%- elif node is not none and 'bronze' in node.path | lower -%}
        bronze

    {%- elif node is not none and 'silver' in node.path | lower -%}
        silver

    {%- elif node is not none and 'gold' in node.path | lower -%}
        gold

    {%- else -%}
        {{ target.schema }}
    {%- endif -%}

{%- endmacro %}