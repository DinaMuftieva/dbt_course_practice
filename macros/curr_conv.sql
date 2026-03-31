{% macro curr_conv(column_name, scale = 2) %}
    ({{column_name}} / 100)::numeric(16, {{scale}})
{% endmacro %}