{% set current_date = run_started_at | string | truncate(10, true, "") %}
{% set current_year = run_started_at | string | truncate(4, true, "") | int %}
{% set prev_year = current_year - 20 %}
SELECT
    count(*) as {{ adapter.quote('cnt')}} --adapter.quote для кавычек, подходящей для любой бд
FROM
    {{ ref('fct_flights') }}
Where
    scheduled_departure between '{{current_date | replace(current_year, prev_year) }}'::timestamptz and '{{ current_date }}'::timestamptz
    --scheduled_departure between '2017-08-31'::timestamptz and '2026-03-30'::timestamptz
{#
--получить инфу по модели
{%-
    set source_relation = adapter.get_relation(
        database = "dwh_flights",
        schema = "intermediate",
        identifier = "fct_flights"
    )
%}

{{ source_relation.database }}
{{ source_relation.schema }}
{{ source_relation.identifier }}
{{ source_relation.is_table }}
{{ source_relation.is_view }}
{{ source_relation.is_cte }}
#}

{#
--получить инфу по модели
{% set source_relation = load_relation(ref("fct_flights")) %}

{{ source_relation.database }}
{{ source_relation.schema }}
{{ source_relation.identifier }}
{{ source_relation.is_table }}
{{ source_relation.is_view }}
{{ source_relation.is_cte }}
#}
{#
--получить инфу колонкам модели
{%- set source_relation = api.Relation.create(
        database = "dwh_flights",
        schema = "intermediate",
        identifier = "fct_flights",
        type = "table"
    )
%}

{% set columns = adapter.get_columns_in_relation(source_relation) %}

{% for column in columns -%}
    {{'Columns: ' ~ column }}
{%endfor%}
#}
{#
--создание схемы из dbt
{% do adapter.create_schema(
    api.Relation.create (
        database = "dwh_flights",
        schema = "test_schema"
    )
)
%}
#}
{#
--удаление схемы из dbt
{% do adapter.drop_schema(
    api.Relation.create (
        database = "dwh_flights",
        schema = "test_schema"
    )
)
%}
#}
{#
--получить разницу в колонках между двумя моделями
{%- set fct_flights = api.Relation.create(
        database = "dwh_flights",
        schema = "intermediate",
        identifier = "fct_flights",
        type = "table"
    )
%}

{%- set stg_flights__flights = api.Relation.create(
        database = "dwh_flights",
        schema = "intermediate",
        identifier = "stg_flights__flights",
        type = "table"
    )
%}

{% for column in adapter.get_missing_columns(stg_flights__flights, fct_flights) %}
    {{'Columns: ' ~ column }}
{%endfor%}
#}
{#
--расширить поля одной модели, чтобы туда вмещались значения из полей второй модели
{%- set fct_flights = api.Relation.create(
        database = "dwh_flights",
        schema = "intermediate",
        identifier = "fct_flights",
        type = "table"
    )
%}

{%- set stg_flights__flights = api.Relation.create(
        database = "dwh_flights",
        schema = "intermediate",
        identifier = "stg_flights__flights",
        type = "table"
    )
%}

{% do adapter.expand_target_column_types (stg_flights__flights, fct_flights) %}

{%- for column in adapter.get_columns_in_relation(stg_flights__flights) %}
    {{'Columns: ' ~ column }}
{%- endfor %}

{% for column in adapter.get_columns_in_relation(fct_flights) %}
    {{'Columns: ' ~ column }}
{%- endfor %}
#}
