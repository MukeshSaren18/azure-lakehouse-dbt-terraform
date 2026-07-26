-- ─────────────────────────────────────────────────────────────────────────────
-- Macro: safe_divide
-- Returns NULL instead of raising divide-by-zero errors
-- Usage: {{ safe_divide('numerator_col', 'denominator_col') }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then null
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- Macro: days_between
-- Returns integer number of days between two timestamp columns
-- Usage: {{ days_between('created_at', 'updated_at') }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro days_between(start_col, end_col) %}
    datediff(day, date({{ start_col }}), date({{ end_col }}))
{% endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- Macro: rolling_sum
-- Returns a rolling sum over a configurable window (days)
-- Usage: {{ rolling_sum('revenue_eur', 'order_date', 30) }}
-- ─────────────────────────────────────────────────────────────────────────────
{% macro rolling_sum(measure_col, date_col, days) %}
    sum({{ measure_col }}) over (
        partition by customer_id
        order by {{ date_col }}
        range between interval {{ days }} days preceding and current row
    )
{% endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- Macro: generate_schema_name
-- Override: respects target-specific schema prefixes (dev vs prod isolation)
-- Default dbt behaviour can cause schema collisions in shared workspaces.
-- ─────────────────────────────────────────────────────────────────────────────
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# In prod: use the custom schema directly (no prefix) #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In dev/local: prefix with default_schema to isolate per developer #}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


-- ─────────────────────────────────────────────────────────────────────────────
-- Macro: audit_log
-- Logs row counts and column stats to an audit table after each model run.
-- Called via post-hook in dbt_project.yml for Gold layer models.
-- ─────────────────────────────────────────────────────────────────────────────
{% macro audit_log(model_name) %}
    insert into {{ target.schema }}.dbt_audit_log
    select
        '{{ model_name }}'                          as model_name,
        '{{ target.name }}'                         as environment,
        current_timestamp()                         as run_at,
        (select count(*) from {{ ref(model_name) }}) as row_count
{% endmacro %}
