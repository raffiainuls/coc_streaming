{# Materialization custom: CREATE MATERIALIZED VIEW di RisingWave.
   Drop-recreate setiap run, dengan opsi rate limit untuk mengontrol
   kecepatan backfill/konsumsi source. Diadaptasi dari dbt-risingwave-v5. #}

{% materialization risingwave_mv, default %}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}

  {% set source_rate_limit = config.get('source_rate_limit') %}
  {% set backfill_rate_limit = config.get('backfill_rate_limit') %}
  {% set backfill_order = config.get('backfill_order') %}

  {% if existing_relation %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% set build_sql %}
    CREATE MATERIALIZED VIEW {{ target_relation }}
    {% if source_rate_limit or backfill_rate_limit or backfill_order %}
    WITH (
    {%- if source_rate_limit -%}
      source_rate_limit = {{ source_rate_limit }}
      {%- if backfill_rate_limit -%},{%- endif -%}
    {%- endif -%}
    {%- if backfill_rate_limit -%}
      backfill_rate_limit = {{ backfill_rate_limit }}
    {%- endif -%}
    {%- if backfill_order -%}
      {%- if backfill_rate_limit or source_rate_limit -%},{%- endif -%}
      backfill_order = {{ backfill_order }}
    {%- endif -%}
    )
    {% endif %}
    AS (
    {{ sql }}
    )
  {% endset %}

  {% call statement('main') -%}
    {{ build_sql }}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
