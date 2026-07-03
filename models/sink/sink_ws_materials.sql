{{ config(
    pre_hook="SET streaming_parallelism = 2"
) }}

CREATE SINK {{ this }} AS
SELECT
    CAST(s.id AS BIGINT),
    CAST(s.global_id AS BIGINT),
    CAST(s.parent_global_id AS BIGINT),
    CAST(s.parent_id AS BIGINT),
    CAST(s.program_id AS BIGINT),
    CAST(s.status AS SMALLINT),
    CAST(s.is_open_vial AS SMALLINT),
    CAST(s.is_addremove AS SMALLINT),
    CAST(s.material_is_stock_opname_mandatory AS SMALLINT) AS is_stock_opname_mandatory,
    CAST(s.name AS TEXT),
    CAST(s.description AS TEXT),
    CAST(s.material_level_id AS BIGINT),
    CAST(s.code AS TEXT),
    CAST(s.hierarchy_code AS TEXT),
    CAST(s.unit_of_consumption_id AS BIGINT),
    CAST(s.unit_of_consumption AS TEXT),
    CAST(s.unit_of_distribution AS TEXT),
    CAST(s.unit_of_distribution_id AS BIGINT),
    CAST(s.consumption_unit_per_distribution_unit AS INTEGER),
    CAST(s.is_temperature_sensitive AS BOOLEAN),
    CAST(s.min_retail_price AS DOUBLE PRECISION),
    CAST(s.max_retail_price AS DOUBLE PRECISION),
    CAST(s.min_temperature AS DOUBLE PRECISION),
    CAST(s.max_temperature AS DOUBLE PRECISION),
    CAST(s.material_type_id AS BIGINT),
    CAST(s.material_type AS TEXT),
    CAST(s.is_managed_in_batch AS INT),
    CAST(s.created_by AS BIGINT),
    CAST(s.updated_by AS BIGINT),
    CAST(s.deleted_by AS BIGINT),
    TO_CHAR(s.created_at, 'YYYY-MM-DDTHH24:MI:SSZ') AS created_at,
    TO_CHAR(s.updated_at, 'YYYY-MM-DDTHH24:MI:SSZ') AS updated_at,
    TO_CHAR(s.deleted_at, 'YYYY-MM-DDTHH24:MI:SSZ') AS deleted_at
FROM
    {{ ref("ws_materials") }} s
WITH (
    connector = 'kafka',
    properties.bootstrap.server = '{{ env_var("DEBEZIUM_HOSTNAME") }}:{{ env_var("DEBEZIUM_PORT") | int }}',
    topic = '{{ env_var("TOPIC_PREFIX") }}ws_materials',
    primary_key = 'id',
    backfill_rate_limit = 1000
) FORMAT PLAIN ENCODE JSON (force_append_only = 'true');
