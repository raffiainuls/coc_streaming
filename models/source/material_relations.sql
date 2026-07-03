{{ config(
    pre_hook="SET streaming_parallelism = 2"
) }}

CREATE TABLE {{ this }} (
    id BIGINT,
    child_material_id BIGINT,
    parent_material_id BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    __deleted STRING,
    PRIMARY KEY (id)
)
INCLUDE timestamp AS kafka_timestamp
WITH (
    connector = 'kafka',
    topic = '{{ env_var("TOPIC_PREFIX") }}material_relations',
    properties.bootstrap.server = '{{ env_var("DEBEZIUM_HOSTNAME") }}:{{ env_var("DEBEZIUM_PORT") | int }}',
    scan.startup.mode = 'earliest',
    source_rate_limit = 7000
) FORMAT PLAIN ENCODE JSON;
