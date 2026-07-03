{{ config(
    pre_hook="SET streaming_parallelism = 2"
) }}

CREATE TABLE {{ this }} (
    id BIGINT,
    material_id BIGINT,
    workspace_id BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    status SMALLINT,
    is_open_vial SMALLINT,
    is_addremove SMALLINT,
    created_by BIGINT,
    updated_by BIGINT,
    deleted_by BIGINT,
    __deleted STRING,
    PRIMARY KEY (id)
)
INCLUDE timestamp AS kafka_timestamp
WITH (
    connector = 'kafka',
    topic = '{{ env_var("TOPIC_PREFIX") }}material_workspaces',
    properties.bootstrap.server = '{{ env_var("DEBEZIUM_HOSTNAME") }}:{{ env_var("DEBEZIUM_PORT") | int }}',
    scan.startup.mode = 'earliest',
    source_rate_limit = 7000
) FORMAT PLAIN ENCODE JSON;
