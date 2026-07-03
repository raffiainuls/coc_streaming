{{ config(
    pre_hook="SET streaming_parallelism = 2"
) }}

CREATE TABLE {{ this }} (
    id BIGINT,
    name STRING,
    description STRING,
    material_level_id BIGINT,
    code STRING,
    hierarchy_code STRING,
    unit_of_consumption_id BIGINT,
    unit_of_distribution_id BIGINT,
    consumption_unit_per_distribution_unit INT,
    is_temperature_sensitive BOOLEAN,
    min_retail_price DOUBLE,
    max_retail_price DOUBLE,
    min_temperature DOUBLE,
    max_temperature DOUBLE,
    material_type_id BIGINT,
    is_managed_in_batch INT,
    status BOOLEAN,
    created_by BIGINT,
    updated_by BIGINT,
    deleted_by BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    is_stock_opname_mandatory INT,
    __deleted STRING,
    PRIMARY KEY (id)
)
INCLUDE timestamp AS kafka_timestamp
WITH (
    connector = 'kafka',
    topic = '{{ env_var("TOPIC_PREFIX") }}materials',
    properties.bootstrap.server = '{{ env_var("DEBEZIUM_HOSTNAME") }}:{{ env_var("DEBEZIUM_PORT") | int }}',
    scan.startup.mode = 'earliest',
    source_rate_limit = 7000
) FORMAT PLAIN ENCODE JSON;
