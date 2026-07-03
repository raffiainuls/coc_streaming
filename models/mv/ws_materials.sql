{{ config(
    materialized='risingwave_mv',
    source_rate_limit=1000,
    primary_key='id',
    backfill_rate_limit=2000,
    pre_hook="SET streaming_parallelism = 2"
) }}

SELECT
    mw.id AS id,
    m.id AS global_id,
    mr.parent_material_id AS parent_global_id,
    p.id AS parent_id,
    mw.workspace_id AS program_id,
    mw.status AS status,
    mw.is_open_vial AS is_open_vial,
    mw.is_addremove AS is_addremove,
    m.is_stock_opname_mandatory AS material_is_stock_opname_mandatory,
    m.name AS name,
    m.description AS description,
    m.material_level_id AS material_level_id,
    m.code AS code,
    m.hierarchy_code AS hierarchy_code,
    m.unit_of_consumption_id AS unit_of_consumption_id,
    cu.name AS unit_of_consumption,
    du.name AS unit_of_distribution,
    m.unit_of_distribution_id AS unit_of_distribution_id,
    m.consumption_unit_per_distribution_unit AS consumption_unit_per_distribution_unit,
    m.is_temperature_sensitive AS is_temperature_sensitive,
    m.min_retail_price AS min_retail_price,
    m.max_retail_price AS max_retail_price,
    m.min_temperature AS min_temperature,
    m.max_temperature AS max_temperature,
    m.material_type_id AS material_type_id,
    mt.name AS material_type,
    m.is_managed_in_batch AS is_managed_in_batch,
    mw.created_by AS created_by,
    mw.updated_by AS updated_by,
    mw.deleted_by AS deleted_by,
    m.created_at AS created_at,
    m.updated_at AS updated_at,
    m.deleted_at AS deleted_at
FROM
    {{ ref("material_workspaces") }} mw
JOIN {{ ref("materials") }} m ON
    (m.id = mw.material_id)
LEFT JOIN {{ ref("material_relations") }} mr ON
    (m.id = mr.child_material_id AND mr.__deleted = 'false' AND mr.deleted_at IS NULL)
LEFT JOIN {{ ref("material_workspaces") }} p ON
    (p.material_id = mr.parent_material_id)
        AND (p.workspace_id = mw.workspace_id)
JOIN {{ ref("material_units") }} cu ON
    (cu.id = m.unit_of_consumption_id)
JOIN {{ ref("material_units") }} du ON
    (du.id = m.unit_of_distribution_id)
JOIN {{ ref("material_types") }} mt ON
    (mt.id = m.material_type_id)
