{{ config(
    schema='raw',
    materialized='incremental',
    unique_key='ID',
    incremental_strategy='merge',
    merge_update_columns=['PERSON_NAME', 'STREET', 'CITY_ID', '_AIRBYTE_AB_ID',
                          '_AIRBYTE_EMITTED_AT', '_AIRBYTE_NORMALIZED_AT',
                          '_AB_CDC_LSN', '_AB_CDC_UPDATED_AT']
) }}

/*
    CDC-based addresses_raw table (Incremental)
    - Process only new or updated records from addresses_cdc
    - Automatically handles INSERT, UPDATE, DELETE operations
    - Uses LSN to track changes incrementally
*/

WITH latest_cdc_records AS (
    SELECT
        ID,
        PERSON_NAME,
        STREET,
        CITY_ID,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        _AIRBYTE_NORMALIZED_AT,
        _AIRBYTE_ADDRESSES_HASHID,
        _AB_CDC_LSN,
        _AB_CDC_UPDATED_AT,
        _AB_CDC_DELETED_AT,
        _AB_CDC_LOG_POS,
        OP
    FROM {{ source('dev', 'addresses_cdc') }}

    {% if is_incremental() %}
    -- Only process records with LSN greater than the max LSN in target table
    WHERE _AB_CDC_LSN > (SELECT COALESCE(MAX(_AB_CDC_LSN), 0) FROM {{ this }})
    {% endif %}
),

deduplicated_records AS (
    SELECT
        ID,
        PERSON_NAME,
        STREET,
        CITY_ID,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        _AIRBYTE_NORMALIZED_AT,
        _AB_CDC_LSN,
        _AB_CDC_UPDATED_AT,
        _AB_CDC_DELETED_AT,
        OP
    FROM latest_cdc_records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ID
        ORDER BY _AB_CDC_LSN DESC, _AB_CDC_UPDATED_AT DESC
    ) = 1
)

SELECT
    ID,
    PERSON_NAME,
    STREET,
    CITY_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    _AIRBYTE_NORMALIZED_AT,
    _AB_CDC_LSN,
    _AB_CDC_UPDATED_AT
FROM deduplicated_records
WHERE _AB_CDC_DELETED_AT IS NULL  -- Exclude deleted records