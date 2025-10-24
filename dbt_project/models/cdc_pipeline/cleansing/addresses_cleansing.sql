{{ config(
    schema='cleansing',
    materialized='incremental',
    unique_key='ID',
    incremental_strategy='merge',
    merge_update_columns=['PERSON_NAME', 'STREET', 'CITY_ID',
                          '_AB_CDC_LSN', '_AB_CDC_UPDATED_AT']
) }}

/*
    Silver Layer: addresses_cleansing
    - Remove null/invalid data from raw layer
    - Standardize data formats
    - Apply business rules
*/

WITH raw_data AS (
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
    FROM {{ ref('addresses_raw') }}

    {% if is_incremental() %}
    WHERE _AB_CDC_LSN > (SELECT COALESCE(MAX(_AB_CDC_LSN), 0) FROM {{ this }})
    {% endif %}
),

cleansed_data AS (
    SELECT
        ID,
        -- Clean person name: trim whitespace, remove multiple spaces
        TRIM(REGEXP_REPLACE(PERSON_NAME, '\\s+', ' ')) AS PERSON_NAME,
        -- Clean street address: trim and standardize
        TRIM(REGEXP_REPLACE(STREET, '\\s+', ' ')) AS STREET,
        CITY_ID,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        _AIRBYTE_NORMALIZED_AT,
        _AB_CDC_LSN,
        _AB_CDC_UPDATED_AT,
        CURRENT_TIMESTAMP() AS CLEANSED_AT
    FROM raw_data
    WHERE
        -- Remove records with null required fields
        ID IS NOT NULL
        AND PERSON_NAME IS NOT NULL
        AND TRIM(PERSON_NAME) != ''
        AND STREET IS NOT NULL
        AND TRIM(STREET) != ''
        AND CITY_ID IS NOT NULL
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
    _AB_CDC_UPDATED_AT,
    CLEANSED_AT
FROM cleansed_data