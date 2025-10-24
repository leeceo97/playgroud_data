{{ config(
    schema='cleansing',
    materialized='incremental',
    unique_key='ID',
    incremental_strategy='merge',
    merge_update_columns=['CITY_NAME', 'COUNTRY',
                          '_AB_CDC_LSN', '_AB_CDC_UPDATED_AT']
) }}

/*
    Silver Layer: cities_cleansing
    - Remove null/invalid data from raw layer
    - Standardize data formats
    - Apply business rules
*/

WITH raw_data AS (
    SELECT
        ID,
        CITY_NAME,
        COUNTRY,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        _AIRBYTE_NORMALIZED_AT,
        _AB_CDC_LSN,
        _AB_CDC_UPDATED_AT
    FROM {{ ref('cities_raw') }}

    {% if is_incremental() %}
    WHERE _AB_CDC_LSN > (SELECT COALESCE(MAX(_AB_CDC_LSN), 0) FROM {{ this }})
    {% endif %}
),

cleansed_data AS (
    SELECT
        ID,
        -- Clean city name: trim whitespace, title case
        INITCAP(TRIM(REGEXP_REPLACE(CITY_NAME, '\\s+', ' '))) AS CITY_NAME,
        -- Clean country name: trim and title case
        INITCAP(TRIM(REGEXP_REPLACE(COUNTRY, '\\s+', ' '))) AS COUNTRY,
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
        AND CITY_NAME IS NOT NULL
        AND TRIM(CITY_NAME) != ''
        AND COUNTRY IS NOT NULL
        AND TRIM(COUNTRY) != ''
)

SELECT
    ID,
    CITY_NAME,
    COUNTRY,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    _AIRBYTE_NORMALIZED_AT,
    _AB_CDC_LSN,
    _AB_CDC_UPDATED_AT,
    CLEANSED_AT
FROM cleansed_data