{{ config(
    schema='mart',
    materialized='table'
) }}

/*
    Gold Layer: person_location_detail
    - Detailed person and location information
    - Denormalized for easy querying
    - Used for operational reports
*/

WITH addresses AS (
    SELECT
        ID AS ADDRESS_ID,
        PERSON_NAME,
        STREET,
        CITY_ID,
        _AB_CDC_LSN AS ADDRESS_LSN,
        _AB_CDC_UPDATED_AT AS ADDRESS_UPDATED_AT,
        CLEANSED_AT AS ADDRESS_CLEANSED_AT
    FROM {{ ref('addresses_cleansing') }}
),

cities AS (
    SELECT
        ID AS CITY_ID,
        CITY_NAME,
        COUNTRY,
        _AB_CDC_LSN AS CITY_LSN,
        _AB_CDC_UPDATED_AT AS CITY_UPDATED_AT
    FROM {{ ref('cities_cleansing') }}
)

SELECT
    a.ADDRESS_ID,
    a.PERSON_NAME,
    a.STREET,
    c.CITY_NAME,
    c.COUNTRY,
    -- Concatenated full address
    a.STREET || ', ' || c.CITY_NAME || ', ' || c.COUNTRY AS FULL_ADDRESS,
    -- Metadata
    a.ADDRESS_LSN,
    a.ADDRESS_UPDATED_AT,
    c.CITY_LSN,
    c.CITY_UPDATED_AT,
    -- Data quality flags
    CASE
        WHEN LENGTH(a.PERSON_NAME) < 3 THEN 'SHORT_NAME'
        WHEN LENGTH(a.STREET) < 5 THEN 'SHORT_ADDRESS'
        ELSE 'OK'
    END AS DATA_QUALITY_FLAG,
    CURRENT_TIMESTAMP() AS REPORT_GENERATED_AT
FROM addresses a
INNER JOIN cities c
    ON a.CITY_ID = c.CITY_ID
ORDER BY c.COUNTRY, c.CITY_NAME, a.PERSON_NAME