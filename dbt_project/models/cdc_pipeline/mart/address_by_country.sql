{{ config(
    schema='mart',
    materialized='table'
) }}

/*
    Gold Layer: address_by_country
    - Country-level aggregated statistics
    - Executive dashboard metrics
*/

WITH city_data AS (
    SELECT
        CITY_ID,
        CITY_NAME,
        COUNTRY,
        TOTAL_ADDRESSES,
        UNIQUE_PERSONS
    FROM {{ ref('city_address_summary') }}
)

SELECT
    COUNTRY,
    COUNT(DISTINCT CITY_ID) AS TOTAL_CITIES,
    SUM(TOTAL_ADDRESSES) AS TOTAL_ADDRESSES,
    SUM(UNIQUE_PERSONS) AS TOTAL_PERSONS,
    ROUND(SUM(TOTAL_ADDRESSES) * 1.0 / COUNT(DISTINCT CITY_ID), 2) AS AVG_ADDRESSES_PER_CITY,
    ROUND(SUM(UNIQUE_PERSONS) * 1.0 / COUNT(DISTINCT CITY_ID), 2) AS AVG_PERSONS_PER_CITY,
    CURRENT_TIMESTAMP() AS REPORT_GENERATED_AT
FROM city_data
GROUP BY COUNTRY
ORDER BY TOTAL_ADDRESSES DESC