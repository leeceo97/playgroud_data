{{ config(
    schema='mart',
    materialized='table'
) }}

/*
    Gold Layer: city_address_summary
    - Business-ready aggregated data
    - City-level address statistics
    - Used for reporting and analytics
*/

WITH cities AS (
    SELECT
        ID AS CITY_ID,
        CITY_NAME,
        COUNTRY,
        _AB_CDC_UPDATED_AT AS CITY_LAST_UPDATED
    FROM {{ ref('cities_cleansing') }}
),

addresses AS (
    SELECT
        ID AS ADDRESS_ID,
        PERSON_NAME,
        STREET,
        CITY_ID,
        _AB_CDC_UPDATED_AT AS ADDRESS_LAST_UPDATED
    FROM {{ ref('addresses_cleansing') }}
),

address_counts AS (
    SELECT
        CITY_ID,
        COUNT(*) AS TOTAL_ADDRESSES,
        COUNT(DISTINCT PERSON_NAME) AS UNIQUE_PERSONS,
        MIN(ADDRESS_LAST_UPDATED) AS FIRST_ADDRESS_DATE,
        MAX(ADDRESS_LAST_UPDATED) AS LAST_ADDRESS_DATE
    FROM addresses
    GROUP BY CITY_ID
)

SELECT
    c.CITY_ID,
    c.CITY_NAME,
    c.COUNTRY,
    COALESCE(ac.TOTAL_ADDRESSES, 0) AS TOTAL_ADDRESSES,
    COALESCE(ac.UNIQUE_PERSONS, 0) AS UNIQUE_PERSONS,
    ac.FIRST_ADDRESS_DATE,
    ac.LAST_ADDRESS_DATE,
    c.CITY_LAST_UPDATED,
    CURRENT_TIMESTAMP() AS REPORT_GENERATED_AT
FROM cities c
LEFT JOIN address_counts ac
    ON c.CITY_ID = ac.CITY_ID
ORDER BY c.COUNTRY, c.CITY_NAME