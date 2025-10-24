{{
  config(
    schema='cleansing'
  )
}}

-- Silver Layer: Cleaned and standardized cities
-- Snowflake 최적화: 단순하고 효율적인 쿼리

WITH bronze_cities AS (
    SELECT c1
    FROM {{ ref('cities_bronze') }}
    WHERE c1 IS NOT NULL
),

cleaned_cities AS (
    SELECT
        TRIM(UPPER(REPLACE(c1, '*', ''))) AS city
    FROM bronze_cities
    WHERE c1 NOT IN ('England', 'Scotland', 'Wales', 'Northern Ireland')
)

SELECT DISTINCT
    city
FROM cleaned_cities
WHERE city IS NOT NULL
  AND city != ''  -- 빈 문자열 제거
ORDER BY city