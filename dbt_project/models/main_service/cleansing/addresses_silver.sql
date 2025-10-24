{{
  config(
    schema='cleansing'
  )
}}

-- Silver Layer: Cleaned and standardized addresses
-- Snowflake 최적화: LIKE 대신 조인 사용, 서브쿼리 제거

WITH bronze_addresses AS (
    SELECT
        company_id,
        address,
        total_spend
    FROM {{ ref('addresses_bronze') }}
    WHERE address IS NOT NULL
),

silver_cities AS (
    SELECT DISTINCT city
    FROM {{ ref('cities_silver') }}
),

addresses_upper AS (
    SELECT
        company_id,
        UPPER(address) AS address,
        CAST(total_spend AS DECIMAL(18, 2)) AS total_spend
    FROM bronze_addresses
),

-- Snowflake 최적화: LIKE 패턴 매칭을 조인으로 변경
address_city_match AS (
    SELECT
        a.company_id,
        a.address,
        a.total_spend,
        c.city,
        -- 매칭 순위 (가장 긴 도시명 우선)
        ROW_NUMBER() OVER (
            PARTITION BY a.company_id, a.address
            ORDER BY LENGTH(c.city) DESC, c.city DESC
        ) AS rn
    FROM addresses_upper a
    LEFT JOIN silver_cities c
        ON a.address LIKE '%' || CHR(10) || c.city || ',%'
           OR a.address LIKE '%, ' || c.city || ',%'
           OR a.address LIKE '%' || c.city || ',%'
)

SELECT
    company_id,
    address,
    total_spend,
    COALESCE(city, 'OTHER') AS city
FROM address_city_match
WHERE rn = 1  -- 가장 적합한 도시만 선택

-- 데이터 품질 보장
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY company_id, address
    ORDER BY city NULLS LAST
) = 1