-- spend_by_city_gold 테이블에 spend 값이 0 미만이면 오류
select
    city,
    total_spend
from {{ ref('spend_by_city_gold') }}
where total_spend < 0