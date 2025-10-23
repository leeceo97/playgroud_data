select
    city,
    total_spend
from test.dbt_gold.spend_by_city_gold
where total_spend < 0
