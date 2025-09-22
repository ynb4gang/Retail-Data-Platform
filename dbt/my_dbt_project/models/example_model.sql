select
sku,
sum(qty) as total_qty,
avg(price) as avg_price
from {{ ref('staging__sales') }}
group by sku