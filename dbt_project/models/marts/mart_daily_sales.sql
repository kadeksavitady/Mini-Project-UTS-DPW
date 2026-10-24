select
    order_date,
    product_category,
    payment_method,
    count(*) as order_count,
    sum(quantity) as units_sold,
    sum(recognized_net_amount) as recognized_net_sales
from {{ ref('fct_orders') }}
group by
    order_date,
    product_category,
    payment_method

