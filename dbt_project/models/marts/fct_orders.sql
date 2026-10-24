select
    order_id,
    customer_id,
    product_category,
    order_ts,
    cast(order_ts as date) as order_date,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    net_amount,
    payment_method,
    status,
    case
        when status in ('paid', 'shipped') then net_amount
        else 0
    end as recognized_net_amount
from {{ ref('stg_orders') }}

