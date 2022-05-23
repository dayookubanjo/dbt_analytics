with payments as (
    select 
        * from {{ ref('stg_payments') }}

),
orders as (
    select 
        * from {{ ref('stg_orders') }}
),
payment_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        sum(case when payments.status = 'success' then payments.amount end) as amount
    from orders left join 
    payments on orders.order_id = payments.order_id
    group by
        orders.order_id,
        orders.customer_id 
)
select 
    order_id,
    customer_id,
    coalesce(amount,0) as amount 
from payment_orders