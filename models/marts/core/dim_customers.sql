with orders as (
    select * from {{ ref('fact_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}

),

customers_fact_orders as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        sum(amount) as amount

    from orders
    
    group by 1,2,3

), 

final as (
    select *
    from customers_fact_orders 
    left join customers using (customer_id)
)
    
select * from final




