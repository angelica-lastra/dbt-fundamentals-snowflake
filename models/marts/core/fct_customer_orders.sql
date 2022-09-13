with

orders as (
    
    select * from {{ ref('stg_orders') }}
),

customers as (

    select * from {{ ref('stg_customers') }}

),

payments as (

    select * from {{ ref('int_payment_amounts') }}

),

customer_orders as (

    select

        orders.customer_id,
        orders.order_id,
        orders.order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        min(orders.order_placed_at) as first_order_date,
        max(orders.order_placed_at) as most_recent_order_date

    from orders
    left join payments
        on orders.order_id = payments.order_id
    left join customers
        on orders.customer_id = customers.customer_id
    group by 1,2,3,4,5,6,7,8
),

payment_amounts_greater as (

    select

        payments.order_id,
        sum(payments.total_amount_paid) over (partition by customer_orders.customer_id order by customer_orders.order_placed_at) as customer_lifetime_value
    
    from payments
    left join customer_orders 
        on payments.order_id = customer_orders.order_id
    order by 1
),

final as (

    select

        customer_orders.customer_id,
        customer_orders.order_id,
        customer_orders.order_placed_at,
        customer_orders.order_status,
        customer_orders.total_amount_paid,
        customer_orders.payment_finalized_date,
        customer_orders.customer_first_name,
        customer_orders.customer_last_name,

        row_number() over (order by payments.order_id) as transaction_seq,
        row_number() over (partition by customer_orders.customer_id order by payments.order_id) as customer_sales_seq,
        
        case when rank() over (partition by customer_orders.customer_id order by customer_orders.order_placed_at, customer_orders.order_id) = 1
        then 'new' else 'return'
        end as nvsr,
        
        payment_amounts_greater.customer_lifetime_value,
        
        first_value(customer_orders.order_placed_at) over (partition by customer_orders.customer_id order by customer_orders.order_placed_at) as fdos

    from payments
    left join customer_orders
        on payments.order_id = customer_orders.order_id
    left join payment_amounts_greater
        on payments.order_id = payment_amounts_greater.order_id
    order by order_id
)

select * from final