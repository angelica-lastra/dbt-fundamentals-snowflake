with 

payments as (

    select * from {{ ref('stg_payments') }}
),

payment_amounts as (

    select

        order_id,
        max(created_at) as payment_finalized_date,
        sum(amount) as total_amount_paid

    from payments
    where status <> 'fail'
    group by 1

)

select * from payment_amounts