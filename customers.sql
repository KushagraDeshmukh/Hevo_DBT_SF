with
    customer as (
        select id as customer_id, first_name, last_name from {{ ref("customer") }}
    ),
    orders as (
        select
            id,
            user_id,
            min(order_date) as most_recent_orders,
            max(order_date) as number_of_orders
        from {{ ref("orders") }}
        group by all
    ),
    payment as (
        select id, order_id, payment_method, sum(amount) as customer_lifetime_value
        from {{ ref("payment") }}
        group by all
    ),
    final as (
        select
            customer.customer_id,
            customer.first_name,
            customer.last_name,
            orders.id,
            orders.user_id,
            orders.most_recent_orders,
            orders.number_of_orders,
            payment.id as payment_id,
            payment.order_id,
            payment.customer_lifetime_value
        from customer
        left join orders on customer.customer_id = orders.user_id
        left join payment on orders.id = payment.order_id
    )

