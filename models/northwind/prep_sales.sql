WITH order_details AS (
SELECT *
FROM {{ref('staging_order_details')}}
),
orders AS (
SELECT *
FROM {{ref('staging_orders')}}
),
products AS (
SELECT *
FROM {{ref('staging_products')}}
)
SELECT d.*
		, o.customer_id
		, o.employee_id
		, o.order_date
		, p.product_name
		, p.supplier_id
		, d.unit_price * d.quantity * (1 - d.discount) AS revenue
		, date_part('year', order_date)
        , category_id
FROM order_details d
LEFT JOIN orders o
ON d.order_id = o.order_id
LEFT JOIN products p
ON d.product_id = p.product_id