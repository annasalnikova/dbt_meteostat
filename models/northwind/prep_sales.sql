WITH order_details AS (
SELECT *
FROM {{ref('staging_order_details')}}
),
orders AS (
SELECT customer_id
		, employee_id
		, order_date
		, required_date
		, shipped_date
		, ship_via
		, ship_city
		, ship_country
FROM {{ref('staging_orders')}}
),
products AS (
SELECT product_name
		, supplier_id
		, category_id
		, unit_price
FROM {{ref('staging_products')}}
)
SELECT d.*
		, o.*
		, p.*
		, d.unit_price * d.quantity * (1 - d.discount) AS revenue
		, date_part('year', order_date)
FROM order_details d
LEFT JOIN orders o
ON d.order_id = o.order_id
LEFT JOIN products p
ON d.product_id = p.product_id