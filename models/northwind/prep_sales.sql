WITH order_details AS (
SELECT *
FROM {{ source('northwind', 'order_details') }}
),
orders AS (
SELECT *
FROM {{ source('northwind', 'orders') }}
),
products AS (
SELECT *
FROM {{ source('northwind', 'products') }}
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