WITH order_details AS (
SELECT *
FROM {{ source('northwind', 'order_details') }}
)
SELECT orderid AS order_id
		, productid AS product_id
		, unitprice::numeric AS unit_price
		, quantity::int
		, discount::numeric
FROM order_details