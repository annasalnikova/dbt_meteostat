WITH products AS (
SELECT *
FROM {{ source('northwind', 'northwind_products') }}
)
SELECT productid AS product_id
		, productname AS product_name
		, supplierid AS supplier_id
		, categoryid AS category_id
		, unitprice::numeric AS unit_price
FROM products