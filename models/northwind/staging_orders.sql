SELECT orderid AS order_id
		, customerid AS customer_id
		, employeeid AS employee_id
		, orderdate::date AS order_date
		, requireddate::date AS required_date
		, shippeddate::date AS shipped_date
		, shipvia AS ship_via
		, shipcity AS ship_city
		, shipcountry AS ship_country
FROM {{ source('northwind', 'orders') }}