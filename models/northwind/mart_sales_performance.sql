WITH prep AS (
SELECT *
FROM {{ref('prep_sales')}})
SELECT date_part('year', order_date)
		, date_part('month', order_date)
		, category_name
		, SUM(revenue)
		, COUNT(order_id)
		, AVG(revenue)
FROM prep p
LEFT JOIN {{ref('staging_categories')}} c
ON p.category_id = c.category_id
GROUP BY date_part('year', order_date)
		, date_part('month', order_date)
		, category_name