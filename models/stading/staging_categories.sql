WITH categories AS (
SELECT *
FROM {{ source('northwind', 'northwind_categories') }}
)
SELECT categoryid AS category_id
		, categoryname AS category_name
FROM categories
