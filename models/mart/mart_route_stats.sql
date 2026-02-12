WITH route_data AS (
		SELECT origin
				, dest
				, airline
				, tail_number
				, actual_elapsed_time
				, arr_delay
				, cancelled
				, diverted
		FROM {{ref('prep_flights')}}
		)
SELECT origin
		, dest
		, a_origin.name AS origin_name
		, a_origin.city AS origin_city
		, a_origin.country AS origin_country
		, a_dest.name AS dest_name
		, a_dest.city AS dest_city
		, a_dest.country AS dest_country
		, COUNT(*) AS total_flights
		, COUNT(DISTINCT tail_number) AS n_tail_number
		, COUNT(DISTINCT airline) AS n_airline
		, AVG(actual_elapsed_time) AS avg_actual_elapsed_time
		, AVG(arr_delay) AS avg_arr_delay
		, MAX(arr_delay) AS max_arr_delay
		, MIN(arr_delay) AS min_arr_delay
		, COUNT(cancelled) AS amm_cancelled
		, COUNT(diverted) AS amm_diverted
FROM route_data f
LEFT JOIN {{ref('prep_airports')}} a_origin
ON f.origin = a_origin.faa
LEFT JOIN {{ref('prep_airports')}} a_dest
ON f.dest = a_dest.faa
GROUP BY f.origin
		, f.dest
		, a_origin.name
		, a_origin.city
		, a_origin.country
		, a_dest.name
		, a_dest.city
		, a_dest.country
ORDER BY origin, dest