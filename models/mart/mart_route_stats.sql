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
		, a_origin.name
		, a_origin.city
		, a_origin.country
		, a_dest.name
		, a_dest.city
		, a_dest.country
		, COUNT(*) AS total_flights
		, COUNT(DISTINCT tail_number)
		, COUNT(DISTINCT airline)
		, AVG(actual_elapsed_time)
		, AVG(arr_delay)
		, MAX(arr_delay)
		, MAX(arr_delay)
		, COUNT(cancelled)
		, COUNT(diverted)
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