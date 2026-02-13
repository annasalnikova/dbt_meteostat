WITH departures AS (SELECT origin
		, COUNT(DISTINCT dest) AS nunique_to
		, COUNT(sched_dep_time) AS total_planned
		, COUNT(cancelled) AS total_cancelled
		, COUNT(diverted) AS total_diverted
		, COUNT(arr_time) AS actual_flights
FROM {{ref('prep_flights')}}
GROUP BY origin),
arrivals AS (SELECT dest
		, COUNT(DISTINCT origin) AS nunique_from
		, COUNT(sched_dep_time) AS total_planned
		, COUNT(cancelled) AS total_cancelled
		, COUNT(diverted) AS total_diverted
		, COUNT(arr_time) AS actual_flights
FROM {{ref('prep_flights')}}
GROUP BY dest),
total_flights AS (
		SELECT origin AS airport_code
				, nunique_to
				, nunique_from
				, d.total_planned + a.total_planned AS total_planned
				, d.total_cancelled + a.total_cancelled AS total_cancelled
				, d.total_diverted + a.total_diverted AS total_diverted
				, d.actual_flights + a.actual_flights AS actual_flights
		FROM departures d
		JOIN arrivals a
		ON d.origin = a.dest
)
SELECT city, country, name, tf.*
		, min_temp_c
		, max_temp_c
		, precipitation_mm
		, max_snow_mm
		, avg_wind_direction
		, avg_wind_speed_kmh
		, wind_peakgust_kmh
FROM total_flights tf
LEFT JOIN {{ref('prep_airports')}} a
ON tf.airport_code = a.faa
LEFT JOIN prep_weather_daily w
ON tf.airport_code = w.airport_code