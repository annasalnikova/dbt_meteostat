WITH flights_one_month AS (
        SELECT * 
        FROM {{source('flights_project', 'air_force')}}
    )
    SELECT * FROM flights_one_month