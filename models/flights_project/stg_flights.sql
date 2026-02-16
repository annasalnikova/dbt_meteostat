WITH flights_one_month AS (
        SELECT * 
        FROM {{source('air_force', 'project_flights')}}
    )
    SELECT * FROM flights_one_month