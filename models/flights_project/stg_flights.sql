WITH flights_one_month AS (
        SELECT * 
        FROM {{source('air_force', 'flights_project')}}
    )
    SELECT * FROM flights_one_month