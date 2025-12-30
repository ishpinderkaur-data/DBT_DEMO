WITH CTE AS (
    SELECT 
    t.*,
    w.*
    FROM {{ ref('trips_fact') }} t
    LEFT JOIN {{ ref('daily_weather') }} w
    on t.TRIP_DATE =w.daily_weather
)

SELECT* FROM CTE