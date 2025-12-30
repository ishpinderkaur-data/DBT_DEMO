WITH daily_weather AS(

    select 

    DATE(TIME) AS daily_weather,
    WEATHER,
     TEMP,PRESSURE,
     HUMIDITY,
     CLOUDS
    
    
    from 
    {{ source('DEMO', 'WEATHER') }}

    -- LIMIT 10 initially to see less data for analysis
),

daily_weather_agg AS(
select DAILY_WEATHER,WEATHER,
round(avg(TEMP),2) as avg_temp,
round(avg(PRESSURE),2) as avg_pressure,
round(avg(HUMIDITY),2) as avg_humidity,
round(avg(CLOUDS),2) as avg_clouds
--COUNT(WEATHER), we calculated it to check

--ROW_NUMBER() OVER (PARTITION BY DAILY_WEATHER ORDER BY COUNT(WEATHER) DESC) AS row_number did it for the qualify below
FROM DAILY_WEATHER

GROUP BY DAILY_WEATHER,WEATHER
QUALIFY ROW_NUMBER() OVER (PARTITION BY DAILY_WEATHER ORDER BY COUNT(WEATHER)DESC)=1

)

SELECT * FROM daily_weather_agg