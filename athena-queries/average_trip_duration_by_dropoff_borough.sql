-- Average Trip Duration by Dropoff Borough
SELECT
    t.doborough AS dropoff_borough,
    ROUND(SUM(total_trip_duration) / SUM(total_trips), 2) AS avg_trip_duration
FROM
    "tlc-trip-data-stage"."tlc-stage" t
GROUP BY
    t.doborough
ORDER BY
    avg_trip_duration DESC;