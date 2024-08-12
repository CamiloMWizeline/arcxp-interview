-- Average Trip Distance by Pickup Borough
SELECT
    t.puborough AS pickup_borough,
    ROUND(SUM(total_trip_distance) / SUM(total_trips), 2) AS avg_trip_distance
FROM
    "tlc-trip-data-stage"."tlc-stage" t
GROUP BY
    t.puborough
ORDER BY
    avg_trip_distance DESC;