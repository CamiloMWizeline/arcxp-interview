-- Boroughs with the Highest Revenue per Trip
SELECT
    t.partition_0 as vehicle_type,
    t.puborough AS pickup_borough,
    t.doborough AS dropoff_borough,
    ROUND(SUM(avg_total_amount * total_trips) / SUM(total_trips), 2) AS avg_revenue_per_trip
FROM
    "tlc-trip-data-stage"."tlc-stage" t
GROUP BY
    t.partition_0, t.puborough, t.doborough
HAVING
    SUM(total_trips) > 100  -- Filter out borough pairs with few trips
ORDER BY
    avg_revenue_per_trip DESC;
