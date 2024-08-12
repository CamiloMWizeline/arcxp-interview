-- Total Number of Trips by Borough and Month
SELECT
    t.puborough AS pickup_borough,
    concat(t.year, '-', t.month) year_month,
    SUM(t.total_trips) AS total_trips
FROM
    "tlc-trip-data-stage"."tlc-stage" t
GROUP BY
    t.puborough, t.year, t.month
ORDER BY
    t.year, t.month, total_trips DESC;