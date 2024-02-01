WITH previous_months AS (
    SELECT
        driver_uuid,
        MIN((DATE_PART('year', DATE '{date}') - DATE_PART('year', DATE(coalesce(reservation_ride_start_time,create_time) + interval '{timezone} hour'))) * 12 +
              (DATE_PART('month', DATE '{date}') - DATE_PART('month', DATE(coalesce(reservation_ride_start_time,create_time) + interval '{timezone} hour')))) AS prev_trip_month
    FROM ride_entity
    WHERE region = '{region}'
        AND ride_type in (1, 10, 20)
        AND ride_status = 70
        AND driver_uuid IS NOT NULL
        AND DATE(coalesce(reservation_ride_start_time,create_time) + interval '{timezone} hour') < date_trunc('month', DATE '{date}')
    GROUP BY driver_uuid
),
selected_month AS (
    SELECT
        driver_uuid,
        MIN(DATE(coalesce(reservation_ride_start_time,create_time) + interval '{timezone} hour')) AS first_trip
    FROM ride_entity
    WHERE region = '{region}'
        AND ride_type in (1, 10, 20)
        AND ride_status = 70
        AND driver_uuid IS NOT NULL
        AND date_trunc('month', (coalesce(reservation_ride_start_time,create_time) + interval '{timezone} hour')) = date_trunc('month', DATE '{date}')
    GROUP BY driver_uuid
),
joined AS (
    SELECT
        s.driver_uuid,
        p.prev_trip_month
    FROM selected_month s
    LEFT JOIN previous_months p ON s.driver_uuid = p.driver_uuid
),
agg AS (
    SELECT
        COUNT(*) FILTER (WHERE prev_trip_month = 1) AS repeated,
        COUNT(*) FILTER (WHERE prev_trip_month > 1) AS resurrected,
        COUNT(*) FILTER (WHERE prev_trip_month IS NULL) AS activated
    FROM joined
),
prev_month_completion AS (
    SELECT COUNT(*) AS prev_total
    FROM previous_months
    WHERE prev_trip_month = 1
)
SELECT
    a.repeated,
    a.activated,
    a.resurrected,
    pmc.prev_total - a.repeated AS churned
FROM agg a, prev_month_completion pmc
