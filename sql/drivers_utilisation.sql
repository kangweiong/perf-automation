WITH trip AS
  (SELECT
    id,
    driver_uuid,
    CASE WHEN ride_type = 20 THEN (reservation_ride_start_time at time zone '{timezone}')::date
                             ELSE (create_time at time zone '{timezone}')::date END AS day,
    CASE WHEN ride_type = 20 THEN (EXTRACT(EPOCH from drop_off_time - reservation_ride_start_time)/60)::numeric::float
                             ELSE (EXTRACT(EPOCH from drop_off_time - assign_time)/60)::numeric::float END AS utilisation_min
    -- (EXTRACT(EPOCH from drop_off_time - assign_time)/60)::numeric::float as utilisation_min
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_type in (1,10,20)
    AND ride_status = 70
    AND driver_uuid is not null --matched trips only
    AND drop_off_time is not null
    AND CASE WHEN ride_type = 20 THEN date_trunc('month', DATE(reservation_ride_start_time AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')
                               ELSE date_trunc('month', DATE(create_time AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}') END)
, _month AS
  (SELECT 
    day,
    SUM(utilisation_min)::float/COUNT(distinct driver_uuid) as avg_utilisation_min
  FROM trip
  WHERE utilisation_min <= 100.00
  GROUP BY day
)

SELECT
    avg(avg_utilisation_min)/60 AS avg_utilisation_hours
FROM _month
