WITH trips AS
  (SELECT
    CASE WHEN ride_type = 20 THEN (reservation_ride_start_time at time zone '{timezone}')::date
                             ELSE (create_time at time zone '{timezone}')::date END AS LocalDate,
    driver_uuid
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_type in (1,10,20)
    AND rider_uuid is not null
    AND driver_uuid is not null
    AND CASE WHEN ride_type = 20 THEN date_trunc('month', DATE(reservation_ride_start_time AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')
                               ELSE date_trunc('month', DATE(create_time AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}') END
    AND ride_status=70)
, daily AS
  (SELECT
    LocalDate,
    COUNT(*) AS completed_trips,
    COUNT(DISTINCT driver_uuid) AS completed_driver_daily
  FROM trips
  GROUP BY LocalDate)
  
SELECT
    AVG(completed_trips/completed_driver_daily) AS ride_per_driver,
    AVG(completed_driver_daily) AS completed_driver_daily
FROM daily