WITH trips AS
  (SELECT 
    DATE(coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}') AS trip_date,
    ride_status,
    rider_uuid
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_type in (1,10,20)
    AND rider_uuid is not null
    AND date_trunc('month', DATE(coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}'))
, daily AS (
  SELECT
    trip_date,
    COUNT(DISTINCT rider_uuid) AS book_daily,
    COUNT(DISTINCT CASE WHEN ride_status=70 THEN rider_uuid ELSE NULL END) AS completed_daily
  FROM trips
  GROUP BY trip_date)
, monthly AS (
  SELECT
    COUNT(DISTINCT rider_uuid) AS book_monthly,
    COUNT(DISTINCT rider_uuid) FILTER(WHERE ride_status=70) AS completed_monthly
  FROM trips)

SELECT
  AVG(book_monthly) AS book_monthly,
  AVG(completed_monthly) AS completed_monthly,
  AVG(book_daily) AS book_daily,
  AVG(completed_daily) AS completed_daily
FROM daily,monthly