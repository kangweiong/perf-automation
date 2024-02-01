SELECT
    COUNT(*) FILTER(WHERE ride_status=100 AND ride_type <> 100) AS rider_cancel,
    COUNT(*) FILTER(WHERE ride_status=310 AND ride_type <> 100) AS driver_cancel,
    COUNT(*) FILTER(WHERE ride_status=70) AS completed,
    COUNT(*) FILTER(WHERE driver_uuid is not null and rider_uuid is not null) AS matched,
    COUNT(*) AS demand,
    COUNT(DISTINCT driver_uuid) FILTER(WHERE ride_status=70) AS completed_drivers
FROM ride_entity
WHERE region = '{region}'
  AND ride_type in (1,10,20)
  AND rider_uuid is not null
  AND date_trunc('month', DATE(coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')