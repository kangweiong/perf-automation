SELECT
    COUNT(*) FILTER(WHERE ride_status = 70) AS delivery_completed,
    COUNT(*) AS delivery_count
FROM ride_entity
WHERE ride_type = 100
  AND region='{region}'
  AND date_trunc('month', DATE(create_time AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')