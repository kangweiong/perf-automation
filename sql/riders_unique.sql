WITH trips AS
  (SELECT
    rider_uuid ||','|| destination_h3_address ||','|| pick_up_h3_address ||','|| EXTRACT(EPOCH FROM (create_time AT TIME ZONE '{timezone}'))::integer / 1800 as unique_id,
    rider_uuid,
    ride_status
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_type in (1,10,20)
    AND rider_uuid is not null
    AND date_trunc('month', DATE(coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}'))

SELECT
    COUNT(DISTINCT unique_id) AS unique
FROM trips