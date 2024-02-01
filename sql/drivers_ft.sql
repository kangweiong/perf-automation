WITH first_trip AS
  (SELECT DISTINCT ON (driver_uuid)
    driver_uuid,
    CASE WHEN ride_type = 20 THEN (reservation_ride_start_time at time zone '{timezone}')::date
                             ELSE (create_time at time zone '{timezone}')::date END AS first_trip
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_status = 70
    AND ride_type in (1,10,20)
    AND rider_uuid is not null
    AND driver_uuid is not null
  ORDER BY driver_uuid, create_time ASC)
, account AS
  (SELECT
    id,
    (created_at at time zone '{timezone}')::date AS account_created_date,
    approved
  FROM tada_member_service.driver
  WHERE id in
    (SELECT driver_uuid::uuid FROM first_trip
    WHERE date_trunc('month', first_trip) = date_trunc('month', date '{date}')))

SELECT
    COUNT(*) AS all_time,
    COUNT(*) FILTER(WHERE date_trunc('month', account_created_date) = date_trunc('month', date '{date}')) AS same_month
FROM account