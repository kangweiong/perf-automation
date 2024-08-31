WITH first_trip AS
  (SELECT DISTINCT ON (rider_uuid)
    rider_uuid,
    (coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}')::date AS first_trip
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_status = 70
    AND ride_type in (1,10,20)
    AND rider_uuid is not null
  ORDER BY rider_uuid, create_time ASC)
, account AS
  (SELECT
    id,
    (created_at at TIME ZONE '{timezone}')::date AS account_created_date
  FROM tada_member_service.rider r
  WHERE region = '{region}'
  AND id in
    (SELECT rider_uuid::uuid FROM first_trip
    WHERE date_trunc('month', first_trip) = date_trunc('month', date '{date}')))


SELECT
    COUNT(*) AS all_time,
    SUM(CASE WHEN date_trunc('month', account_created_date) = date_trunc('month', date '{date}') THEN 1 ELSE 0 END) AS same_month
FROM account