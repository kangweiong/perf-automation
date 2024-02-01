with _ride AS
  (SELECT
    ride_status,
    driver_uuid,
    id,
    ride_type
FROM ride_entity
WHERE region = '{region}'
  AND ride_type in (1,10,20)
  AND rider_uuid is not null
  AND date_trunc('month', DATE(coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}')
  AND ride_status in (70,310,100,400,300))
, _driver AS
  (SELECT
    _ride.id,
    _ride.driver_uuid,
    CASE WHEN d.type in ('PRIVATE_HIRE','HOURLY_RENTAL') AND _ride.ride_status = 70 then 1 ELSE 0 END AS PHV_trip,
    CASE WHEN d.type in ('PRIVATE_HIRE','HOURLY_RENTAL') then 1 ELSE 0 END AS PHV_trip_booking,
    CASE WHEN d.type = 'TAXI' AND _ride.ride_status = 70 then 1 ELSE 0 END AS Taxi_trip,
    CASE WHEN d.type = 'TAXI' then 1 ELSE 0 END AS Taxi_trip_booking
  FROM tada_member_service.driver d
  JOIN _ride ON (_ride.driver_uuid::uuid = d.id))

SELECT 
    SUM(PHV_trip) AS PHV_Trip_Count,
    SUM(PHV_trip_booking) AS PHV_trip_booking,
    COUNT(DISTINCT driver_uuid) FILTER(WHERE PHV_trip=1) AS PHV_driver_completed,
    SUM(Taxi_trip) AS Taxi_Trip_Count,
    SUM(Taxi_trip_booking) AS Taxi_trip_booking,
    COUNT(DISTINCT driver_uuid) FILTER(WHERE Taxi_trip=1) AS Taxi_driver_completed
FROM _driver
