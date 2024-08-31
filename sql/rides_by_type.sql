with _ride AS
  (SELECT
    ride_status,
    driver_uuid,
    id,
    ride_type,
    car_type
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
    CASE WHEN d.type in ('PRIVATE_HIRE','HOURLY_RENTAL') AND _ride.ride_status = 70 then 1 ELSE 0 END AS phv_trip,
    CASE WHEN d.type in ('PRIVATE_HIRE','HOURLY_RENTAL') then 1 ELSE 0 END AS phv_trip_booking,
    CASE WHEN d.type = 'TAXI' AND _ride.ride_status = 70 then 1 ELSE 0 END AS taxi_trip,
    CASE WHEN d.type = 'TAXI' then 1 ELSE 0 END AS taxi_trip_booking,
    CASE WHEN _ride.car_type = 1001 AND _ride.ride_status = 70 then 1 ELSE 0 END AS bike_trip,
    CASE WHEN _ride.car_type = 1001 then 1 ELSE 0 END AS bike_trip_booking
  FROM _ride
  LEFT JOIN tada_member_service.driver d ON (_ride.driver_uuid::uuid = d.id))

SELECT 
    SUM(phv_trip) AS phv_trip_count,
    SUM(phv_trip_booking) AS phv_trip_booking,
    COUNT(DISTINCT driver_uuid) FILTER(WHERE phv_trip=1) AS phv_driver_completed,
    SUM(taxi_trip) AS taxi_trip_count,
    SUM(taxi_trip_booking) AS taxi_trip_booking,
    COUNT(DISTINCT driver_uuid) FILTER(WHERE taxi_trip=1) AS taxi_driver_completed,
    SUM(bike_trip) AS bike_trip_Count,
    SUM(bike_trip_booking) AS bike_trip_booking,
    COUNT(DISTINCT driver_uuid) FILTER(WHERE bike_trip=1) AS bike_driver_completed
FROM _driver
