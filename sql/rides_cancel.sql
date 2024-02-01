WITH trip AS (
SELECT
    id,
    driver_uuid,
    ride_status,
    case 
        when ride_status = 310 and start_time is null then round((EXTRACT(EPOCH from drop_off_time - pick_up_arrived_time)/60)::numeric,2)::float 
        when ride_status = 100 and start_time is null then round((EXTRACT(EPOCH from drop_off_time - pick_up_arrived_time)/60)::numeric,2)::float 
        when ride_status = 70 and start_time is not null then round((EXTRACT(EPOCH from start_time - pick_up_arrived_time)/60)::numeric,2)::float
    else null end as waiting_time
  FROM ride_entity
  WHERE region = '{region}'
    AND ride_status in (310,100,70) 
    AND pick_up_arrived_time is not null
    AND driver_uuid is not null
    AND ride_price <> 0.00
    AND ride_type in (1,10,20)
    AND rider_uuid is not null
    AND date_trunc('month', DATE(coalesce(reservation_ride_start_time,create_time) AT TIME ZONE '{timezone}')) = date_trunc('month', date '{date}'))

SELECT
    AVG(waiting_time) FILTER(where ride_status = 70) AS avg_waiting_time_completed,
    AVG(waiting_time) FILTER(where ride_status = 100) AS avg_waiting_time_Rider_cxl,
    AVG(waiting_time) FILTER(where ride_status = 310) AS avg_waiting_time_Driver_cxl,
    COUNT(*) AS total_booking_count,
    COUNT(ride_status) FILTER(where ride_status = 70) AS booking_Count_Completed,
    COUNT(ride_status) FILTER(where ride_status = 100) AS booking_Count_Rider_cxl,
    COUNT(ride_status) FILTER(where ride_status = 310) AS booking_Count_Driver_cxl
FROM trip
WHERE waiting_time is not null
    AND waiting_time <= 20.00