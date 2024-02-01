WITH _rides AS
  (SELECT
    (create_time at time zone '{timezone}') as local_create_time,
    date_trunc('day', (create_time at time zone '{timezone}')) as local_create_time_day_trunc,
    id as ride_uuid, 
    ride_status,
    pick_up_arrived_time,
    assign_time,
    create_time,
    EXTRACT(EPOCH from (assign_time - create_time))::float as assign_timediff,
    EXTRACT(EPOCH from (pick_up_arrived_time - assign_time))::float/60 as arrive_assign_timediff,
    EXTRACT(epoch from (pick_up_arrived_time - create_time))::float/60 as arrive_request_timediff
    FROM ride_entity
    WHERE region = '{region}'
    AND ride_type in (1,10)
    AND rider_uuid is not null
    AND ride_status = 70
    AND date_trunc('month', (create_time at time zone '{timezone}')::date) = date_trunc('month', date '{date}')
    ORDER BY create_time DESC)
, _sorted AS
  (SELECT
    local_create_time_day_trunc,
    arrive_request_timediff,
    ntile(2) over (partition by local_create_time_day_trunc order by arrive_request_timediff) as pickup_rank
  FROM _rides)
, _median AS
  (SELECT
    local_create_time_day_trunc,
    max(arrive_request_timediff) as T2pickup_median
  FROM _sorted
  WHERE pickup_rank =1
  GROUP BY local_create_time_day_trunc
)

SELECT
    avg(T2pickup_median) AS daily_median_ETA
FROM _median
