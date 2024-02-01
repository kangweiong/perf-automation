import argparse
import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.constants import IDS, REGIONS, TIMEZONES
from utils.helpers import Postgres, Query, Redash
from utils.slack import SlackBot


def main(region:str):

  load_dotenv()

  redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))
  pg = Postgres()

  # Get last date of previous month
  dt_format = "%Y-%m-%d"
  date = (datetime.today().replace(day=1) - timedelta(days=1)).strftime(dt_format)

  DAYS_IN_MONTH = int(date.split("-")[2])

  output_date = datetime.strptime(date, dt_format).strftime("%b_%Y")

  timezone = TIMEZONES[region]
  region_str = REGIONS[region]
  region_id = IDS[region]


  # Query 1
  df1 = pg.from_file("sql/rides_summary.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 2
  df2 = pg.from_file("sql/rides_by_type.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 3
  df3 = pg.from_file("sql/drivers_approved.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 4
  df4 = pg.from_file("sql/delivery_summary.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 5
  df5 = pg.from_file("sql/rider_signup.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 6
  df6 = pg.from_file("sql/riders_ft.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 7
  df7 = pg.from_file("sql/riders_daily.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 8
  df8 = pg.from_file("sql/riders_unique.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 9
  df9 = pg.from_file("sql/rides_cancel.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 10
  df10 = pg.from_file("sql/drivers_daily.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 11
  df11 = pg.from_file("sql/drivers_signup.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 12
  df12 = pg.from_file("sql/drivers_ft.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 13
  df13 = pg.from_file("sql/drivers_utilisation.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 14
  df14 = pg.from_file("sql/drivers_eta.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 15
  df15 = pg.from_file("sql/riders_car.sql", params={"region": region, "timezone": timezone, "date": date})

  # Query 16
  df16 = pg.from_file("sql/drivers_car.sql", params={"region": region, "timezone": timezone, "date": date})

  queries = [
    [Query(2667, params={"region": region_str, "timezone": timezone, "date": date}), 
    Query(2668, params={"region": region_str, "timezone": timezone, "date": date}), 
    Query(2669, params={"region": region_id, "timezone": timezone, "date": date}),
    Query(2670, params={"region": region_id, "timezone": timezone, "date": date}),
    Query(2671, params={"region": region_id, "timezone": timezone, "date": date}),],
  ]

  for query_list in queries:
    redash.run_queries(query_list)

  # Query 1
  bq1 = redash.get_result(2667)

  # Query 2
  bq2 = redash.get_result(2668)

  # Query 3
  bq3 = redash.get_result(2669)

  # Query 4
  bq4 = redash.get_result(2670)

  # Query 5
  bq5 = redash.get_result(2671)

  df = pd.DataFrame()

  df['rides'] = df1.completed
  df['demand'] = df1.demand
  df['match_rate'] = df1.matched/df.demand
  df['completion_rate'] = df.rides/df.demand
  df['daily_rides'] = df.rides/DAYS_IN_MONTH
  df['uncompleted'] = df.demand - df.rides

  df['phv_rides'] = df2.phv_trip_count
  df['phv_demand'] = df2.phv_trip_booking
  df['phv_completed_drivers'] = df2.phv_driver_completed
  df['phv_approved_drivers'] = df3.approved_phv

  df['taxi_rides'] = df2.taxi_trip_count
  df['taxi_demand'] = df2.taxi_trip_booking
  df['taxi_completed_drivers'] = df2.taxi_driver_completed
  df['taxi_approved_drivers'] = df3.approved_taxi

  df['delivery_rides'] = df4.delivery_completed
  df['delivery_demand'] = df4.delivery_count
  df['delivery_completion_rate'] = df.delivery_rides/df.delivery_demand

  df['rider_mau'] = bq1.active_users
  df['rider_mau_demand'] = df.demand/df.rider_mau
  df['rider_mau_rides'] = df.rides/df.rider_mau

  df['rider_downloads'] = None

  df['rider_signup'] = df5.rider_signup
  df['rider_signup_daily'] = df.rider_signup/DAYS_IN_MONTH

  df['rider_ft_all_time'] = df6.all_time
  df['rider_ft_same_month'] = df6.same_month
  df['rider_same_month_activation'] = df.rider_ft_same_month/df.rider_signup

  df['rider_unique_open_monthly'] = bq2.open_monthly
  df['rider_unique_search_monthly'] = bq2.search_monthly
  df['rider_unique_book_monthly'] = df7.book_monthly
  df['rider_unique_complete_monthly'] = df7.completed_monthly

  df['rider_unique_open_daily'] = bq2.open_daily
  df['rider_unique_search_daily'] = bq2.search_daily
  df['rider_unique_book_daily'] = df7.book_daily
  df['rider_unique_complete_daily'] = df7.completed_daily

  df['book_search_ratio_daily'] = df.rider_unique_book_daily/df.rider_unique_search_daily
  df['booking_per_user'] = df.demand/df.rider_unique_book_monthly
  df['complete_per_user'] = df.rides/df.rider_unique_complete_monthly

  df['duplicate_ratio'] = df.demand/df8.unique

  df['rider_waiting_before_cancel'] = df9.avg_waiting_time_rider_cxl
  df['rider_cancellation_rate'] = df1.rider_cancel/df.demand*100

  df['driver_mau'] = bq3.online_driver_count
  df['completed_driver'] = df1.completed_drivers
  df['driver_online_daily'] = bq3.online_driver_daily
  df['pinged_drivers_daily'] = bq4.pinged_drivers_daily
  df['completed_driver_daily'] = df10.completed_driver_daily

  df['online_mau'] = df.driver_online_daily/df.driver_mau
  df['completed_online'] = df.completed_driver_daily/df.driver_online_daily
  df['online_no_complete'] = df.driver_online_daily-df.completed_driver_daily

  df['ride_per_driver'] = df10.ride_per_driver

  df['driver_downloads'] = None

  df['driver_sign_up'] = df11.driver_sign_up
  df['driver_sign_up_daily'] = df.driver_sign_up/DAYS_IN_MONTH

  df['driver_ft_all_time'] = df12.all_time
  df['driver_ft_same_month'] = df12.same_month
  df['driver_sign_up_activation_rate'] = df.driver_ft_same_month/df.driver_sign_up

  df['driver_approved_activation_rate'] = df.driver_ft_same_month/df11.driver_same_month_approved
  df['driver_approved'] = df11.driver_approved
  df['driver_same_month_approved'] = df11.driver_same_month_approved

  df['driver_average_online_hours'] = bq5.avg_online_hour
  df['driver_average_utilisation_hours'] = df13.avg_utilisation_hours

  df['ping_per_driver_daily'] = bq4.ping_per_driver_daily

  df['driver_waiting_before_cancel'] = df9.avg_waiting_time_driver_cxl
  df['driver_cancellation_rate'] = df1.driver_cancel/df.demand*100

  df['cater_rate'] = df.rides/df8.unique
  df['r_d_ratio'] = df.rider_mau/df.driver_mau

  df['average_eta'] = df14.daily_median_eta

  df['first_time_unique'] = df.rider_ft_all_time/df.rider_unique_complete_monthly

  df['riders_repeated'] = df15.repeated
  df['rider_activated'] = df15.activated
  df['rider_resurrected'] = df15.resurrected
  df['rider_churned'] = df15.churned
  df['rider_inflow'] = df.rider_activated + df.rider_resurrected - df.rider_churned

  df['drivers_repeated'] = df16.repeated
  df['driver_activated'] = df16.activated
  df['driver_resurrected'] = df16.resurrected
  df['driver_churned'] = df16.churned
  df['driver_inflow'] = df.driver_activated + df.driver_resurrected - df.driver_churned

  df = df.T
  df.columns = [f"{output_date}"]

  df.to_csv(f"{region}_{output_date}.csv")

  slack = SlackBot()
  slack.uploadFile(f"{region}_{output_date}.csv", 
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report for {region} {output_date}")

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--region', type=str, help='Region to run the script for, i.e., SG, VN, KH or TH')
  args = parser.parse_args()
  main(region=args.region)
