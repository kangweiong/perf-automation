import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.slack import SlackBot


def main():
  load_dotenv()

  redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

  dt_format = "%Y-%m-%d"
  start_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime(dt_format)
  end_date = (datetime.today().replace(day=1) - timedelta(days=1)).strftime(dt_format)

  DAYS_IN_MONTH = int(end_date.split("-")[2])

  output_date = datetime.strptime(start_date, dt_format).strftime("%b_%Y")

  queries = [[
    Query(2856, params={"date": start_date}),
    Query(2857, params={"date": start_date}),
    Query(3000, params={"date": start_date}),
    Query(3001, params={"date": start_date}),
    Query(3004, params={"date": start_date}),
  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  df1 = redash.get_result(2856)  # SG - All trips breakdown by product
  df2 = redash.get_result(2857)  # KH/TH - All trips breakdown by type
  df3 = redash.get_result(3000)  # GMV - SG
  df4 = redash.get_result(3001)  # GMV - KH/TH/VN
  df5 = redash.get_result(3004)  # KH - T1

    # Save to Excel
  output_file = f"monthly_report_{output_date}.xlsx"

  with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
      df1.to_excel(writer, sheet_name="SG Trips Breakdown", index=False)
      df2.to_excel(writer, sheet_name="KH_TH Trips Breakdown", index=False)
      df3.to_excel(writer, sheet_name="GMV SG", index=False)
      df4.to_excel(writer, sheet_name="GMV KH_TH_VN", index=False)
      df5.to_excel(writer, sheet_name="KH T1", index=False)

  slack = SlackBot()
  slack.uploadFile(output_file, 
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Operational Matrix Report for {output_date}")

if __name__ == "__main__":
  main()