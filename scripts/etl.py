import json
import os
import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TOKEN = os.getenv("INFLUXDB_TOKEN")

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def extract(cell_ranges: list) -> list:
    
    creds, _ = google.auth.default()

    try:
        service = build("sheets", "v4", credentials=creds)

        result = (
            service.spreadsheets()
            .values()
            .batchGet(spreadsheetId=SPREADSHEET_ID, ranges=cell_ranges)
            .execute()
        )
        ranges = result.get("valueRanges", [])
        print(f"{len(ranges)} ranges retrieved")
        return ranges
    except HttpError as error:
        print(f"An error occured: {error}")
        return error

def transform(jsons: list) -> dict:
    expense_df = pd.DataFrame(data=jsons[0]['values'][1:], columns=jsons[0]['values'][0]).dropna()
    paycheck_df = pd.DataFrame(data=jsons[1]['values'][1:], columns=jsons[1]['values'][0]).dropna()

    expense_df["Date"] = pd.to_datetime(expense_df["Date"], format='%m/%d/%Y')
    paycheck_df["Date"] = pd.to_datetime(paycheck_df["Date"], format='%m/%d/%y')

    return {"Expenses": expense_df, "Paychecks": paycheck_df}

def load(dataframes: dict) -> None:
    client = InfluxDBClient('http://influxdb2:8086', token=TOKEN, org='docs')

    bucket = "expenses"

    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    val = 0
    for _, row in dataframes["Expenses"].iterrows():
        point = (
            Point(bucket)
            .tag("type", row["Type of Expense"])
            .tag("name", row["Expense Name"])
            .field("amount_spent", float(row["Amount Spent"][1:]))
            .time(row["Date"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org='docs', record=point)
        val += 1
    bucket = "paychecks"

    val = 0
    for _, row in dataframes["Paychecks"].iterrows():
        point = (
            Point(bucket)
            .field("paycheck", row["Paycheck"])
            .time(row["Date"], WritePrecision.NS)
            .field("savings_budget", float(row["Savings Budget"][1:]))
            .field("needs_budget", float(row["Needs Budget"][1:]))
            .field("wants_budget", float(row["Wants Budget"][1:]))
            .field("savings_actual", float(row["Savings Actual"][1:]))
            .field("needs_actual", float(row["Needs Actual"][1:]))
            .field("wants_actual", float(row["Wants Actual"][1:]))
        )
        write_api.write(bucket=bucket, org='docs', record=point)
        val += 1

    

if __name__ == "__main__":
    range_vals = extract(["A1:D300", "G1:N100"])

    dataframes = transform(range_vals)

    load(dataframes)

