import os
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import sys

CSV_FILENAME = "common_with_sorted_pe.csv"

if not os.path.exists(CSV_FILENAME):
    print(f"ERROR: {CSV_FILENAME} not found. main.py did not create it.")
    sys.exit(2)

df = pd.read_csv(CSV_FILENAME)

if not os.path.exists("/tmp/gcp.json"):
    print("ERROR: /tmp/gcp.json not found. Secret did not load.")
    sys.exit(3)

creds = Credentials.from_service_account_file(
    "/tmp/gcp.json",
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)

date_suffix = datetime.utcnow().strftime("%Y-%m-%d")
sheet_name = f"common_with_sorted_pe-{date_suffix}"

print("Creating Google Sheet:", sheet_name)
sh = gc.create(sheet_name)

target_email = os.environ.get("TARGET_EMAIL")
if target_email:
    sh.share(target_email, perm_type="user", role="writer")
    print("Shared with:", target_email)

ws = sh.get_worksheet(0)
rows = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
ws.update(rows)

print("Upload complete. Sheet URL:", sh.url)
