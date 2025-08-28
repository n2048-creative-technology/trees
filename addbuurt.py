import pandas as pd
import requests
import time
import csv

INPUT_FILE = "input.csv"
OUTPUT_FILE = "output_with_buurt.csv"
API_URL = "https://openpostcode.nl/api/nearest"

# Read the full input once to loop over
df = pd.read_csv(INPUT_FILE, delimiter=",")

# Strip any whitespace from column headers
df.columns = df.columns.str.strip()

# Prepare the output CSV and write the header (only once)
header = list(df.columns) + ["buurt"]
with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(header)

    # Process row-by-row
    for idx, row in df.iterrows():
        lon = row["latitude"]
        lat = row["longitude"]

        buurt = "N/A"
        try:
            response = requests.get(API_URL, params={"latitude": lat, "longitude": lon}, timeout=5)
            response.raise_for_status()
            data = response.json()
            buurt = data.get("buurt", "N/A")

        except Exception as e:
            print(f"⚠️  Error on row {idx} (lat={lat}, lon={lon}): {e}")
            buurt = "error"

        # Convert row to list and add buurt
        row_list = row.tolist() + [buurt]
        writer.writerow(row_list)
        print(f"✅ Row {idx + 1}/{len(df)} written – buurt: {buurt}")

        time.sleep(0.2)  # be nice to the API
