import pandas as pd
import requests
import time

INPUT_OUTPUT_FILE = "/home/mauricio/Documents/Mauricio/Inge/trees/output_with_buurt.csv"
API_URL = "https://openpostcode.nl/api/nearest"

# Load CSV
df = pd.read_csv(INPUT_OUTPUT_FILE)

# Strip whitespace from column headers
df.columns = df.columns.str.strip()

# Identify rows where buurt is empty or error
mask = df["buurt"].isna() | (df["buurt"].astype(str).str.strip().str.lower() == "error") | (df["buurt"].astype(str).str.strip() == "")
rows_to_fix = df[mask]

print(f"Found {len(rows_to_fix)} rows to update.")

error_count = 0

for idx in rows_to_fix.index:
    lon = df.at[idx, "longitude"]  # swap if needed
    lat = df.at[idx, "latitude"]

    buurt = "N/A"
    try:
        response = requests.get(API_URL, params={"latitude": lon, "longitude": lat}, timeout=5)
        response.raise_for_status()
        data = response.json()
        buurt = data.get("buurt", "N/A")
        error_count = 0
    except Exception as e:
        print(f"⚠️  Error on row {idx} (lat={lon}, lon={lat}): {e}")
        buurt = "error"
        error_count = error_count + 1

    if error_count > 3:
        exit()

    df.at[idx, "buurt"] = buurt
    print(f"✅ Updated row {idx} – buurt: {buurt}")

    # Save file immediately after each update
    df.to_csv(INPUT_OUTPUT_FILE, index=False)

    time.sleep(0.2)  # avoid hammering the API

print("All updates saved.")

