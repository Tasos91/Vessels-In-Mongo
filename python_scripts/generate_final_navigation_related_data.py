import os
import pandas as pd
import json

# === CONFIG ===
BASE_ROOT = "/Users/tasos.sotiriou/Downloads/mongo_db_asignement/ais_synopses"
YEARS = ["2017", "2018", "2019"]

AIS_JSON = "ais_synopses_2017_2018_2019_sorted.json"
FINAL_JSON = "ais_positions_with_static.json"
STATIC_FILE = "/Users/tasos.sotiriou/Documents/Python_Big_Data/vessel_static_mongo.json"

def load_year_csvs(year_path):
    dfs = []
    for file in os.listdir(year_path):
        if file.endswith(".csv"):
            full_path = os.path.join(year_path, file)
            print(f"Loading: {full_path}")
            df = pd.read_csv(full_path)
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


# === Load all years ===
all_years_df = []

for year in YEARS:
    year_path = os.path.join(BASE_ROOT, year)
    print(f"Loading YEAR: {year}")
    year_df = load_year_csvs(year_path)
    all_years_df.append(year_df)

# === CONCAT 2017 + 2018 + 2019 ===
full_df = pd.concat(all_years_df, ignore_index=True)

# === Convert timestamp t to integer ===
full_df["t"] = pd.to_numeric(full_df["t"], errors="coerce").astype("Int64")

# === Sort by timestamp ===
full_df = full_df.sort_values(by="t")

# === Remove duplicates based on unique vessel message ===
full_df = full_df.drop_duplicates(subset=["t", "vessel_id"])

# === Save AIS JSON ===
full_df.to_json(AIS_JSON, orient="records", indent=2)
print(f"✔ AIS JSON created: {AIS_JSON}  ({len(full_df)} rows)")


# === LOAD STATIC VESSEL INFO ===
with open(STATIC_FILE, "r") as f:
    static_data = json.load(f)

# convert static list to dict for fast lookup
static_lookup = {row["vessel_id"]: row for row in static_data}

# === MERGE AIS + STATIC INFO ===
merged_records = []

for row in full_df.to_dict(orient="records"):
    vid = row["vessel_id"]

    static_row = static_lookup.get(vid, {})

    # append static fields
    row.update({
        "country": static_row.get("country"),
        "shiptype": static_row.get("shiptype"),
        "type_description": static_row.get("type_description"),
    })

    merged_records.append(row)

# === SAVE FINAL MERGED JSON ===
with open(FINAL_JSON, "w") as f:
    json.dump(merged_records, f, indent=2)

print("--------------------------------------------------")
print(f"✔ FINAL MERGED JSON created: {FINAL_JSON}")
print(f"✔ Total merged rows: {len(merged_records)}")
print("--------------------------------------------------")