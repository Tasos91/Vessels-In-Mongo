import pandas as pd

SYNOPSES_PATH= "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_synopses_2017_2018_2019_sorted.json"

DYNAMIC_PATH_2017 = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_dynamic_2017_sorted.json"
DYNAMIC_PATH_2018 = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_dynamic_2018_sorted.json"
DYNAMIC_PATH_2019 = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_dynamic_2019_sorted.json"
DYNAMIC_PATH_GENERAL = ""

years = ["2017", "2018", "2019"]

json_final = {}

for year in years:
    if year == "2017":
        DYNAMIC_PATH_GENERAL = DYNAMIC_PATH_2017
    elif year == "2018":
        DYNAMIC_PATH_GENERAL = DYNAMIC_PATH_2018
    elif year == "2019":    
        DYNAMIC_PATH_GENERAL = DYNAMIC_PATH_2019   
    print("\n read json for year --------------------------------------------------" + year)
    json_synopses = pd.read_json(SYNOPSES_PATH)
    json_dynamic = pd.read_json(DYNAMIC_PATH_GENERAL)

    print("\n to numeric --------------------------------------------------")

    json_synopses["t"] = pd.to_numeric(json_synopses["t"], errors="coerce").astype("Int64")
    json_dynamic["t"] = pd.to_numeric(json_dynamic["t"], errors="coerce").astype("Int64")

    print("\n year final json --------------------------------------------------")

    json_final = json_synopses.merge(json_dynamic, on=["vessel_id", "t"], how="left")

json_final.to_json("ais_synopses_with_dynamic_2017_2018_2019_FINAL.json", orient="records", indent=2)
print("\n ALL YEARS FINAL json --------------------------------------------------")
