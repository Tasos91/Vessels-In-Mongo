import json

INPUT_FILE = "/Users/tasos.sotiriou/Documents/Python_Big_Data/ais_positions_with_static.json"
OUTPUT_FILE = "ais_positions_FIRST_1000.json"

with open(INPUT_FILE, "r") as f:
    data = json.load(f)

first_10 = data[:10000]

with open(OUTPUT_FILE, "w") as f:
    json.dump(first_10, f, indent=2)

print("✔ Saved:", OUTPUT_FILE)
