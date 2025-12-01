import json

INPUT_PATH = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/geodata/weather_related.json"
OUTPUT_PATH = "weather_releated_pre_processed.json"

with open(INPUT_PATH, "r") as f:
    data = json.load(f)

result = []

for key, value in data.items():
    obj = {
        "id": value.get("id", key),   # keep original id or fallback to key
        "properties": value.get("properties", [])
    }
    result.append(obj)

# Save final JSON array
with open(OUTPUT_PATH, "w") as f:
    json.dump(result, f, indent=2)

print("DONE → Converted to array format!")
print("Saved to:", OUTPUT_PATH)
