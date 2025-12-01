import ijson
import json
from decimal import Decimal

SYNOPSES_PATH = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_synopses_2017_2018_2019_sorted.json"

DYNAMIC_PATHS = [
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2017.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2018_PART1.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2018_PART2.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2018_PART3.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2018_PART4.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2019_PART1.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2019_PART2.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2019_PART3.json",
"/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_static_of_vessel/dynamic_merge_with_static_of_vessel_2019_PART4.json"
]

OUTPUT_PATH = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/dynamic_merge_with_synopses/ais_synopses_final.json"


def convert_decimals(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    else:
        return obj


print("Loading SYNOPSES into memory once...\n")

# ------------------------------------------------------------
# LOAD SYNOPSES ONE TIME IN RAM
# ------------------------------------------------------------
syn_list = []
with open(SYNOPSES_PATH, "r") as f:
    for syn in ijson.items(f, "item"):
        syn_list.append(syn)

print(f"Loaded synopses entries: {len(syn_list)}\n")


# Build fast lookup: key = (vessel_id, t)
syn_index = {(s["vessel_id"], s["t"]): idx for idx, s in enumerate(syn_list)}
print("Built synopses index.\n")


# ------------------------------------------------------------
# PROCESS EACH DYNAMIC PART (streaming)
# ------------------------------------------------------------
for DYNAMIC_PATH in DYNAMIC_PATHS:
    print(f"Processing dynamic file: {DYNAMIC_PATH}")
    trick_hack_print_timestamp = ""
    trick_hack_print_t = ""
    with open(DYNAMIC_PATH, "r") as f_dyn:
        for dyn in ijson.items(f_dyn, "item"):

            key = (dyn.get("vessel_id"), dyn.get("timestamp"))
            if key in syn_index:
                idx = syn_index[key]
                syn_list[idx]["course"] = dyn.get("course")
                if trick_hack_print_timestamp == "":
                    print("Found match on 'timestamp' key for dynamic: " + DYNAMIC_PATH)
                    trick_hack_print_timestamp = "trick_hack_print_done"
            
            # Also check for "t" key if "timestamp" not found
            key = (dyn.get("vessel_id"), dyn.get("t"))    
            if key in syn_index:
                idx = syn_index[key]
                syn_list[idx]["course"] = dyn.get("course")    
                if trick_hack_print_t == "":
                    print("Found match on 't' key for dynamic: " + DYNAMIC_PATH)
                    trick_hack_print_t = "trick_hack_print_done"

    print("Done with part.\n")


# ------------------------------------------------------------
# WRITE FINAL OUTPUT
# ------------------------------------------------------------

print("Writing final merged synopses...\n")

with open(OUTPUT_PATH, "w") as out:
    out.write("[\n")
    first = True

    for s in syn_list:
        safe = convert_decimals(s)

        if first:
            out.write(json.dumps(safe, indent=2))
            first = False
        else:
            out.write(",\n" + json.dumps(safe, indent=2))

    out.write("\n]\n")

print("DONE. Final file saved at:")
print(OUTPUT_PATH)