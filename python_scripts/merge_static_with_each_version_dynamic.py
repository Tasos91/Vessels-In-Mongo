import pandas as pd
import ijson

STATIC_PATH= "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/vessel_static_mongo.json"

DYNAMIC_PATH_2017 = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_dynamic_2017_sorted.json"
DYNAMIC_PATH_2018 = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_dynamic_2018_sorted.json"
DYNAMIC_PATH_2019 = "/Users/tasos.sotiriou/Documents/Python_Big_Data/json_files/ais_dynamic_2019_sorted.json"

years = ["2019"]

NUM_OUTPUT_FILES = 4   # ΘΕΛΕΙΣ 4 JSON output files


for year in years:

    # ------------------------- SELECT FILE -------------------------
    if year == "2017":
        dynamic_path = DYNAMIC_PATH_2017
    elif year == "2018":
        dynamic_path = DYNAMIC_PATH_2018
    elif year == "2019":
        dynamic_path = DYNAMIC_PATH_2019

    print("\nReading STATIC...")
    json_static = pd.read_json(STATIC_PATH)

    print(f"\nCounting objects in {year} JSON...\n")

    # ---------------------------------------------------------
    # FIRST PASS: COUNT TOTAL OBJECTS (streaming)
    # ---------------------------------------------------------
    total_objects = 0
    with open(dynamic_path, "r") as f:
        for _ in ijson.items(f, "item"):
            total_objects += 1

    print(f"Total objects found: {total_objects}")

    chunk_size = total_objects // NUM_OUTPUT_FILES
    print(f"Chunk size per output: {chunk_size}\n")

    # ---------------------------------------------------------
    # SECOND PASS: PROCESS AND WRITE 4 OUTPUT FILES
    # ---------------------------------------------------------
    print(f"Streaming and writing {NUM_OUTPUT_FILES} final JSON files...\n")

    batch = []
    out_index = 1
    processed = 0

    with open(dynamic_path, "r") as f:
        for obj in ijson.items(f, "item"):

            batch.append(obj)
            processed += 1

            # αν το batch συμπληρώσει chunk -> γράψε αρχείο
            if len(batch) >= chunk_size and out_index < NUM_OUTPUT_FILES:

                df = pd.DataFrame(batch)
                merged = df.merge(json_static, on="vessel_id", how="left")
                merged = merged.drop_duplicates()

                output_file = f"dynamic_{year}_PART{out_index}.json"
                merged.to_json(output_file, orient="records", indent=2)
                print(f"Saved {output_file}")

                batch = []  # reset
                out_index += 1

        # ΤΕΛΕΥΤΑΙΟ output (όλα τα leftover)
        df = pd.DataFrame(batch)
        merged = df.merge(json_static, on="vessel_id", how="left")
        merged = merged.drop_duplicates()

        output_file = f"dynamic_{year}_PART{out_index}.json"
        merged.to_json(output_file, orient="records", indent=2)

        print(f"Saved {output_file}")

print("\nALL YEARS COMPLETED.\n")