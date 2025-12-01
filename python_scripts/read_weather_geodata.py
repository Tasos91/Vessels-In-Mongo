import geopandas as gpd
import json
import os

#BASE_DIR_WEATHER = "/Users/tasos.sotiriou/Downloads/mongo_db_asignement/noaa_weather"
BASE_DIR_GEODATA = "/Users/tasos.sotiriou/Downloads/mongo_db_asignement/geodata/regions"

def process_shapefile(shp_path):
    print(f"\n→ Reading Shapefile: {shp_path}")

    try:
        gdf = gpd.read_file(shp_path)
    except Exception as e:
        print(f"❌ ERROR reading shapefile: {e}")
        return

    print(f"✔ Records loaded: {len(gdf)}")

    geojson_dict = json.loads(gdf.to_json())

    base_name = os.path.basename(shp_path).replace(".shp", "")
    output_file = f"{base_name}_mongo.json"

    with open(output_file, "w") as f:
        json.dump(geojson_dict["features"], f, indent=2)

    print(f"✔ Saved → {output_file}")

# for root, dirs, files in os.walk(BASE_DIR_WEATHER):
#     for file in files:
#         if file.endswith(".shp"):
#             full_path = os.path.join(root, file)
#             process_shapefile(full_path)


for root, dirs, files in os.walk(BASE_DIR_GEODATA):
    for file in files:
        if file.endswith(".shp"):
            full_path = os.path.join(root, file)
            process_shapefile(full_path)
