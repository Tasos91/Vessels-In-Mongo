import geopandas as gpd
from pathlib import Path


INPUT_SHP = Path("harbours.shp")       # δώσε full path αν δεν είσαι στο ίδιο folder
OUTPUT_GEOJSON = Path("harbours.geojson")

# === φόρτωση shapefile ===
gdf = gpd.read_file(INPUT_SHP)

# === export σε GeoJSON ===
gdf.to_file(OUTPUT_GEOJSON, driver="GeoJSON")

print(f"Saved: {OUTPUT_GEOJSON.resolve()}")
print(f"Rows: {len(gdf)}  Columns: {list(gdf.columns)}  CRS: {gdf.crs}")
