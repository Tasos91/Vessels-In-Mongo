# AIS Big Data Management in MongoDB

## Project Overview

Αυτό το project υλοποιεί μια ολοκληρωμένη λύση **NoSQL (MongoDB)** για τη διαχείριση και ανάλυση θαλάσσιων δεδομένων μεγάλης κλίμακας (Big Data). Σκοπός είναι η δημιουργία "εμπλουτισμένων τροχιών πλοίων" (enriched vessel trajectories) συνδυάζοντας διαφορετικές πηγές δεδομένων:

* **Dynamic Data:** Θέση, ταχύτητα και πορεία πλοίων σε πραγματικό χρόνο.
* **Static Data:** Όνομα πλοίου, τύπος, εθνικότητα και διαστάσεις.
* **Synopses:** Περιληπτικά δεδομένα ταξιδιών (trips).
* **External Data:** Μετεωρολογικά δεδομένα (Weather) και γεωγραφικές περιοχές (Geodata).

**Πηγή Δεδομένων:** Zenodo Record 6323416.

---

## Prerequisites

Για την εκτέλεση των script απαιτούνται οι παρακάτω βιβλιοθήκες της Python:

```bash
pip install pandas pymongo geopandas ijson

```

> [!IMPORTANT]
> Η βιβλιοθήκη **ijson** χρησιμοποιείται για το streaming αρχείων JSON μεγέθους 5GB+, αποφεύγοντας σφάλματα μνήμης (Memory Errors).

---

##  Execution Pipeline (Step-by-Step)

Ακολουθήστε την παρακάτω σειρά για την αναπαραγωγή της βάσης δεδομένων.

### Phase 1: Data Pre-processing (External Data)

1. **Prepare Geographic Regions**
* **Script:** `read_weather_geodata.py`
* **Action:** Μετατρέπει Shapefiles (.shp) σε μορφή **GeoJSON**.
* **Why:** Η MongoDB απαιτεί GeoJSON για την εκτέλεση χωρικών ερωτημάτων (spatial queries).


2. **Prepare Weather Data**
* **Script:** `change_json_structure.py`
* **Action:** Αναδιαρθρώνει το αρχικό JSON του καιρού (flattening properties).
* **Why:** Η MongoDB αποδίδει καλύτερα με πίνακες αντικειμένων για ευκολότερη ευρετηρίαση (indexing).



### Phase 2: AIS Data Enrichment & Merging

3. **Create Base Navigation Data**
* **Script:** `generate_final_navigation_data.py`
* **Action:** Συνενώνει τα raw CSV (2017-2019), τα ταξινομεί χρονικά και τα συγχωνεύει με το `vessel_static_mongo.json`.


4. **Optimize Large File Merging (Chunking)**
* **Script:** `merge_static_with_each_version_dynamic.py`
* **Action:** Χωρίζει τα δεδομένα σε μικρότερα τμήματα (PART1, PART2) λόγω του μεγάλου όγκου τους.


5. **Create Final Enriched Synopses**
* **Script:** `merge_dynamic_with_synopses.py`
* **Action:** **Κρίσιμο στάδιο.** Συνδέει τα Synopses με τα Dynamic δεδομένα χρησιμοποιώντας **ijson**.
* **Output:** `ais_synopses_final.json`.



### Phase 3: Database Loading

6. **Load Main Trajectory Data**
* **Script:** `connection_with_mongo.py`
* **Action:** Εισάγει το τελικό αρχείο στη συλλογή `navigation_related_and_meta` σε batches των 5000 εγγραφών.


7. **Load Weather Data**
* **Script:** `insert_weather_related_data.py`
* **Action:** Φορτώνει τα επεξεργασμένα δεδομένα καιρού στη συλλογή `weather_related`.



---

## File Dictionary

| Filename | Purpose |
| --- | --- |
| `read_weather_geodata.py` | Μετατροπή .shp σε GeoJSON για τη MongoDB. |
| `generate_final_navigation_data.py` | Σύνδεση CSV (2017-19) με Static δεδομένα. |
| `merge_dynamic_with_synopses.py` | **ΚΥΡΙΟ SCRIPT:** Σύνδεση Synopses με Dynamic δεδομένα (ijson). |
| `connection_with_mongo.py` | Μαζική εισαγωγή δεδομένων στη MongoDB (Streaming). |
| `generate_1000_records_from_file.py` | Utility για τη δημιουργία δείγματος (sample) για δοκιμές. |

---

## Configuration Notes

Πριν την εκτέλεση, ενημερώστε τις διαδρομές αρχείων (paths) στα scripts:

* `JSON_PATH`: Η τοποθεσία των αρχείων δεδομένων.
* `MONGO_URI`: Default είναι `localhost:27017`.

---