# AIS & Environmental Data Processing Pipeline using MongoDB  
A complete data engineering workflow for transforming, enriching, and storing large-scale AIS, geospatial, and weather datasets in a non-relational database.

---

##  Overview

This repository contains a full data processing pipeline that:

- Converts raw AIS, meteorological, and geospatial data (CSV, SHP, JSON) into a unified JSON format.
- Cleans, normalizes, and merges heterogeneous datasets.
- Creates enriched documents by combining AIS dynamic data with vessel metadata, synopses/annotations, and weather information.
- Stores the processed data efficiently in **MongoDB**, using streaming inserts for large files.
- Supports geospatial queries, time-based queries, filtering, and analytical aggregations.

The project implements a scalable, modular approach suitable for large datasets (5GB+) and environments with limited RAM.
