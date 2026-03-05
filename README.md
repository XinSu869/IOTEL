# IoT Data Processing Tool

A local, Flask-based web application designed to process IoT CSV data into a common IoT schema, explore Object-Centric Event Logs (OCEL), and seamlessly integrate IoT data into event logs. No external services or network calls are required.

## Features

![alt text](<static/images/The Architecture of IoTEL.png>)

The tool is composed of three main features:
1. **Process IoT Data**: Upload, profile, and transform IoT data into the common IoT schema.
2. **Explore OCEL**: Upload and explore Object-Centric Event Logs (OCEL) formats.
3. **Integrate IoT into OCEL**: Integrate processed IoT data into Object-Centric Event Logs to generate a IoT-enriched Object-Centric Event Log.

## Using The App

### Using Python/Conda

#### Setup

```bash
conda create -n mytool python=3.10
conda activate mytool
pip install -r requirements.txt
```

Feel free to use `python -m venv` instead of Conda if you prefer standard virtual environments.

#### Running The App

```bash
python app.py
```
The development server starts on `http://127.0.0.1:5000/`.

### Using Docker
You can also run the application using Docker. Simply use Docker Compose:
```bash
docker-compose up --build
```
The container starts the web application on `http://127.0.0.1:5001/`. Volumes are mapped to keep your data persistent on your host machine.

### Use the interface to:

1. **Upload** IoT CSV files or an OCEL SQLite file via the Home page.
2. **Process IoT Data**: Use the IoT Files overview to review statuses, Profile the dataset, Adjust & Map columns to the common IoT schema, and Combine & Export the results as `.parquet`.
3. **Explore OCEL**: Browse the Event Log Overview to view objects, events, and temporal distributions of your uploaded OCEL data.
4. **Integrate**: Follow the 6-step process to integrate the processed IoT data into the OCEL event logs.

Warnings are surfaced in-app when uploads are rejected, data parsing fails, or combined IoT data/integrated database encounter issues.

## Directory Overview

- `uploads/` – raw CSV and SQLite uploads saved from the UI.
- `adjusted/` – per-source parquet files after overrides/mappings.
- `processed/` – combined parquet dataset generated via DuckDB.
- `integrated/` – final integrated datasets combining IoT data and event logs.
- `dataset/` – sample datasets provided for trying out the tool.
- `iot_processing/` – core IoT data processing package (storage, profiling, adjustments, combine, preview).
- `integration/` – core package for OCEL exploration and IoT-to-OCEL integration logic.
- `static/js/` – front-end scripts (e.g., live preview for Adjust & Map, dynamic step loading).
- `templates/` – Flask templates for all UI pages.
- `config/name_list.json` – reference list of the Common IoT Schema.
- `common_iot_schema/` - Collected public IoT datasets that are used to verify the common IoT schema.

## Notes

- IoT data must be in `.csv` format; Event Logs must be in SQLite format (`.db`, `.sqlite`, `.sqlite3`).
- Datetime overrides fall back to text with a warning if parsing fails.
- Combining requires all IoT files to be processed; empty results produce a clear error.
- Setting up the integration requires both a combined IoT dataset and an uploaded OCEL event log.

## Introduction to Parquet

Parquet is a columnar storage file format that offers significant performance improvements and space savings over CSV for analytical queries. It is built to support very efficient compression and encoding schemes, making it ideal for processing large datasets.

Here is a piece of Python code to open and review the Parquet data using `pandas`:

```python
import pandas as pd

# Path to your parquet file
file_path = 'processed/combined.parquet'

# Read the parquet file into a pandas DataFrame
df = pd.read_parquet(file_path)

# Display the first few rows of the data
print(df.head())

# Review the schema and basic information
print(df.info())
```

*(Note: You may need to install pandas and a parquet engine like pyarrow or fastparquet: `pip install pandas pyarrow`)*
