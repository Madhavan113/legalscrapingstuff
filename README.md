# Amethyst Data Integration

Offline dataset fetcher + normalizer + local store for legal and government data sources.

## Quick Start

```bash
# Install dependencies
pip3 install pyyaml duckdb

# Set FRED API key
export FRED_API_KEY=your_key_here

# Run the Phase 1 manifest
python3 -m src.cli run --manifest manifests/phase1.yaml

# Check status
python3 -m src.cli status --data-dir ./data
```

---

## Manifest-Driven Ingestion

### Running a Manifest

```bash
python3 -m src.cli run --manifest manifests/phase1.yaml
```

Output:
```
2026-01-16 17:47:08 - INFO - Starting ingestion run a84965cd for manifest 'phase1'
2026-01-16 17:47:08 - INFO - Processing 6 datasets
2026-01-16 17:47:13 - INFO -   [SUCCESS] fred_gdp - 319 rows
2026-01-16 17:47:14 - INFO -   [SUCCESS] fred_unemployment - 936 rows
...
Run ID: a84965cd
Status: completed
Datasets: 6 succeeded, 0 skipped, 0 failed
```

### Manifest Format

```yaml
name: phase1
description: "Initial test sources"

sources:
  - id: fred_gdp
    source: fred
    series_id: GDP
    format: JSON
    license: "Public Domain"
    
  - id: datagov_census
    source: data.gov
    query: "population census"
    format: CSV

settings:
  output_dir: ./data
  store_type: duckdb
```

### CLI Options

```bash
# Dry run - show what would be fetched
python3 -m src.cli run --manifest manifests/phase1.yaml --dry-run

# Force re-fetch even if unchanged
python3 -m src.cli run --manifest manifests/phase1.yaml --force

# Override data directory
python3 -m src.cli run --manifest manifests/phase1.yaml --data-dir ./custom_data

# Pass API key
python3 -m src.cli run --manifest manifests/phase1.yaml --api-key YOUR_KEY
```

---

## Data Storage Structure

```
data/
├── raw/                        # Verbatim downloaded files
│   ├── fred/
│   │   └── fred_gdp_20260116_224713.json
│   └── data_gov/
│       └── datagov_crime_20260116_224803.csv
│
├── normalized/                 # Cleaned, standardized data
│   ├── fred/
│   │   └── fred_gdp.json       # Schema + typed data
│   └── data_gov/
│       └── datagov_crime.json
│
└── metadata.duckdb             # Ingestion history + dataset registry
```

### Normalized Data Format

```json
{
  "schema": {
    "fields": [
      {"name": "date", "type": "date", "nullable": false},
      {"name": "value", "type": "number", "nullable": false}
    ],
    "row_count": 319
  },
  "data": [
    {"date": "1947-01-01", "value": 243.1},
    ...
  ]
}
```

### Querying Metadata (DuckDB)

```bash
python3 -c "
import duckdb
conn = duckdb.connect('./data/metadata.duckdb')
print(conn.execute('SELECT * FROM datasets').fetchdf())
"
```

---

## Check Status

```bash
python3 -m src.cli status --data-dir ./data
```

Output:
```
Data directory: /path/to/data
Store type: duckdb

Datasets (6):
  - fred_gdp (fred) - last updated: 2026-01-16 22:47:13
  - fred_unemployment (fred) - last updated: 2026-01-16 22:47:14
  ...

Recent runs:
  - a84965cd (phase1) - completed - 2026-01-16 22:47:08
```

---

## Supported Sources

| Source | Type | Identifier |
|--------|------|------------|
| data.gov | CKAN API | `dataset_id` or `query` |
| FRED | REST API | `series_id` |

---

## Key Features

- **Manifest-driven** — YAML/JSON config defines exactly what to fetch
- **Deterministic** — Same manifest → same outputs
- **Change detection** — Content hashing skips unchanged datasets
- **Versioned** — Raw snapshots preserved with timestamps
- **Normalized** — Consistent column names, date formats, types
- **Queryable metadata** — DuckDB for SQL access to history

---

## Legacy CLI (still supported)

```bash
# Query-based fetch
python3 -m src.cli fetch data.gov --query "health" --limit 5 --output ./output

# FRED series
python3 -m src.cli fetch fred --query "GDP" --limit 5 --api-key YOUR_KEY
```

---

## Phase 1 Guinea Pig Datasets

The included `manifests/phase1.yaml` contains:

| ID | Source | Description |
|----|--------|-------------|
| fred_gdp | FRED | Gross Domestic Product |
| fred_unemployment | FRED | Unemployment Rate |
| fred_inflation | FRED | Consumer Price Index |
| fred_interest_rate | FRED | Federal Funds Rate |
| datagov_population | data.gov | Census population estimates |
| datagov_crime | data.gov | FBI crime statistics |
