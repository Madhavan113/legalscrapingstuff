# Legal Scraping Stuff

## Data.gov ingestion

Use the CLI or a JSON config to pull datasets from data.gov (CKAN) and normalize the metadata. You can optionally cache CKAN pages to disk for offline ingestion runs.

### JSON output

```bash
python -m src.cli data.gov --query "health" --limit 5 --format json --output ./output
```

Expected output:

```text
Wrote 5 datasets to ./output/data.gov_datasets_YYYYMMDDHHMMSS.json
```

The JSON file contains a list of normalized datasets:

```json
[
  {
    "title": "Example dataset",
    "description": "Dataset description",
    "resources": [
      {
        "url": "https://example.com/data.csv",
        "format": "CSV"
      }
    ]
  }
]
```

### CSV output

```bash
python -m src.cli data.gov --query "climate" --limit 10 --format csv --output ./output
```

Expected output:

```text
Wrote 10 datasets to ./output/data.gov_datasets_YYYYMMDDHHMMSS.csv
```

The CSV file includes `title`, `description`, and `resources` columns where `resources` is JSON-encoded.

### Offline ingestion workflow

First, run a cached ingestion to store CKAN responses locally:

```bash
python -m src.cli data.gov --query "justice" --limit 50 --format json --output ./output --cache-dir ./cache
```

Then, re-run using the cached pages without any network calls:

```bash
python -m src.cli data.gov --offline --cache-dir ./cache --format json --output ./output
```

Expected output:

```text
Wrote 50 datasets to ./output/data.gov_datasets_YYYYMMDDHHMMSS.json
```

### No-code config-driven ingestion

Create a JSON config file (e.g., `ingest.json`):

```json
{
  "source": "data.gov",
  "query": "justice",
  "limit": 50,
  "output": "./output",
  "format": "json",
  "cache_dir": "./cache",
  "offline": true
}
```

Run the ingestion using the config file:

```bash
python -m src.ingest ingest.json
```

Expected output:

```text
Wrote datasets to ./output/data.gov_datasets_YYYYMMDDHHMMSS.json
```
