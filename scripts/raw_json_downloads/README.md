# Raw JSON Download Scripts

This directory contains one standalone script per dataset in `core/catalog.py`.

## Run one dataset script

```bash
python scripts/raw_json_downloads/fred_us_gdp.py
```

## Run all datasets

```bash
python scripts/raw_json_downloads/run_all.py
```

## Output

Each script saves yearly raw responses to:

```text
data/raw_json/<group_and_dataset_slug>/<year>.json
```

Year range is fixed to `1995` through `2026` in `scripts/raw_json_downloads/_common.py`.

Each dataset folder also includes `_summary.json` with:

- yearly `status` (`ok` or `error`)
- `error_type`
- `recommended_action`

`run_all.py` writes an aggregate run report to:

```text
data/raw_json/_runs/run_all_<timestamp>.json
```

## Reliability behavior

- Uses `tenacity` retries with exponential backoff + jitter for transient failures.
- Retries HTTP `429`, `500`, `502`, `503`, `504` and network exceptions.
- Applies per-source pacing to reduce burst/rate-limit failures.

## API keys

Keys are loaded in this order:

1. Environment variables (`FRED_API_KEY`, `BLS_API_KEY`)
2. `.streamlit/secrets.toml` in the project root
