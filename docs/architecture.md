## B. `docs/architecture.md`
# Architecture

## Current pipeline

1. `ingestion.py`
   - loads CSV/XLSX files into pandas DataFrames

2. `profiling.py`
   - profiles dataset structure
   - infers semantic column types
   - detects basic quality issues
   - builds dataset-level and column-level profile objects

3. `reporting.py`
   - writes JSON and text reports to disk

4. `clean_data.py`
   - orchestrates the pipeline from command line input to final output files

## Current outputs
- `data/reports/profile_report.json`
- `data/reports/profile_summary.txt`

## Next planned modules
- `cleaning.py`
- `validation.py`
- `finance_rules.py`