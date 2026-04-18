# SAA Platform – Portfolio Data Engine

## Overview

This project is a modular Python-based data engine designed to ingest, profile, clean, and validate institutional portfolio datasets for strategic asset allocation (SAA) and portfolio analytics.

The goal is to build a scalable system capable of handling heterogeneous financial datasets from different sources (e.g., banks, asset managers, family offices) with minimal manual intervention.

---

## Why this project?

Financial datasets are often:

- inconsistent across sources
- poorly formatted (numeric values stored as text, mixed date formats, etc.)
- difficult to standardize at scale

This project aims to:

- automate data profiling and cleaning
- detect data quality issues early
- standardize datasets for downstream analytics
- provide transparent and explainable transformations

---

## Current Features (Day 1)

- CSV and Excel (XLSX) ingestion
- Automated dataset profiling
- Column type inference:
  - numeric
  - datetime
  - categorical
  - boolean
  - text
  - mixed
- Missing value analysis
- Duplicate row detection
- Sample values extraction per column
- Column-level issue detection:
  - high missing rate
  - numeric stored as text
  - datetime stored as text
  - mixed formats
  - inconsistent whitespace
- Dataset-level issue summary
- Exported reports:
  - `profile_report.json` (machine-readable)
  - `profile_summary.txt` (human-readable)

---

## Project Structure
saa-platform/
│
├── data/
│   ├── raw/          # input datasets (not tracked)
│   ├── processed/    # cleaned datasets
│   └── reports/      # profiling and validation reports
│
├── docs/             # project documentation
├── logs/             # transformation logs
│
├── src/
│   └── saa_platform/
│       ├── ingestion.py
│       ├── profiling.py
│       ├── reporting.py
│       ├── cleaning.py
│       ├── validation.py
│       └── finance_rules.py
│
├── tests/            # test suite
│
├── clean_data.py     # CLI entry point
├── requirements.txt
└── README.md

---

## Installation

Clone the repository:

```bash
git clone https://github.com/noussaibakr99/saa-platform.git
cd saa-platform

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Usage 
PYTHONPATH=src python clean_data.py data/raw/your_file.xlsx


Roadmap

Automated Cleaning Engine
	•	column name standardization
	•	missing value harmonization
	•	string normalization
	•	duplicate removal
	•	safe type conversion
	•	transformation logging

Validation & Finance Rules
	•	finance-specific validation rules
	•	currency normalization
	•	identifier checks (ISIN, ticker, etc.)
	•	date consistency checks
	•	portfolio-level diagnostics

More to follow 

Long-Term Vision

Build a platform that:
	•	standardizes financial datasets across institutions
	•	enables automated portfolio diagnostics
	•	supports strategic asset allocation workflows
	•	can scale into a client-facing product

⸻

🚀 Quick Start — Run on Your Own Data

Follow these steps to run the SAA Platform on your own dataset.

⸻

1. Clone the Repository

git clone https://github.com/noussaibakr99/saa-platform.git
cd saa-platform


⸻

2. Create a Virtual Environment

python3 -m venv .venv
source .venv/bin/activate


⸻

3. Install the Project

pip install -e .

This installs the tool and enables the saa-clean command.

⸻

4. Run the Tool on Your Dataset

saa-clean /path/to/your_file.xlsx

Example:

saa-clean ~/Desktop/portfolio.xlsx


⸻

⚙️ Optional Commands

Profile only (no cleaning or validation)

saa-clean your_file.xlsx --profile-only

Skip validation

saa-clean your_file.xlsx --clean-only

Custom output folder

saa-clean your_file.xlsx --output-dir results/


⸻

📁 Outputs

After running the tool, the following files will be generated:

data/
├── processed/
│   └── cleaned_data.xlsx
├── reports/
│   ├── profile_report_before.json
│   ├── profile_report_after.json
│   ├── profile_comparison.json
│   └── validation_report.json
logs/
└── cleaning_log.txt


⸻

📌 Supported Input Formats
	•	Excel (.xlsx)
	•	CSV (.csv)

⸻

❓ Help

To see all available options:

saa-clean --help


⸻

💡 Notes
	•	You do NOT need to modify any code
	•	Just provide your dataset path
	•	Outputs are generated automatically
	•	Works on any structured tabular financial dataset

⸻

🎯 Example Full Workflow

git clone https://github.com/noussaibakr99/saa-platform.git
cd saa-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

saa-clean ~/Desktop/client_portfolio.xlsx


⸻

This will automatically:
	•	profile the dataset
	•	clean the data
	•	validate financial consistency
	•	export clean outputs and reports
:::
Author

Noussaiba Krichene
MSc Analytics &  Management– London Business School