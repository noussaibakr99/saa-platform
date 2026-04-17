# Data Dictionary

This file documents the meaning and expected analytical role of important columns in the synthetic institutional portfolio dataset.

## Initial interpretation approach
For each column, document:
- business meaning
- likely data type
- relevance for strategic asset allocation
- expected quality checks

## Example entries

### portfolio_code
- Meaning: portfolio or mandate identifier
- Likely type: categorical / identifier
- Relevance: groups holdings into portfolios
- Checks: missing values, duplicates, formatting consistency

### strategy_code
- Meaning: investment strategy classification
- Likely type: categorical
- Relevance: useful for portfolio segmentation and policy analysis
- Checks: standard labels, missingness

### currency
- Meaning: currency of denomination
- Likely type: categorical
- Relevance: FX exposure and valuation analysis
- Checks: valid ISO-like codes, uppercase consistency

### market_value_chf
- Meaning: holding market value converted to CHF
- Likely type: numeric
- Relevance: key for exposure and allocation analysis
- Checks: numeric parsing, missing values, suspicious negatives

### maturity_date
- Meaning: contractual end date for fixed-income-like instruments
- Likely type: datetime
- Relevance: duration and liquidity analysis
- Checks: valid dates, consistency with instrument type