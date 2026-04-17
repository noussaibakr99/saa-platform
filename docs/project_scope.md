# Project Scope

## Project name
SAA Platform

## Vision
Build a modular Python platform that ingests, profiles, cleans, validates, and standardizes institutional portfolio datasets for strategic asset allocation and portfolio analytics.

## Day 1 objective
Create a reusable profiling engine that can automatically inspect unknown CSV/XLSX datasets and generate structured quality diagnostics without manual code changes.

## Day 1 deliverables
- CSV/XLSX ingestion
- Dataset summary
- Column-level profiling
- Type inference
- Missing value analysis
- Duplicate detection
- Sample values per column
- JSON profile report
- Human-readable text summary

## Success criteria reached
A user can run:

```bash
python clean_data.py <input_file>