import json
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd


def ensure_directory(path: Path) -> None:
    """
    Create the output directory if it does not already exist.
    """
    path.mkdir(parents=True, exist_ok=True)


def write_json_report(report: Dict[str, Any], output_path: Union[str, Path]) -> None:
    """
    Write a Python dictionary to a JSON file.
    """
    path = Path(output_path)
    ensure_directory(path.parent)

    with path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=False, default=str)


def build_text_summary(report: Dict[str, Any]) -> str:
    """
    Build a human-readable text summary from a profiling report.
    """
    dataset_summary = report.get("dataset_summary", {})
    duplicate_summary = report.get("duplicate_summary", {})
    issue_summary = report.get("issue_summary", {})
    columns = report.get("columns", [])

    lines = []
    lines.append("DATASET PROFILE SUMMARY")
    lines.append("=" * 60)
    lines.append(f"Rows: {dataset_summary.get('row_count', 0)}")
    lines.append(f"Columns: {dataset_summary.get('column_count', 0)}")
    lines.append(
        f"Duplicate rows: {duplicate_summary.get('duplicate_row_count', 0)} "
        f"({duplicate_summary.get('duplicate_row_rate', 0):.2%})"
    )
    lines.append("")

    lines.append("ISSUE SUMMARY")
    lines.append("-" * 60)
    if issue_summary:
        for issue_name, count in issue_summary.items():
            lines.append(f"- {issue_name}: {count}")
    else:
        lines.append("No dataset-level issues detected.")
    lines.append("")

    lines.append("COLUMN OVERVIEW")
    lines.append("-" * 60)

    for col in columns:
        lines.append(f"Column: {col.get('column_name')}")
        lines.append(f"  Raw dtype: {col.get('raw_dtype')}")
        lines.append(f"  Inferred type: {col.get('inferred_type')}")
        lines.append(
            f"  Missing: {col.get('missing_count')} "
            f"({col.get('missing_rate', 0):.2%})"
        )
        lines.append(f"  Unique non-null: {col.get('unique_count_non_null')}")
        lines.append(f"  Sample values: {col.get('sample_values', [])}")
        issues = col.get("issues", [])
        lines.append(f"  Issues: {issues if issues else 'None'}")
        lines.append("")

    return "\n".join(lines)


def write_text_summary(report: Dict[str, Any], output_path: Union[str, Path]) -> None:
    """
    Write a human-readable text summary for a profiling report.
    """
    path = Path(output_path)
    ensure_directory(path.parent)

    summary_text = build_text_summary(report)

    with path.open("w", encoding="utf-8") as file:
        file.write(summary_text)


def write_cleaned_dataset(df: pd.DataFrame, output_path: Union[str, Path]) -> None:
    """
    Export the cleaned dataset to Excel.
    """
    path = Path(output_path)
    ensure_directory(path.parent)
    df.to_excel(path, index=False)


def build_cleaning_log_text(cleaning_report: Dict[str, Any]) -> str:
    """
    Build a readable cleaning log from the structured cleaning report.
    """
    summary = cleaning_report.get("summary", {})
    steps = cleaning_report.get("steps", [])

    lines: List[str] = []
    lines.append("CLEANING LOG")
    lines.append("=" * 60)
    lines.append(f"Rows before: {summary.get('row_count_before', 0)}")
    lines.append(f"Rows after: {summary.get('row_count_after', 0)}")
    lines.append(f"Columns before: {summary.get('column_count_before', 0)}")
    lines.append(f"Columns after: {summary.get('column_count_after', 0)}")
    lines.append(f"Total rows removed: {summary.get('rows_removed_total', 0)}")
    lines.append("")

    lines.append("CLEANING STEPS")
    lines.append("-" * 60)

    for step in steps:
        step_name = step.get("step", "unknown_step")
        lines.append(f"Step: {step_name}")

        for key, value in step.items():
            if key == "step":
                continue
            lines.append(f"  {key}: {value}")

        lines.append("")

    return "\n".join(lines)


def write_cleaning_log(cleaning_report: Dict[str, Any], output_path: Union[str, Path]) -> None:
    """
    Write the cleaning log to a text file.
    """
    path = Path(output_path)
    ensure_directory(path.parent)

    log_text = build_cleaning_log_text(cleaning_report)

    with path.open("w", encoding="utf-8") as file:
        file.write(log_text)


def build_profile_comparison(before_report: Dict[str, Any], after_report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare selected high-value profiling metrics before vs after cleaning.
    """
    before_duplicates = before_report.get("duplicate_summary", {}).get("duplicate_row_count", 0)
    after_duplicates = after_report.get("duplicate_summary", {}).get("duplicate_row_count", 0)

    before_issues = before_report.get("issue_summary", {})
    after_issues = after_report.get("issue_summary", {})

    tracked_issues = [
        "high_missing_rate",
        "numeric_stored_as_text",
        "datetime_stored_as_text",
        "inconsistent_whitespace",
    ]

    issue_comparison = {}
    for issue_name in tracked_issues:
        before_count = before_issues.get(issue_name, 0)
        after_count = after_issues.get(issue_name, 0)

        issue_comparison[issue_name] = {
            "before": before_count,
            "after": after_count,
            "improvement": before_count - after_count,
        }

    comparison = {
        "duplicates": {
            "before": before_duplicates,
            "after": after_duplicates,
            "improvement": before_duplicates - after_duplicates,
        },
        "issues": issue_comparison,
    }

    return comparison


def build_profile_comparison_text(comparison: Dict[str, Any]) -> str:
    """
    Build a readable comparison summary between raw and cleaned profiling reports.
    """
    duplicates = comparison.get("duplicates", {})
    issues = comparison.get("issues", {})

    lines: List[str] = []
    lines.append("BEFORE / AFTER CLEANING COMPARISON")
    lines.append("=" * 60)
    lines.append(
        f"Duplicate rows: before={duplicates.get('before', 0)}, "
        f"after={duplicates.get('after', 0)}, "
        f"improvement={duplicates.get('improvement', 0)}"
    )
    lines.append("")
    lines.append("ISSUE COMPARISON")
    lines.append("-" * 60)

    for issue_name, values in issues.items():
        lines.append(
            f"{issue_name}: "
            f"before={values.get('before', 0)}, "
            f"after={values.get('after', 0)}, "
            f"improvement={values.get('improvement', 0)}"
        )

    return "\n".join(lines)


def write_profile_comparison(
    before_report: Dict[str, Any],
    after_report: Dict[str, Any],
    json_output_path: Union[str, Path],
    text_output_path: Union[str, Path],
) -> None:
    """
    Write both JSON and text versions of the before/after comparison.
    """
    comparison = build_profile_comparison(before_report, after_report)
    write_json_report(comparison, json_output_path)

    path = Path(text_output_path)
    ensure_directory(path.parent)

    comparison_text = build_profile_comparison_text(comparison)
    with path.open("w", encoding="utf-8") as file:
        file.write(comparison_text)