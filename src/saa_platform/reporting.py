import json
from pathlib import Path
from typing import Any, Dict, Union

import json
from pathlib import Path
from typing import Any, Dict, Union


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json_report(report: Dict[str, Any], output_path: Union[str, Path]) -> None:
    path = Path(output_path)
    ensure_directory(path.parent)

    with path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=False)


def build_text_summary(report: Dict[str, Any]) -> str:
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
    path = Path(output_path)
    ensure_directory(path.parent)

    summary_text = build_text_summary(report)

    with path.open("w", encoding="utf-8") as file:
        file.write(summary_text)