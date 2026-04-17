from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


BOOLEAN_TRUE_VALUES = {
    "true",
    "t",
    "yes",
    "y",
    "1",
    "on",
}
BOOLEAN_FALSE_VALUES = {
    "false",
    "f",
    "no",
    "n",
    "0",
    "off",
}
COMMON_NULL_STRINGS = {
    "",
    "na",
    "n/a",
    "nan",
    "null",
    "none",
    "missing",
    "unknown",
    "-",
    "--",
}


def profile_dataset(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a full dataset profile containing:
    - dataset-level summary
    - duplicate statistics
    - column-by-column profiling
    """
    column_profiles = []

    for column_name in df.columns:
        series = df[column_name]
        column_profiles.append(profile_column(series))

    profile = {
        "dataset_summary": get_dataset_summary(df),
        "duplicate_summary": get_duplicate_summary(df),
        "columns": column_profiles,
    }

    return profile


def get_dataset_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Return high-level dataset information.
    """
    return {
        "row_count": int(df.shape[0]),
        "column_count": int(df.shape[1]),
        "memory_usage_bytes": int(df.memory_usage(deep=True).sum()),
        "column_names": [str(col) for col in df.columns],
    }


def get_duplicate_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Return exact duplicate row statistics.
    """
    duplicate_count = int(df.duplicated().sum())
    row_count = int(df.shape[0])

    return {
        "duplicate_row_count": duplicate_count,
        "duplicate_row_rate": safe_divide(duplicate_count, row_count),
    }


def profile_column(series: pd.Series) -> Dict[str, Any]:
    """
    Build a structured profile for one column.
    """
    non_null_series = series.dropna()

    inferred_type = infer_semantic_type(series)
    sample_values = get_sample_values(series)
    issues = detect_column_issues(series, inferred_type)

    unique_count_non_null = int(non_null_series.nunique(dropna=True))
    non_null_count = int(non_null_series.shape[0])
    missing_count = int(series.isna().sum())
    total_count = int(series.shape[0])

    profile = {
        "column_name": str(series.name),
        "raw_dtype": str(series.dtype),
        "inferred_type": inferred_type,
        "row_count": total_count,
        "non_null_count": non_null_count,
        "missing_count": missing_count,
        "missing_rate": round(safe_divide(missing_count, total_count), 6),
        "unique_count_non_null": unique_count_non_null,
        "unique_ratio_non_null": round(
            safe_divide(unique_count_non_null, non_null_count), 6
        ),
        "sample_values": sample_values,
        "issues": issues,
    }

    if inferred_type == "numeric":
        profile["numeric_summary"] = get_numeric_summary(series)

    if inferred_type == "datetime":
        profile["datetime_summary"] = get_datetime_summary(series)

    return profile


def infer_semantic_type(series: pd.Series) -> str:
    """
    Infer a human-meaningful type beyond pandas raw dtype.

    Possible outputs:
    - empty
    - boolean
    - numeric
    - datetime
    - categorical
    - text
    - mixed
    """
    non_null_series = series.dropna()

    if non_null_series.empty:
        return "empty"

    if pd.api.types.is_bool_dtype(series):
        return "boolean"

    if pd.api.types.is_numeric_dtype(series):
        return "numeric"

    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"

    normalized_values = normalize_series_for_inference(non_null_series)

    if normalized_values.empty:
        return "empty"

    bool_ratio = get_boolean_like_ratio(normalized_values)
    numeric_ratio = get_numeric_like_ratio(normalized_values)
    datetime_ratio = get_datetime_like_ratio(normalized_values)

    unique_ratio = safe_divide(
        normalized_values.nunique(dropna=True),
        len(normalized_values),
    )

    if bool_ratio >= 0.95:
        return "boolean"

    if numeric_ratio >= 0.95:
        return "numeric"

    if datetime_ratio >= 0.95:
        return "datetime"

    if should_be_categorical(normalized_values, unique_ratio):
        return "categorical"

    if max(bool_ratio, numeric_ratio, datetime_ratio) >= 0.5:
        return "mixed"

    return "text"


def normalize_series_for_inference(series: pd.Series) -> pd.Series:
    """
    Convert values to normalized strings for heuristic checks.
    """
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned[~cleaned.str.lower().isin(COMMON_NULL_STRINGS)]
    cleaned = cleaned[cleaned != ""]
    return cleaned


def get_boolean_like_ratio(series: pd.Series) -> float:
    lowered = series.str.lower()
    valid = lowered.isin(BOOLEAN_TRUE_VALUES.union(BOOLEAN_FALSE_VALUES))
    return safe_divide(int(valid.sum()), len(series))


def get_numeric_like_ratio(series: pd.Series) -> float:
    standardized = (
        series.str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.strip()
    )
    converted = pd.to_numeric(standardized, errors="coerce")
    return safe_divide(int(converted.notna().sum()), len(series))


def get_datetime_like_ratio(series: pd.Series) -> float:
    converted = pd.to_datetime(series, errors="coerce")
    return safe_divide(int(converted.notna().sum()), len(series))


def should_be_categorical(series: pd.Series, unique_ratio: float) -> bool:
    """
    Heuristic for likely categorical columns.
    """
    unique_count = int(series.nunique(dropna=True))
    row_count = int(len(series))

    if row_count == 0:
        return False

    # Low-cardinality columns are strong categorical candidates.
    if unique_count <= 20:
        return True

    # A relatively small unique ratio also suggests categorical data.
    if row_count >= 50 and unique_ratio <= 0.2:
        return True

    return False


def get_sample_values(series: pd.Series, max_samples: int = 5) -> List[str]:
    """
    Return up to max_samples distinct non-null sample values as strings.
    """
    non_null = series.dropna()

    if non_null.empty:
        return []

    unique_values = []
    seen = set()

    for value in non_null:
        value_str = str(value).strip()
        if value_str not in seen:
            seen.add(value_str)
            unique_values.append(value_str)

        if len(unique_values) >= max_samples:
            break

    return unique_values


def detect_column_issues(series: pd.Series, inferred_type: str) -> List[str]:
    """
    Flag basic column-level issues for review.
    """
    issues = []

    total_count = int(series.shape[0])
    missing_count = int(series.isna().sum())
    missing_rate = safe_divide(missing_count, total_count)

    non_null_series = series.dropna()
    unique_count = int(non_null_series.nunique(dropna=True))

    if total_count == 0:
        issues.append("empty_column")
        return issues

    if missing_rate >= 0.5:
        issues.append("high_missing_rate")

    if not non_null_series.empty and unique_count == 1:
        issues.append("single_unique_value")

    if inferred_type == "mixed":
        issues.append("mixed_format_values")

    if inferred_type == "numeric" and str(series.dtype) == "object":
        issues.append("numeric_stored_as_text")

    if inferred_type == "datetime" and not pd.api.types.is_datetime64_any_dtype(series):
        issues.append("datetime_stored_as_text")

    if inferred_type == "boolean" and str(series.dtype) == "object":
        issues.append("boolean_stored_as_text")

    if inferred_type == "text":
        whitespace_issue = detect_whitespace_issue(non_null_series)
        if whitespace_issue:
            issues.append("inconsistent_whitespace")

    return issues


def detect_whitespace_issue(series: pd.Series) -> bool:
    """
    Detect leading/trailing spaces or repeated internal spaces in text values.
    """
    if series.empty:
        return False

    as_str = series.astype(str)
    has_leading_or_trailing = (as_str != as_str.str.strip()).any()
    has_multiple_spaces = as_str.str.contains(r"\s{2,}", regex=True).any()

    return bool(has_leading_or_trailing or has_multiple_spaces)


def get_numeric_summary(series: pd.Series) -> Dict[str, Any]:
    """
    Produce safe numeric summary after coercion.
    Works both for native numeric columns and text-stored numbers.
    """
    cleaned = (
        series.dropna()
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.strip()
    )

    numeric = pd.to_numeric(cleaned, errors="coerce").dropna()

    if numeric.empty:
        return {}

    return {
        "min": safe_round(numeric.min()),
        "max": safe_round(numeric.max()),
        "mean": safe_round(numeric.mean()),
        "median": safe_round(numeric.median()),
    }


def get_datetime_summary(series: pd.Series) -> Dict[str, Any]:
    """
    Produce safe datetime summary after coercion.
    """
    converted = pd.to_datetime(series, errors="coerce").dropna()

    if converted.empty:
        return {}

    return {
        "min_date": converted.min().isoformat(),
        "max_date": converted.max().isoformat(),
    }


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def safe_round(value: Any, ndigits: int = 6) -> Any:
    """
    Round numeric values for JSON readability.
    """
    try:
        return round(float(value), ndigits)
    except (TypeError, ValueError):
        return value