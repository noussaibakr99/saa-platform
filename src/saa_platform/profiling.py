from __future__ import annotations

import warnings
from typing import Any, Dict, List, Tuple

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

# Common formats used for safer datetime profiling.
COMMON_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%d.%m.%Y",
    "%Y.%m.%d",
]


def profile_dataset(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a full dataset profile containing:
    - dataset-level summary
    - duplicate statistics
    - dataset-level issue counts
    - column-by-column profiling
    """
    column_profiles = []

    for column_name in df.columns:
        series = df[column_name]
        column_profiles.append(profile_column(series))

    profile = {
        "dataset_summary": get_dataset_summary(df),
        "duplicate_summary": get_duplicate_summary(df),
        "issue_summary": get_issue_summary(column_profiles),
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

    profile["recommendations"] = generate_recommendations(profile)

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
    """
    Detect date-like columns while avoiding pandas format-inference warnings.
    """
    success_count, _best_format = try_parse_dates_with_best_strategy(series)
    return safe_divide(success_count, len(series))


def try_parse_dates_with_best_strategy(series: pd.Series) -> Tuple[int, str]:
    """
    Try several common formats first, then a generic parser.
    Returns:
    - best success count
    - best parser label
    """
    best_success_count = -1
    best_format = "none"

    series_as_str = series.astype(str).str.strip()

    # Try explicit formats first.
    for date_format in COMMON_DATE_FORMATS:
        converted_try = pd.to_datetime(series_as_str, format=date_format, errors="coerce")
        success_count_try = int(converted_try.notna().sum())

        if success_count_try > best_success_count:
            best_success_count = success_count_try
            best_format = date_format

    # Fall back to the generic parser, but suppress the noisy warning.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        generic_converted = pd.to_datetime(series_as_str, errors="coerce")

    generic_success_count = int(generic_converted.notna().sum())

    if generic_success_count > best_success_count:
        best_success_count = generic_success_count
        best_format = "generic_parser"

    return best_success_count, best_format


def should_be_categorical(series: pd.Series, unique_ratio: float) -> bool:
    """
    Heuristic for likely categorical columns.
    """
    unique_count = int(series.nunique(dropna=True))
    row_count = int(len(series))

    if row_count == 0:
        return False

    if unique_count <= 20:
        return True

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


def generate_recommendations(column_profile: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Generate user-friendly recommended actions for a column based on its issues.
    """
    recommendations = []

    issues = column_profile.get("issues", [])
    inferred_type = column_profile.get("inferred_type")
    missing_rate = column_profile.get("missing_rate", 0)

    if "numeric_stored_as_text" in issues:
        recommendations.append({
            "message": "Likely numeric column stored as text — convert to numeric type.",
            "severity": "warning",
        })

    if "datetime_stored_as_text" in issues:
        recommendations.append({
            "message": "Likely date column stored as text — review and standardize date format.",
            "severity": "warning",
        })

    if "boolean_stored_as_text" in issues:
        recommendations.append({
            "message": "Likely boolean column stored as text — consider boolean conversion.",
            "severity": "info",
        })

    if missing_rate > 0.5:
        recommendations.append({
            "message": "High missingness — inspect business relevance before using this column.",
            "severity": "warning",
        })

    if "mixed_format_values" in issues:
        recommendations.append({
            "message": "Mixed formats detected — manual review recommended before automated use.",
            "severity": "critical",
        })

    if "single_unique_value" in issues:
        recommendations.append({
            "message": "Column has a single non-null value — check whether it is analytically useful.",
            "severity": "info",
        })

    if inferred_type == "categorical":
        recommendations.append({
            "message": "Consider category normalization or mapping for consistent reporting.",
            "severity": "info",
        })

    return recommendations


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
    Produce safe datetime summary after coercion, while avoiding noisy warnings.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        converted = series.dropna()
    else:
        series_non_null = series.dropna().astype(str).str.strip()

        best_success_count = -1
        best_converted = None

        for date_format in COMMON_DATE_FORMATS:
            converted_try = pd.to_datetime(series_non_null, format=date_format, errors="coerce")
            success_count_try = int(converted_try.notna().sum())

            if success_count_try > best_success_count:
                best_success_count = success_count_try
                best_converted = converted_try

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            generic_converted = pd.to_datetime(series_non_null, errors="coerce")

        generic_success_count = int(generic_converted.notna().sum())

        if generic_success_count > best_success_count:
            best_converted = generic_converted

        converted = (
            best_converted.dropna()
            if best_converted is not None
            else pd.Series(dtype="datetime64[ns]")
        )

    if converted.empty:
        return {}

    return {
        "min_date": converted.min().isoformat(),
        "max_date": converted.max().isoformat(),
    }


def get_issue_summary(column_profiles: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Count how many times each issue appears across all column profiles.
    """
    issue_counts: Dict[str, int] = {}

    for column_profile in column_profiles:
        issues = column_profile.get("issues", [])
        for issue in issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1

    return dict(sorted(issue_counts.items(), key=lambda item: item[1], reverse=True))


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