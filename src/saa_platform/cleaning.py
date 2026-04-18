from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

import pandas as pd


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

BOOLEAN_TRUE_VALUES = {"true", "t", "yes", "y", "1", "on"}
BOOLEAN_FALSE_VALUES = {"false", "f", "no", "n", "0", "off"}

CURRENCY_SYMBOLS_PATTERN = r"[€$£¥₣₹₽₺₩₪฿₫₴₦₱₡₲₵₸₭₮₤₥₨₼₾₿]"

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

VALID_CURRENCY_CODES = {
    "AED", "ARS", "AUD", "BHD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP",
    "CZK", "DKK", "EGP", "EUR", "GBP", "HKD", "HUF", "IDR", "ILS", "INR",
    "JPY", "KRW", "KWD", "MAD", "MXN", "MYR", "NOK", "NZD", "OMR", "PEN",
    "PHP", "PLN", "QAR", "RON", "RUB", "SAR", "SEK", "SGD", "THB", "TRY",
    "TWD", "UAH", "USD", "VND", "ZAR",
}


def clean_dataset(
    df: pd.DataFrame,
    profile_report: Dict[str, Any],
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cleaned_df = df.copy()
    cleaning_steps: List[Dict[str, Any]] = []

    cleaned_df, column_name_log = standardize_column_names(cleaned_df)
    cleaning_steps.append(column_name_log)

    cleaned_df, missing_value_log = harmonize_missing_values(cleaned_df)
    cleaning_steps.append(missing_value_log)

    cleaned_df, empty_structure_log = remove_fully_empty_rows_and_columns(cleaned_df)
    cleaning_steps.append(empty_structure_log)

    cleaned_df, artifact_column_log = remove_obvious_artifact_columns(cleaned_df)
    cleaning_steps.append(artifact_column_log)

    cleaned_df, text_normalization_log = normalize_text_columns(cleaned_df)
    cleaning_steps.append(text_normalization_log)

    cleaned_df, finance_normalization_logs = normalize_finance_columns(cleaned_df)
    cleaning_steps.extend(finance_normalization_logs)

    cleaned_df, duplicate_log = remove_exact_duplicates(cleaned_df)
    cleaning_steps.append(duplicate_log)

    cleaned_df, type_conversion_logs = convert_column_types(cleaned_df, profile_report)
    cleaning_steps.extend(type_conversion_logs)

    cleaning_report = build_cleaning_report(
        original_row_count=int(df.shape[0]),
        cleaned_row_count=int(cleaned_df.shape[0]),
        original_column_count=int(df.shape[1]),
        cleaned_column_count=int(cleaned_df.shape[1]),
        cleaning_steps=cleaning_steps,
    )

    return cleaned_df, cleaning_report


def standardize_column_names(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    original_columns = [str(col) for col in df.columns]
    standardized_columns = []
    seen_counts: Dict[str, int] = {}
    renamed_columns: Dict[str, str] = {}

    for original_name in original_columns:
        clean_name = str(original_name).strip().lower()
        clean_name = re.sub(r"[^\w]+", "_", clean_name)
        clean_name = re.sub(r"_+", "_", clean_name)
        clean_name = clean_name.strip("_")

        if not clean_name:
            clean_name = "unnamed_column"

        base_name = clean_name
        if base_name in seen_counts:
            seen_counts[base_name] += 1
            clean_name = f"{base_name}_{seen_counts[base_name]}"
        else:
            seen_counts[base_name] = 0

        standardized_columns.append(clean_name)

        if original_name != clean_name:
            renamed_columns[original_name] = clean_name

    df = df.copy()
    df.columns = standardized_columns

    log = {
        "step": "standardize_column_names",
        "columns_renamed_count": len(renamed_columns),
        "renamed_columns": renamed_columns,
    }

    return df, log


def harmonize_missing_values(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    replacements_count = 0
    affected_columns: Dict[str, int] = {}

    for column in df.columns:
        series = df[column]

        if not pd.api.types.is_object_dtype(series) and not pd.api.types.is_string_dtype(series):
            continue

        original_isna = int(series.isna().sum())
        cleaned_series = series.apply(replace_common_nulls)
        new_isna = int(cleaned_series.isna().sum())
        delta = new_isna - original_isna

        if delta > 0:
            replacements_count += delta
            affected_columns[str(column)] = delta

        df[column] = cleaned_series

    log = {
        "step": "harmonize_missing_values",
        "total_replacements": replacements_count,
        "affected_columns": affected_columns,
    }

    return df, log


def replace_common_nulls(value: Any) -> Any:
    if pd.isna(value):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in COMMON_NULL_STRINGS:
            return pd.NA

    return value


def remove_fully_empty_rows_and_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()

    row_count_before = int(df.shape[0])
    column_names_before = [str(col) for col in df.columns]

    df = df.dropna(axis=0, how="all")

    fully_empty_columns = [str(col) for col in df.columns if df[col].isna().all()]
    df = df.dropna(axis=1, how="all")

    row_count_after = int(df.shape[0])
    column_names_after = [str(col) for col in df.columns]

    log = {
        "step": "remove_fully_empty_rows_and_columns",
        "empty_rows_removed": row_count_before - row_count_after,
        "empty_columns_removed": len(fully_empty_columns),
        "removed_column_names": fully_empty_columns,
        "column_count_before": len(column_names_before),
        "column_count_after": len(column_names_after),
    }

    return df, log


def remove_obvious_artifact_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    removed_columns: List[str] = []

    for column in list(df.columns):
        column_name = str(column).strip().lower()

        is_unnamed_like = column_name.startswith("unnamed")
        is_index_like = column_name in {"index", "level_0"}

        if not (is_unnamed_like or is_index_like):
            continue

        series = df[column]

        if series.isna().all():
            removed_columns.append(str(column))
            df = df.drop(columns=[column])
            continue

        non_null = series.dropna()
        if non_null.empty:
            removed_columns.append(str(column))
            df = df.drop(columns=[column])
            continue

        numeric_series = pd.to_numeric(non_null, errors="coerce")
        if numeric_series.notna().all():
            values = numeric_series.astype(int).tolist()
            zero_based = list(range(len(values)))
            one_based = list(range(1, len(values) + 1))

            if values == zero_based or values == one_based:
                removed_columns.append(str(column))
                df = df.drop(columns=[column])

    log = {
        "step": "remove_obvious_artifact_columns",
        "artifact_columns_removed_count": len(removed_columns),
        "removed_columns": removed_columns,
    }

    return df, log


def normalize_text_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    affected_columns: Dict[str, int] = {}
    total_changes = 0

    for column in df.columns:
        series = df[column]

        if not pd.api.types.is_object_dtype(series) and not pd.api.types.is_string_dtype(series):
            continue

        changes = 0
        normalized_values = []

        for value in series:
            if pd.isna(value):
                normalized_values.append(value)
                continue

            if not isinstance(value, str):
                normalized_values.append(value)
                continue

            new_value = normalize_whitespace(value)

            if values_differ(value, new_value):
                changes += 1

            normalized_values.append(new_value)

        if changes > 0:
            df[column] = normalized_values
            affected_columns[str(column)] = changes
            total_changes += changes

    log = {
        "step": "normalize_text_columns",
        "total_cells_changed": total_changes,
        "affected_columns": affected_columns,
    }

    return df, log


def normalize_whitespace(value: str) -> str:
    value = value.replace("\xa0", " ")
    value = value.replace("\n", " ")
    value = value.replace("\t", " ")
    value = value.strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_finance_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    df = df.copy()
    logs: List[Dict[str, Any]] = []

    df, currency_log = normalize_currency_columns(df)
    logs.append(currency_log)

    df, identifier_log = normalize_identifier_columns(df)
    logs.append(identifier_log)

    df, category_log = normalize_category_columns(df)
    logs.append(category_log)

    return df, logs


def normalize_currency_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    affected_columns: Dict[str, int] = {}
    invalid_currency_examples: Dict[str, List[str]] = {}
    detected_currency_columns: List[str] = []

    for column in df.columns:
        if not is_likely_currency_column(str(column)):
            continue

        detected_currency_columns.append(str(column))
        series = df[column]

        if not pd.api.types.is_object_dtype(series) and not pd.api.types.is_string_dtype(series):
            continue

        changes = 0
        invalid_examples: List[str] = []
        normalized_values = []

        for value in series:
            if pd.isna(value):
                normalized_values.append(value)
                continue

            original_value = str(value)
            normalized_value = normalize_currency_value(original_value)

            if values_differ(original_value, normalized_value):
                changes += 1

            if not pd.isna(normalized_value) and normalized_value not in VALID_CURRENCY_CODES:
                if normalized_value not in invalid_examples:
                    invalid_examples.append(str(normalized_value))

            normalized_values.append(normalized_value)

        df[column] = normalized_values

        if changes > 0:
            affected_columns[str(column)] = changes

        if invalid_examples:
            invalid_currency_examples[str(column)] = invalid_examples[:5]

    log = {
        "step": "normalize_currency_columns",
        "detected_currency_columns": detected_currency_columns,
        "affected_columns": affected_columns,
        "invalid_currency_examples": invalid_currency_examples,
    }

    return df, log


def is_likely_currency_column(column_name: str) -> bool:
    name = column_name.strip().lower()

    if "currency_divisor" in name:
        return False

    if "currency_rate" in name:
        return False

    if "coupon_currency" in name:
        return False

    return (
        name == "currency"
        or name == "base_currency"
        or name == "local_currency"
        or name == "quote_currency"
        or name == "settlement_currency"
        or name.endswith("_currency")
        or name == "ccy"
        or name.endswith("_ccy")
    )


def normalize_currency_value(value: str) -> Any:
    cleaned = normalize_whitespace(value)
    cleaned = re.sub(r"[^A-Za-z]", "", cleaned)
    cleaned = cleaned.upper()

    if cleaned == "":
        return pd.NA

    return cleaned


def normalize_identifier_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    affected_columns: Dict[str, int] = {}
    detected_identifier_columns: List[str] = []

    for column in df.columns:
        column_name = str(column)

        if not is_likely_identifier_column(column_name):
            continue

        detected_identifier_columns.append(column_name)
        series = df[column]

        if not pd.api.types.is_object_dtype(series) and not pd.api.types.is_string_dtype(series):
            continue

        changes = 0
        normalized_values = []

        for value in series:
            if pd.isna(value):
                normalized_values.append(value)
                continue

            original_value = str(value)
            normalized_value = normalize_identifier_value(original_value, column_name)

            if values_differ(original_value, normalized_value):
                changes += 1

            normalized_values.append(normalized_value)

        df[column] = normalized_values

        if changes > 0:
            affected_columns[column_name] = changes

    log = {
        "step": "normalize_identifier_columns",
        "detected_identifier_columns": detected_identifier_columns,
        "affected_columns": affected_columns,
    }

    return df, log


def is_likely_identifier_column(column_name: str) -> bool:
    name = column_name.strip().lower()

    identifier_keywords = [
        "isin",
        "ticker",
        "cusip",
        "sedol",
        "ric",
        "portfolio_code",
        "strategy_code",
        "bank_code",
        "asset_id",
        "security_id",
        "instrument_id",
        "portfolio_id",
        "strategy_id",
        "internal_id",
    ]

    return any(keyword in name for keyword in identifier_keywords)


def normalize_identifier_value(value: str, column_name: str) -> str:
    name = column_name.strip().lower()
    cleaned = normalize_whitespace(value).upper()

    if cleaned.startswith("'"):
        cleaned = cleaned[1:]

    if "isin" in name or "cusip" in name or "sedol" in name:
        cleaned = re.sub(r"[^A-Z0-9]", "", cleaned)
        return cleaned

    if "ticker" in name or "ric" in name:
        cleaned = re.sub(r"\s+", "", cleaned)
        cleaned = re.sub(r"[^A-Z0-9.\-/_]", "", cleaned)
        return cleaned

    cleaned = cleaned.replace(" ", "_")
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = re.sub(r"[^A-Z0-9_\-/]", "", cleaned)
    cleaned = cleaned.strip("_")

    return cleaned


def normalize_category_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    affected_columns: Dict[str, int] = {}
    detected_category_columns: List[str] = []

    for column in df.columns:
        column_name = str(column)

        if not is_likely_category_column(column_name):
            continue

        detected_category_columns.append(column_name)
        series = df[column]

        if not pd.api.types.is_object_dtype(series) and not pd.api.types.is_string_dtype(series):
            continue

        changes = 0
        normalized_values = []

        for value in series:
            if pd.isna(value):
                normalized_values.append(value)
                continue

            original_value = str(value)
            normalized_value = normalize_category_value(original_value, column_name)

            if values_differ(original_value, normalized_value):
                changes += 1

            normalized_values.append(normalized_value)

        df[column] = normalized_values

        if changes > 0:
            affected_columns[column_name] = changes

    log = {
        "step": "normalize_category_columns",
        "detected_category_columns": detected_category_columns,
        "affected_columns": affected_columns,
    }

    return df, log


def is_likely_category_column(column_name: str) -> bool:
    name = column_name.strip().lower()

    category_keywords = [
        "asset_class",
        "asset_subclass",
        "sector",
        "industry",
        "region",
        "country",
        "instrument_type",
        "subtype",
        "rating",
        "credit_rating",
    ]

    return any(keyword in name for keyword in category_keywords)


def normalize_category_value(value: str, column_name: str) -> str:
    cleaned = normalize_whitespace(value)

    if cleaned.startswith("'"):
        cleaned = cleaned[1:]

    name = column_name.strip().lower()

    if "rating" in name:
        cleaned = cleaned.upper()
        cleaned = cleaned.replace(" ", "")
        return cleaned

    if cleaned.isupper() and len(cleaned) <= 5:
        return cleaned

    if cleaned.lower() == cleaned:
        return cleaned.title()

    return cleaned


def convert_column_types(
    df: pd.DataFrame,
    profile_report: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    df = df.copy()
    conversion_logs: List[Dict[str, Any]] = []

    column_profiles = profile_report.get("columns", [])

    for column_profile in column_profiles:
        original_column_name = column_profile.get("column_name")
        inferred_type = column_profile.get("inferred_type")

        normalized_column_name = normalize_column_name_for_lookup(original_column_name)

        if normalized_column_name not in df.columns:
            continue

        if inferred_type == "numeric":
            df, log = convert_numeric_column(df, normalized_column_name)
            conversion_logs.append(log)

        elif inferred_type == "datetime":
            df, log = convert_datetime_column(df, normalized_column_name)
            conversion_logs.append(log)

        elif inferred_type == "boolean":
            df, log = convert_boolean_column(df, normalized_column_name)
            conversion_logs.append(log)

    return df, conversion_logs


def normalize_column_name_for_lookup(column_name: Any) -> str:
    clean_name = str(column_name).strip().lower()
    clean_name = re.sub(r"[^\w]+", "_", clean_name)
    clean_name = re.sub(r"_+", "_", clean_name)
    clean_name = clean_name.strip("_")

    if not clean_name:
        clean_name = "unnamed_column"

    return clean_name


def convert_numeric_column(
    df: pd.DataFrame,
    column_name: str,
    min_success_ratio: float = 0.90,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    series = df[column_name]

    if pd.api.types.is_numeric_dtype(series):
        return df, {
            "step": "convert_numeric_column",
            "column": column_name,
            "status": "skipped_already_numeric",
        }

    non_null = series.dropna()

    if non_null.empty:
        return df, {
            "step": "convert_numeric_column",
            "column": column_name,
            "status": "skipped_empty_column",
        }

    cleaned_strings = non_null.astype(str).apply(clean_numeric_string)
    converted = pd.to_numeric(cleaned_strings, errors="coerce")

    success_count = int(converted.notna().sum())
    total_non_null = int(len(non_null))
    success_ratio = safe_divide(success_count, total_non_null)

    failed_examples = non_null[converted.isna()].astype(str).head(5).tolist()
    percent_like_count = int(non_null.astype(str).str.contains("%", regex=False).sum())

    if success_ratio < min_success_ratio:
        return df, {
            "step": "convert_numeric_column",
            "column": column_name,
            "status": "skipped_low_success_ratio",
            "success_ratio": round(success_ratio, 6),
            "converted_values": success_count,
            "non_null_values": total_non_null,
            "failed_examples": failed_examples,
            "percentage_like_values_detected": percent_like_count,
            "percentage_policy": "strip_percent_and_store_as_percentage_points",
        }

    full_cleaned_series = series.apply(
        lambda x: clean_numeric_string(str(x)) if not pd.isna(x) else x
    )
    full_converted_series = pd.to_numeric(full_cleaned_series, errors="coerce")

    df[column_name] = full_converted_series

    return df, {
        "step": "convert_numeric_column",
        "column": column_name,
        "status": "converted",
        "success_ratio": round(success_ratio, 6),
        "converted_values": success_count,
        "non_null_values": total_non_null,
        "failed_examples": failed_examples,
        "percentage_like_values_detected": percent_like_count,
        "percentage_policy": "strip_percent_and_store_as_percentage_points",
    }


def clean_numeric_string(value: str) -> str:
    value = normalize_whitespace(value)

    if value.startswith("'"):
        value = value[1:]

    value = re.sub(CURRENCY_SYMBOLS_PATTERN, "", value)
    value = re.sub(r"\b(CHF|USD|EUR|GBP|JPY|AUD|CAD|NOK|SEK|DKK)\b", "", value, flags=re.IGNORECASE)

    value = normalize_whitespace(value)
    value = value.replace(",", "")
    value = value.replace("'", "")
    value = value.replace(" ", "")
    value = value.replace("%", "")

    if re.match(r"^\(.*\)$", value):
        value = "-" + value[1:-1]

    if re.match(r"^.+-$", value):
        value = "-" + value[:-1]

    return value.strip()


def convert_datetime_column(
    df: pd.DataFrame,
    column_name: str,
    min_success_ratio: float = 0.90,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    series = df[column_name]

    if pd.api.types.is_datetime64_any_dtype(series):
        return df, {
            "step": "convert_datetime_column",
            "column": column_name,
            "status": "skipped_already_datetime",
        }

    non_null = series.dropna()

    if non_null.empty:
        return df, {
            "step": "convert_datetime_column",
            "column": column_name,
            "status": "skipped_empty_column",
        }

    best_converted = None
    best_success_count = -1
    best_format = None

    non_null_as_str = non_null.astype(str).str.strip()

    for date_format in COMMON_DATE_FORMATS:
        converted_try = pd.to_datetime(non_null_as_str, format=date_format, errors="coerce")
        success_count_try = int(converted_try.notna().sum())

        if success_count_try > best_success_count:
            best_success_count = success_count_try
            best_converted = converted_try
            best_format = date_format

    generic_converted = pd.to_datetime(non_null_as_str, errors="coerce")
    generic_success_count = int(generic_converted.notna().sum())

    if generic_success_count > best_success_count:
        best_success_count = generic_success_count
        best_converted = generic_converted
        best_format = "generic_parser"

    success_count = best_success_count
    total_non_null = int(len(non_null))
    success_ratio = safe_divide(success_count, total_non_null)

    failed_examples = non_null_as_str[best_converted.isna()].head(5).tolist()
    ambiguity_flag, ambiguous_examples = detect_ambiguous_date_strings(non_null_as_str)

    if success_ratio < min_success_ratio:
        return df, {
            "step": "convert_datetime_column",
            "column": column_name,
            "status": "skipped_low_success_ratio",
            "success_ratio": round(success_ratio, 6),
            "converted_values": success_count,
            "non_null_values": total_non_null,
            "best_parser_used": best_format,
            "ambiguous_date_risk": ambiguity_flag,
            "ambiguous_examples": ambiguous_examples,
            "failed_examples": failed_examples,
        }

    full_series_as_str = series.apply(lambda x: str(x).strip() if not pd.isna(x) else x)

    if best_format == "generic_parser":
        df[column_name] = pd.to_datetime(full_series_as_str, errors="coerce")
    else:
        df[column_name] = pd.to_datetime(full_series_as_str, format=best_format, errors="coerce")

    return df, {
        "step": "convert_datetime_column",
        "column": column_name,
        "status": "converted",
        "success_ratio": round(success_ratio, 6),
        "converted_values": success_count,
        "non_null_values": total_non_null,
        "best_parser_used": best_format,
        "ambiguous_date_risk": ambiguity_flag,
        "ambiguous_examples": ambiguous_examples,
        "failed_examples": failed_examples,
    }


def detect_ambiguous_date_strings(series: pd.Series) -> Tuple[bool, List[str]]:
    ambiguous_examples: List[str] = []

    for value in series.head(50):
        value_str = str(value).strip()

        match = re.match(r"^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})$", value_str)
        if not match:
            continue

        first = int(match.group(1))
        second = int(match.group(2))

        if 1 <= first <= 12 and 1 <= second <= 12 and first != second:
            ambiguous_examples.append(value_str)

        if len(ambiguous_examples) >= 5:
            break

    return len(ambiguous_examples) > 0, ambiguous_examples


def convert_boolean_column(
    df: pd.DataFrame,
    column_name: str,
    min_success_ratio: float = 0.95,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    series = df[column_name]

    if pd.api.types.is_bool_dtype(series):
        return df, {
            "step": "convert_boolean_column",
            "column": column_name,
            "status": "skipped_already_boolean",
        }

    non_null = series.dropna()

    if non_null.empty:
        return df, {
            "step": "convert_boolean_column",
            "column": column_name,
            "status": "skipped_empty_column",
        }

    normalized = non_null.astype(str).str.strip().str.lower()
    valid_mask = normalized.isin(BOOLEAN_TRUE_VALUES.union(BOOLEAN_FALSE_VALUES))

    success_count = int(valid_mask.sum())
    total_non_null = int(len(non_null))
    success_ratio = safe_divide(success_count, total_non_null)

    failed_examples = non_null[~valid_mask].astype(str).head(5).tolist()

    if success_ratio < min_success_ratio:
        return df, {
            "step": "convert_boolean_column",
            "column": column_name,
            "status": "skipped_low_success_ratio",
            "success_ratio": round(success_ratio, 6),
            "converted_values": success_count,
            "non_null_values": total_non_null,
            "failed_examples": failed_examples,
        }

    df[column_name] = series.apply(map_boolean_value)

    return df, {
        "step": "convert_boolean_column",
        "column": column_name,
        "status": "converted",
        "success_ratio": round(success_ratio, 6),
        "converted_values": success_count,
        "non_null_values": total_non_null,
        "failed_examples": failed_examples,
    }


def map_boolean_value(value: Any) -> Any:
    if pd.isna(value):
        return value

    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()

    if normalized in BOOLEAN_TRUE_VALUES:
        return True

    if normalized in BOOLEAN_FALSE_VALUES:
        return False

    return value


def build_cleaning_report(
    original_row_count: int,
    cleaned_row_count: int,
    original_column_count: int,
    cleaned_column_count: int,
    cleaning_steps: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "summary": {
            "row_count_before": original_row_count,
            "row_count_after": cleaned_row_count,
            "column_count_before": original_column_count,
            "column_count_after": cleaned_column_count,
            "rows_removed_total": original_row_count - cleaned_row_count,
        },
        "steps": cleaning_steps,
    }


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


def values_differ(left: Any, right: Any) -> bool:
    """
    Safe comparison helper that handles pd.NA without raising:
    'boolean value of NA is ambiguous'
    """
    left_is_na = pd.isna(left)
    right_is_na = pd.isna(right)

    if left_is_na and right_is_na:
        return False

    if left_is_na != right_is_na:
        return True

    return str(left) != str(right)

def remove_exact_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Remove exact duplicate rows.
    """
    original_row_count = int(df.shape[0])
    deduplicated_df = df.drop_duplicates().copy()
    cleaned_row_count = int(deduplicated_df.shape[0])
    removed_count = original_row_count - cleaned_row_count

    log = {
        "step": "remove_exact_duplicates",
        "rows_removed": removed_count,
        "row_count_before": original_row_count,
        "row_count_after": cleaned_row_count,
    }

    return deduplicated_df, log