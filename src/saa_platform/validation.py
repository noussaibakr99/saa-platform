from typing import Dict, Any, List
import pandas as pd


def validate_financial_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Run finance-specific validation checks.
    """
    checks = []

    checks.append(check_price_consistency(df))
    checks.append(check_missing_currency(df))
    checks.append(check_coupon_maturity(df))
    checks.append(check_negative_values(df))

    return {
        "validation_checks": checks
    }


# -----------------------------------
# 1. Price vs Market Value consistency
# -----------------------------------

def check_price_consistency(df: pd.DataFrame) -> Dict[str, Any]:
    required_cols = {"quantity", "clean_price", "currency_divisor", "market_value"}

    if not required_cols.issubset(df.columns):
        return {
            "check": "price_consistency",
            "status": "skipped",
            "reason": "missing required columns",
        }

    subset = df.dropna(subset=required_cols)

    if subset.empty:
        return {
            "check": "price_consistency",
            "status": "skipped",
            "reason": "no complete rows",
        }

    calculated_mv = subset["quantity"] * (subset["clean_price"] / subset["currency_divisor"])

    diff = (subset["market_value"] - calculated_mv).abs()

    threshold = 1e-2
    mismatches = diff > threshold

    return {
        "check": "price_consistency",
        "status": "completed",
        "rows_checked": int(len(subset)),
        "mismatches": int(mismatches.sum()),
        "severity": "warning" if mismatches.sum() > 0 else "info",
    }


# -----------------------------------
# 2. Missing currency
# -----------------------------------

def check_missing_currency(df: pd.DataFrame) -> Dict[str, Any]:
    if "currency" not in df.columns:
        return {"check": "missing_currency", "status": "skipped"}

    missing = df["currency"].isna().sum()

    return {
        "check": "missing_currency",
        "missing_count": int(missing),
        "severity": "warning" if missing > 0 else "info",
    }


# -----------------------------------
# 3. Coupon vs maturity logic
# -----------------------------------

def check_coupon_maturity(df: pd.DataFrame) -> Dict[str, Any]:
    if not {"coupon_rate", "maturity_date"}.issubset(df.columns):
        return {"check": "coupon_maturity", "status": "skipped"}

    subset = df.dropna(subset=["coupon_rate"])

    missing_maturity = subset["maturity_date"].isna().sum()

    return {
        "check": "coupon_maturity",
        "rows_with_coupon": int(len(subset)),
        "missing_maturity": int(missing_maturity),
        "severity": "warning" if missing_maturity > 0 else "info",
    }


# -----------------------------------
# 4. Suspicious negatives
# -----------------------------------

def check_negative_values(df: pd.DataFrame) -> Dict[str, Any]:
    numeric_cols = df.select_dtypes(include=["number"]).columns

    negatives = {}

    for col in numeric_cols:
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            negatives[col] = int(neg_count)

    return {
        "check": "negative_values",
        "columns_with_negatives": negatives,
        "severity": "warning" if negatives else "info",
    }