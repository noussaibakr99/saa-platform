import argparse
from pathlib import Path

from saa_platform.cleaning import clean_dataset
from saa_platform.ingestion import load_dataset
from saa_platform.profiling import profile_dataset
from saa_platform.reporting import (
    write_cleaned_dataset,
    write_cleaning_log,
    write_json_report,
    write_profile_comparison,
    write_text_summary,
)
from saa_platform.validation import validate_financial_data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SAA Platform - profile, clean, and validate financial datasets"
    )

    parser.add_argument("input_path", help="Path to the input dataset")
    parser.add_argument(
        "--profile-only",
        action="store_true",
        help="Run profiling only and skip cleaning/validation",
    )
    parser.add_argument(
        "--clean-only",
        action="store_true",
        help="Run profiling and cleaning but skip validation",
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Base output directory (default: data)",
    )

    args = parser.parse_args()

    input_path = Path(args.input_path)
    output_base = Path(args.output_dir)
    reports_dir = output_base / "reports"
    processed_dir = output_base / "processed"
    logs_dir = Path("logs")

    raw_df = load_dataset(input_path)
    profile_before = profile_dataset(raw_df)

    write_json_report(profile_before, reports_dir / "profile_report_before.json")
    write_text_summary(profile_before, reports_dir / "profile_summary_before.txt")

    if args.profile_only:
        print("Profile-only mode complete.")
        print(f"Input file: {input_path}")
        print(f"Outputs written to: {reports_dir}")
        return

    cleaned_df, cleaning_report = clean_dataset(raw_df, profile_before)
    profile_after = profile_dataset(cleaned_df)

    write_json_report(profile_after, reports_dir / "profile_report_after.json")
    write_text_summary(profile_after, reports_dir / "profile_summary_after.txt")

    write_profile_comparison(
        before_report=profile_before,
        after_report=profile_after,
        json_output_path=reports_dir / "profile_comparison.json",
        text_output_path=reports_dir / "profile_comparison.txt",
    )

    write_cleaned_dataset(cleaned_df, processed_dir / "cleaned_data.xlsx")
    write_cleaning_log(cleaning_report, logs_dir / "cleaning_log.txt")

    if not args.clean_only:
        validation_report = validate_financial_data(cleaned_df)
        write_json_report(validation_report, reports_dir / "validation_report.json")

    print("Pipeline completed successfully.")
    print(f"Input file: {input_path}")
    print(f"Reports: {reports_dir}")
    print(f"Processed data: {processed_dir}")
    print(f"Logs: {logs_dir}")


if __name__ == "__main__":
    main()