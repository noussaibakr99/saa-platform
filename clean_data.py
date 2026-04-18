import argparse
from pathlib import Path

from saa_platform.ingestion import load_dataset
from saa_platform.profiling import profile_dataset
from saa_platform.cleaning import clean_dataset
from saa_platform.reporting import write_json_report, write_text_summary
from saa_platform.validation import validate_financial_data


def main():
    parser = argparse.ArgumentParser(description="SAA Data Cleaning Tool")

    parser.add_argument("input_path", help="Path to dataset")
    parser.add_argument("--profile-only", action="store_true")
    parser.add_argument("--clean-only", action="store_true")
    parser.add_argument("--output-dir", default="data/")
    parser.add_argument("--strict", action="store_true")

    args = parser.parse_args()

    df = load_dataset(args.input_path)

    output_dir = Path(args.output_dir)
    reports_dir = output_dir / "reports"
    processed_dir = output_dir / "processed"
    logs_dir = Path("logs")

    reports_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    print("Profiling dataset...")
    profile_before = profile_dataset(df)

    write_json_report(profile_before, reports_dir / "profile_before.json")

    if args.profile_only:
        print("Profile-only mode complete.")
        return

    print("Cleaning dataset...")
    cleaned_df, cleaning_log = clean_dataset(df, profile_before)

    if not args.clean_only:
        print("Running financial validation...")
        validation_report = validate_financial_data(cleaned_df)
        write_json_report(validation_report, reports_dir / "validation_report.json")

    cleaned_df.to_excel(processed_dir / "cleaned_data.xlsx", index=False)

    write_json_report(cleaning_log, logs_dir / "cleaning_log.json")

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()