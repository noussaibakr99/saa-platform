import sys
from pathlib import Path

from saa_platform.ingestion import load_dataset
from saa_platform.profiling import profile_dataset
from saa_platform.reporting import write_json_report, write_text_summary


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python clean_data.py <input_file>")
        sys.exit(1)

    input_path = Path(sys.argv[1])

    try:
        df = load_dataset(input_path)
        report = profile_dataset(df)

        json_output_path = Path("data/reports/profile_report.json")
        text_output_path = Path("data/reports/profile_summary.txt")

        write_json_report(report, json_output_path)
        write_text_summary(report, text_output_path)

        dataset_summary = report["dataset_summary"]
        duplicate_summary = report["duplicate_summary"]

        print("Dataset loaded and profiled successfully")
        print(f"Input file: {input_path}")
        print(f"Rows: {dataset_summary['row_count']}")
        print(f"Columns: {dataset_summary['column_count']}")
        print(
            "Duplicate rows: "
            f"{duplicate_summary['duplicate_row_count']} "
            f"({duplicate_summary['duplicate_row_rate']:.2%})"
        )
        print(f"JSON report written to: {json_output_path}")
        print(f"Text summary written to: {text_output_path}")

    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()