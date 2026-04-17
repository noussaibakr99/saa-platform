import sys
from pathlib import Path

from saa_platform.ingestion import load_dataset


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python clean_data.py <input_file>")
        sys.exit(1)

    input_path = Path(sys.argv[1])

    try:
        df = load_dataset(input_path)
        print("Dataset loaded successfully")
        print(f"Rows: {df.shape[0]}")
        print(f"Columns: {df.shape[1]}")
        print("\nColumn names:")
        for col in df.columns:
            print(f"- {col}")
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()