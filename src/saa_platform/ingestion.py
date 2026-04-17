from pathlib import Path
from typing import Union
import pandas as pd


SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def load_dataset(file_path: Union[str, Path]) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type: {path.suffix}. "
            f"Supported types are: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )

    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)

    return pd.read_excel(path)