from pathlib import Path
from typing import Optional


def source_path(
    file_type: str,
    csv_directory: Path,
    json_directory: Path,
) -> Optional[Path]:
    normalized_type = file_type.lower()
    if normalized_type == "csv":
        return csv_directory / "*.csv"
    if normalized_type == "json":
        return json_directory / "*.json"
    return None
