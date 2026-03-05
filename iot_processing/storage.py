"""
Storage helpers for managing IoT transformation assets.
Handles file system operations, path resolution, and status caching for uploaded and processed files.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set


def get_paths() -> Dict[str, str]:
    """
    Returns absolute paths to key storage locations.
    Calculates paths relative to the project root.

    Returns:
        Dict[str, str]: Map of directory keys to absolute paths.
    """
    base_dir = Path(__file__).resolve().parent.parent
    uploads_dir = base_dir / "uploads"
    adjusted_dir = base_dir / "adjusted"
    processed_dir = base_dir / "processed"
    integrated_dir = base_dir / "integrated"
    
    return {
        "base_dir": str(base_dir),
        "uploads_dir": str(uploads_dir),
        "adjusted_dir": str(adjusted_dir),
        "processed_dir": str(processed_dir),
        "integrated_dir": str(integrated_dir),
    }


def ensure_dirs() -> None:
    """
    Ensures that all expected storage directories exist.
    Creates them if they are missing.
    """
    paths = get_paths()
    for key in ("uploads_dir", "adjusted_dir", "processed_dir", "integrated_dir"):
        Path(paths[key]).mkdir(parents=True, exist_ok=True)


def clear_directory(directory: Path) -> None:
    """
    Removes all contents from a directory except for '.gitkeep'.
    
    Args:
        directory (Path): The directory to clear.
    """
    if not directory.exists():
        return
    for entry in directory.iterdir():
        if entry.name == ".gitkeep":
            continue
        try:
            if entry.is_file() or entry.is_symlink():
                entry.unlink()
            elif entry.is_dir():
                shutil.rmtree(entry)
        except FileNotFoundError:
            continue


def reset_storage() -> None:
    """
    Empties the uploads, adjusted, processed, and integrated directories.
    Used for a clean slate.
    """
    paths = get_paths()
    for key in ("uploads_dir", "adjusted_dir", "processed_dir", "integrated_dir"):
        clear_directory(Path(paths[key]))


def allowed_file(filename: str, allowed_extensions: Optional[Set[str]] = None) -> bool:
    """
    Checks if a filename has an allowed extension.

    Args:
        filename (str): The filename to check.
        allowed_extensions (Optional[Set[str]]): Set of allowed extensions (default: {.csv}).

    Returns:
        bool: True if allowed.
    """
    if not filename or "." not in filename:
        return False
    extensions = allowed_extensions or {".csv"}
    normalized = {ext.lower().lstrip(".") for ext in extensions}
    return filename.rsplit(".", 1)[1].lower() in normalized


def safe_stem(filename: str) -> str:
    """
    Generates a safe, sanitized filename stem.
    Replaces non-alphanumeric characters with underscores.

    Args:
        filename (str): The original filename.

    Returns:
        str: A safe string suitable for filenames.
    """
    stem = Path(filename).stem.strip()
    if not stem:
        stem = "file"
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", stem)
    stem = re.sub(r"_+", "_", stem).strip("._-")
    return stem or "file"


def list_uploaded_files() -> List[Dict[str, object]]:
    """
    Lists uploaded CSV files with basic metadata (size, mtime).

    Returns:
        List[Dict[str, object]]: List of file metadata dictionaries.
    """
    uploads_dir = Path(get_paths()["uploads_dir"])
    if not uploads_dir.exists():
        return []

    results: List[Dict[str, object]] = []
    for path in uploads_dir.iterdir():
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue
        stats = path.stat()
        results.append(
            {
                "name": path.name,
                "path": str(path.resolve()),
                "size_bytes": stats.st_size,
                "mtime": datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    results.sort(key=lambda item: item["name"].lower())
    return results


def is_processed(csv_name: str) -> bool:
    """
    Checks if a CSV file has been processed into a parquet file.

    Args:
        csv_name (str): The name of the CSV file.

    Returns:
        bool: True if the corresponding parquet file exists.
    """
    adjusted_dir = Path(get_paths()["adjusted_dir"])
    parquet_path = adjusted_dir / f"{safe_stem(csv_name)}.parquet"
    return parquet_path.exists()


def processed_statuses() -> List[Dict[str, Optional[object]]]:
    """
    Reports processing status for all uploaded CSV files.
    Combines file existence check with cached status data.

    Returns:
        List[Dict[str, object]]: List of status dictionaries.
    """
    paths = get_paths()
    adjusted_dir = Path(paths["adjusted_dir"])
    status_cache = read_status()

    statuses: List[Dict[str, Optional[object]]] = []
    for meta in list_uploaded_files():
        name = meta["name"]
        processed = is_processed(name)
        adjusted_path: Optional[str] = None
        if processed:
            adjusted_path = str((adjusted_dir / f"{safe_stem(name)}.parquet").resolve())
        else:
            cached = status_cache.get(name)
            if cached:
                adjusted_path = cached.get("adjusted_path")
        statuses.append(
            {
                "name": name,
                "processed": processed,
                "adjusted_path": adjusted_path,
            }
        )
    return statuses


def is_ocel_uploaded() -> bool:
    """
    Checks if any OCEL SQLite file exists in the uploads directory.

    Returns:
        bool: True if an OCEL file is found.
    """
    uploads_dir = Path(get_paths()["uploads_dir"])
    extensions = {".db", ".sqlite", ".sqlite3"}
    if not uploads_dir.exists():
        return False
    return any(f.suffix.lower() in extensions for f in uploads_dir.iterdir() if f.is_file())


def status_file_path() -> Path:
    """
    Returns the path to the status JSON cache file.
    """
    return Path(get_paths()["base_dir"]) / "status.json"


def read_status() -> Dict[str, Dict[str, object]]:
    """
    Reads cached status information from JSON.

    Returns:
        Dict[str, Dict]: The status dictionary.
    """
    status_file = status_file_path()
    if not status_file.exists():
        return {}
    try:
        data = json.loads(status_file.read_text())
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def update_status(csv_name: str, processed: bool, adjusted_path: Optional[str]) -> None:
    """
    Updates the cached status information for a specific CSV file.

    Args:
        csv_name (str): Name of the CSV file.
        processed (bool): Processing status.
        adjusted_path (Optional[str]): Path to the adjusted parquet file.
    """
    ensure_dirs()
    status_file = status_file_path()
    status_data = read_status()
    status_data[csv_name] = {
        "processed": bool(processed),
        "adjusted_path": adjusted_path,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    status_file.write_text(json.dumps(status_data, indent=2, sort_keys=True))


def remove_status(csv_name: str) -> bool:
    """
    Removes cached status information for a given CSV filename.

    Args:
        csv_name (str): Name of the CSV file.

    Returns:
        bool: True if removed successfully.
    """
    status_file = status_file_path()
    if not status_file.exists():
        return False

    status_data = read_status()
    if csv_name not in status_data:
        return False

    status_data.pop(csv_name, None)
    status_file.write_text(json.dumps(status_data, indent=2, sort_keys=True))
    return True


def delete_upload_artifacts(csv_name: str) -> Dict[str, bool]:
    """
    Deletes an uploaded CSV, its associated adjusted parquet file, and its cache entry.

    Args:
        csv_name (str): The filename of the CSV to delete.

    Returns:
        Dict[str, bool]: Deletion results.
    """
    paths = get_paths()
    upload_path = Path(paths["uploads_dir"]) / csv_name
    adjusted_path = Path(paths["adjusted_dir"]) / f"{safe_stem(csv_name)}.parquet"

    deleted_upload = False
    deleted_adjusted = False
    if upload_path.exists():
        upload_path.unlink()
        deleted_upload = True
    if adjusted_path.exists():
        adjusted_path.unlink()
        deleted_adjusted = True

    status_removed = remove_status(csv_name)

    return {
        "upload_deleted": deleted_upload,
        "adjusted_deleted": deleted_adjusted,
        "status_removed": status_removed,
    }


def _human_readable_size(size_bytes: int) -> str:
    """
    Formats a byte count into a human-readable string (e.g., '1.5 MB').
    """
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    power = min(int((size_bytes.bit_length() - 1) / 10), len(units) - 1)
    scaled = size_bytes / (1024**power)
    return f"{scaled:.1f} {units[power]}"


def _format_mtime(iso_timestamp: str) -> str:
    """
    Formats an ISO timestamp into a readable date string.
    """
    try:
        dt = datetime.fromisoformat(iso_timestamp)
    except (TypeError, ValueError):
        return ""
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def upload_entries() -> List[Dict[str, object]]:
    """
    Returns a list of upload entries, enriched with status, labels, and metadata.

    Returns:
        List[Dict[str, object]]: List of file entries for display.
    """
    uploads = list_uploaded_files()
    statuses = {item["name"]: item for item in processed_statuses()}
    entries: List[Dict[str, object]] = []
    for item in uploads:
        status = statuses.get(item["name"], {})
        entries.append(
            {
                "name": item["name"],
                "path": item["path"],
                "size_bytes": item["size_bytes"],
                "size_label": _human_readable_size(item["size_bytes"]),
                "mtime": item["mtime"],
                "mtime_label": _format_mtime(item["mtime"]),
                "processed": status.get("processed", False),
                "adjusted_path": status.get("adjusted_path"),
                "adjusted_exists": bool(status.get("adjusted_path")),
            }
        )
    return entries
