"""
Utilities for combining adjusted parquet files with DuckDB.
Manages the background process of merging multiple Parquet files into a single dataset.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional

import duckdb

from .storage import safe_stem

ProgressCallback = Callable[[int, int, str], None]

combine_lock = threading.Lock()
combine_state: Dict[str, object] = {
    "state": "idle",
    "current": 0,
    "total": 0,
    "percent": 0,
    "message": "",
    "row_count": None,
    "error": None,
    "out_path": None,
    "columns": None,
    "files_combined": 0,
}
combine_thread: Optional[threading.Thread] = None


def all_processed(upload_dir: str, adjusted_dir: str) -> bool:
    """
    Checks if every CSV upload in the upload directory has a corresponding 
    adjusted parquet file in the adjusted directory.

    Args:
        upload_dir (str): Path to the uploads directory.
        adjusted_dir (str): Path to the adjusted (parquet) directory.

    Returns:
        bool: True if all Uploads have been processed (adjusted).
    """
    uploads_path = Path(upload_dir)
    adjusted_path = Path(adjusted_dir)

    if not uploads_path.is_dir() or not adjusted_path.is_dir():
        return False

    csv_files = [
        path
        for path in uploads_path.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    ]
    if not csv_files:
        return True

    for csv_file in csv_files:
        expected = adjusted_path / f"{safe_stem(csv_file.name)}.parquet"
        if not expected.exists():
            return False
    return True


def list_adjusted_parquet_files(adjusted_dir: str) -> List[Path]:
    """
    Returns a sorted list of adjusted parquet files ready for combination.

    Args:
        adjusted_dir (str): Path to the directory containing parquet files.

    Returns:
        List[Path]: Sorted list of file paths.
    """
    adjusted_path = Path(adjusted_dir)
    if not adjusted_path.is_dir():
        return []
    return sorted(path for path in adjusted_path.glob("*.parquet") if path.is_file())


def reset_combine_state() -> None:
    """
    Resets the global combine progress state to 'idle'.
    Thread-safe.
    """
    with combine_lock:
        combine_state.update(
            {
                "state": "idle",
                "current": 0,
                "total": 0,
                "percent": 0,
                "message": "",
                "row_count": None,
                "error": None,
                "out_path": None,
                "columns": None,
                "files_combined": 0,
            }
        )


def initialize_combine_state(total: int) -> None:
    """
    Initializes the combine state for a new job.
    Marks state as 'running'.

    Args:
        total (int): Total number of files to combine.
    """
    with combine_lock:
        combine_state.update(
            {
                "state": "running",
                "current": 0,
                "total": total,
                "percent": 0,
                "message": "Preparing to combine adjusted files...",
                "row_count": None,
                "error": None,
                "out_path": None,
                "columns": None,
                "files_combined": total,
            }
        )


def combine_progress_callback(current: int, total: int, message: str) -> None:
    """
    Updates the shared combine state from worker threads.

    Args:
        current (int): Number of files processed so far.
        total (int): Total number of files.
        message (str): Progress message.
    """
    percent = 0
    if total > 0:
        percent = max(0, min(100, int(round((current / total) * 100))))
    with combine_lock:
        combine_state.update(
            {
                "current": current,
                "total": total,
                "percent": percent,
                "message": message,
            }
        )


def combine_is_running() -> bool:
    """
    Checks if a combine job is currently active.

    Returns:
        bool: True if state is 'running'.
    """
    with combine_lock:
        return combine_state.get("state") == "running"


def combine_adjusted_to_parquet(
    adjusted_dir: str,
    out_path: str,
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, object]:
    """
    Combines all adjusted parquet files into a single unified parquet output.
    Uses DuckDB for efficient merging without loading everything into memory.

    Args:
        adjusted_dir (str): Source directory of parquet files.
        out_path (str): Destination path for the combined parquet file.
        progress_callback (Optional[ProgressCallback]): Callback for progress updates.

    Returns:
        Dict[str, object]: Result stats including row count and output path.

    Raises:
        ValueError: If no files are found or the result is empty.
    """
    parquet_files = list_adjusted_parquet_files(adjusted_dir)
    if not parquet_files:
        raise ValueError("No adjusted parquet files were found. Process files before combining.")

    total_files = len(parquet_files)
    file_paths = [str(path) for path in parquet_files]

    def report(current: int, message: str) -> None:
        if progress_callback:
            progress_callback(current, total_files, message)

    report(0, "Preparing to combine adjusted files...")

    destination = Path(out_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    threads = max(os.cpu_count() or 1, 1)

    with duckdb.connect(database=":memory:") as connection:
        connection.execute(f"SET threads TO {threads}")

        # Prepare an empty table with the unioned schema, avoiding row materialisation in Python
        connection.execute(
            "CREATE TEMP TABLE combined AS "
            "SELECT * FROM read_parquet(? , union_by_name := true) WHERE 0 = 1",
            [file_paths],
        )

        columns_result = connection.execute("PRAGMA table_info('combined')").fetchall()
        column_names = [row[1] for row in columns_result]
        if not column_names:
            raise ValueError("Combined dataset has no columns. Verify the adjusted files contain data.")
        
        # Sanitize column names for SQL
        sanitized_columns = ['"' + name.replace('"', '""') + '"' for name in column_names]
        column_projection = ", ".join(sanitized_columns)

        connection.execute("BEGIN TRANSACTION")
        try:
            for index, path in enumerate(file_paths, start=1):
                connection.execute(
                    "INSERT INTO combined "
                    f"SELECT {column_projection} FROM read_parquet(? , union_by_name := true)",
                    [path],
                )
                report(index, f"Combined {index} of {total_files} file(s)...")
            connection.execute("COMMIT")
        except Exception:  # noqa: BLE001
            connection.execute("ROLLBACK")
            raise

        row_count = connection.execute("SELECT COUNT(*) FROM combined").fetchone()[0]
        if row_count == 0:
            raise ValueError("Combined dataset is empty after union by name. Verify the adjusted files contain data.")

        columns = column_names

        report(total_files, "Writing combined dataset to parquet...")
        escaped_destination = str(destination).replace("'", "''")
        connection.execute(
            f"COPY combined TO '{escaped_destination}' "
            "(FORMAT 'parquet', COMPRESSION 'zstd')"
        )

    report(total_files, "Combine completed successfully.")

    return {
        "row_count": row_count,
        "out_path": str(destination.resolve()),
        "columns": columns,
        "files_combined": total_files,
    }


def current_combine_state() -> Dict[str, object]:
    """
    Returns a snapshot of the current combine state.
    """
    with combine_lock:
        return dict(combine_state)


def launch_combine_thread(adjusted_dir: str, out_path: Path) -> None:
    """
    Starts a background thread to run the combine job, updating the shared state.

    Args:
        adjusted_dir (str): Source directory.
        out_path (Path): Destination path.
    """
    global combine_thread

    def worker() -> None:
        global combine_thread
        try:
            result = combine_adjusted_to_parquet(
                adjusted_dir,
                str(out_path),
                progress_callback=combine_progress_callback,
            )
        except Exception as exc:  # noqa: BLE001
            with combine_lock:
                combine_state.update(
                    {
                        "state": "failed",
                        "error": str(exc),
                        "message": str(exc),
                    }
                )
        else:
            with combine_lock:
                combine_state.update(
                    {
                        "state": "completed",
                        "percent": 100,
                        "message": f"Combined dataset created with {result['row_count']} rows.",
                        "row_count": result["row_count"],
                        "out_path": result["out_path"],
                        "columns": result["columns"],
                        "files_combined": result.get("files_combined", combine_state.get("total", 0)),
                    }
                )
        finally:
            with combine_lock:
                combine_thread = None

    combine_thread = threading.Thread(target=worker, daemon=True)
    combine_thread.start()
