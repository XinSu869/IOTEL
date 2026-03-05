"""
Profiling helpers for uploaded CSV files.
Provides functions to infer simple schemas and generate lightweight previews for the UI.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import polars as pl


def _dtype_label(dtype: pl.datatypes.DataType) -> str:
    """
    Maps Polars dtypes to simplified string labels for the UI.

    Args:
        dtype (pl.datatypes.DataType): The Polars data type.

    Returns:
        str: Simplified label (Int64, Float64, Boolean, Datetime, or Utf8).
    """
    if dtype in pl.INTEGER_DTYPES:
        return "Int64"
    if dtype in pl.FLOAT_DTYPES:
        return "Float64"
    if dtype == pl.Boolean:
        return "Boolean"
    if dtype in (pl.Datetime, pl.Date, pl.Time):
        return "Datetime"
    return "Utf8"


def propose_schema(csv_path: str, sample_rows: int = 5000) -> Dict[str, object]:
    """
    Infers simple schema information for a CSV file by scanning a sample.
    Estimates row counts and null values.

    Args:
        csv_path (str): Path to the CSV file.
        sample_rows (int): Number of rows to scan for inference.

    Returns:
        Dict[str, object]: Schema details including column names, proposed types, and null estimates.
    """
    path = Path(csv_path)
    lazy_frame = pl.scan_csv(
        path,
        infer_schema_length=sample_rows,
        try_parse_dates=True,
    )

    schema = lazy_frame.schema
    aggregations: List[pl.Expr] = [pl.len().alias("_row_count")]
    for col in schema:
        aggregations.append(pl.col(col).null_count().alias(col))

    metrics = lazy_frame.select(aggregations).collect()
    row_count = int(metrics["_row_count"][0]) if metrics.height else 0

    columns = []
    for name, dtype in schema.items():
        nulls = int(metrics[name][0]) if name in metrics.columns else 0
        columns.append(
            {
                "name": name,
                "proposed_dtype": _dtype_label(dtype),
                "nulls_estimate": nulls,
            }
        )

    return {
        "columns": columns,
        "row_count_estimate": row_count,
        "file_size_bytes": path.stat().st_size,
    }


def table_summary(csv_path: str, n: int = 20) -> Dict[str, object]:
    """
    Returns a lightweight preview of the CSV file for UI display.

    Args:
        csv_path (str): Path to the CSV file.
        n (int): Number of rows to return.

    Returns:
        Dict[str, object]: Dictionary containing 'head' rows, 'columns' metadata, and dimensions.
    """
    df = pl.read_csv(
        csv_path,
        infer_schema_length=10_000,
        try_parse_dates=True,
    )
    head_df = df.head(n)

    columns = [
        {"name": name, "dtype": _dtype_label(dtype)}
        for name, dtype in df.schema.items()
    ]

    return {
        "head": head_df.to_dicts(),
        "columns": columns,
        "nrows": df.height,
        "ncols": df.width,
    }


def build_preview_context(csv_path: str) -> Dict[str, object]:
    """
    Composes a complete context dictionary for UI previews, including schema and data summary.

    Args:
        csv_path (str): Path to the CSV file.

    Returns:
        Dict[str, object]: Context dictionary ready for template rendering.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path.name}")

    schema = propose_schema(csv_path)
    summary = table_summary(csv_path, n=20)

    headers = [col["name"] for col in summary["columns"]]
    rows = [[row.get(header) for header in headers] for row in summary["head"]]
    proposed_types = {col["name"]: col["proposed_dtype"] for col in schema["columns"]}

    return {
        "headers": headers,
        "rows": rows,
        "proposed_types": proposed_types,
        "schema": schema,
        "summary": summary,
    }
