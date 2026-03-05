"""
Column adjustment utilities for IoT CSV ingest.
Handles reading CSV headers, proposing schema mappings, and applying column adjustments (renaming, type casting, stacking).
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import polars as pl

from .storage import ensure_dirs, get_paths, safe_stem, update_status

_TYPE_MAP: Dict[str, pl.DataType] = {
    "Int64": pl.Int64,
    "Float64": pl.Float64,
    "Utf8": pl.Utf8,
    "Boolean": pl.Boolean,
}


def _read_columns(csv_path: str) -> List[str]:
    """
    Reads just the header of a CSV file to determine available columns.

    Args:
        csv_path (str): Path to the CSV file.

    Returns:
        List[str]: List of column names.
    """
    return pl.read_csv(csv_path, n_rows=0).columns


def _prepare_dtype_map(
    columns: Iterable[str],
    dtype_overrides: Dict[str, str],
) -> tuple[Dict[str, pl.DataType], List[str]]:
    """
    Prepares Polars dtype overrides and identifies datetime columns.

    Args:
        columns (Iterable[str]): Available columns in the CSV.
        dtype_overrides (Dict[str, str]): User-specified type overrides.

    Returns:
        tuple: (Dict[str, pl.DataType] for Polars, List[str] identifying datetime columns).

    Raises:
        ValueError: If an unsupported dtype is requested.
    """
    csv_columns = set(columns)
    dtypes: Dict[str, pl.DataType] = {}
    datetime_cols: List[str] = []

    for column, label in dtype_overrides.items():
        if column not in csv_columns:
            continue
        if label == "Datetime":
            # Read as Utf8 initially, then parse to Datetime later
            dtypes[column] = pl.Utf8
            datetime_cols.append(column)
        else:
            polars_dtype = _TYPE_MAP.get(label)
            if not polars_dtype:
                raise ValueError(f"Unsupported dtype override: {label}")
            dtypes[column] = polars_dtype
    return dtypes, datetime_cols


def _rename_columns(df: pl.DataFrame, name_mapping: Dict[str, str]) -> pl.DataFrame:
    """
    Renames columns using a mapping dictionary, ignoring missing entries.

    Args:
        df (pl.DataFrame): Input DataFrame.
        name_mapping (Dict[str, str]): Map of {old_name: new_name}.

    Returns:
        pl.DataFrame: DataFrame with renamed columns.
    """
    if not name_mapping:
        return df
    rename_map = {old: new for old, new in name_mapping.items() if old in df.columns}
    if not rename_map:
        return df
    return df.rename(rename_map)


def _select_allowed(df: pl.DataFrame, allowed_names: Iterable[str]) -> pl.DataFrame:
    """
    Keeps only columns that are allowed by the configuration/UI.

    Args:
        df (pl.DataFrame): Input DataFrame.
        allowed_names (Iterable[str]): List of allowed column names.

    Returns:
        pl.DataFrame: DataFrame with only allowed columns.
    """
    allowed_set = set(allowed_names)
    allowed_set.add("property_value")
    if not allowed_set:
        return df
    keep = [col for col in df.columns if col in allowed_set]
    if len(keep) == len(df.columns):
        return df
    return df.select(keep)


def _stack_property_columns(
    df: pl.DataFrame,
    property_columns: List[str],
    allowed_names: Iterable[str],
) -> tuple[pl.DataFrame, List[str]]:
    """
    Stacks columns mapped as properties into two columns: 'property' and 'property_value'.
    (Wide-to-Long transformation)

    Args:
        df (pl.DataFrame): Input DataFrame.
        property_columns (List[str]): Columns to be stacked as properties.
        allowed_names (Iterable[str]): Allowed identifier columns to keep fixed (id_vars).

    Returns:
        tuple: (Processed pl.DataFrame, List[str] of warnings).
    """
    if not property_columns:
        return df, []

    warnings: List[str] = []
    allowed_set = set(allowed_names)

    available_props = [col for col in property_columns if col in df.columns]
    missing_props = sorted(set(property_columns) - set(available_props))
    if missing_props:
        warnings.append(
            f"Property-mapped column(s) not found and skipped: {', '.join(missing_props)}."
        )
    if not available_props:
        return df, warnings

    id_vars = [
        col
        for col in df.columns
        if col in allowed_set and col not in available_props and col != "property"
    ]

    stacked = df.melt(
        id_vars=id_vars,
        value_vars=available_props,
        variable_name="property",
        value_name="property_value",
    )
    return stacked, warnings


def _polars_dtype_string(dtype: pl.DataType) -> str:
    """
    Returns a user-friendly string representation of a Polars DataType.
    """
    return dtype.__class__.__name__


def _load_and_transform(
    csv_path: str,
    dtype_overrides: Dict[str, str],
    name_mapping: Dict[str, str],
    allowed_names: Iterable[str],
    add_null_location: bool = False,
) -> tuple[pl.DataFrame, List[str]]:
    """
    Internal helper to load a CSV and apply transforms, returning the DataFrame and any warnings.

    Steps:
    1. Read CSV with specified dtypes.
    2. Convert datetime columns.
    3. Rename columns.
    4. Stack property columns (wide-to-long).
    5. Add null location column if requested.
    6. Select only allowed columns.

    Args:
        csv_path (str): Path to CSV.
        dtype_overrides (Dict[str, str]): Type overrides.
        name_mapping (Dict[str, str]): Column rename mapping.
        allowed_names (Iterable[str]): Allowed output columns.
        add_null_location (bool): Whether to inject a null 'location' column.

    Returns:
        tuple: (pl.DataFrame, List[str] warnings).
    """
    columns = _read_columns(csv_path)
    dtype_map, datetime_cols = _prepare_dtype_map(columns, dtype_overrides)

    df = pl.read_csv(csv_path, dtypes=dtype_map, try_parse_dates=True)

    warnings: List[str] = []

    property_columns = [col for col, target in name_mapping.items() if target == "property"]
    rename_mapping = {col: target for col, target in name_mapping.items() if target != "property"}

    for col in datetime_cols:
        try:
            converted = (
                df.select(pl.col(col).str.to_datetime(strict=False).alias(col))
                .to_series()
                .rename(col)
            )
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Column '{col}' could not be parsed as datetime ({exc}); kept as text.")
            continue
        if df.height > 0 and converted.null_count() == df.height:
            warnings.append(f"Column '{col}' did not contain parseable datetimes; kept as text.")
            continue
        df = df.with_columns(converted)

    df = _rename_columns(df, rename_mapping)
    df, property_warnings = _stack_property_columns(df, property_columns, allowed_names)
    warnings.extend(property_warnings)

    if add_null_location and "location" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("location"))
        warnings.append("Location column added with null values.")

    df = _select_allowed(df, allowed_names)
    return df, warnings


def apply_overrides(
    csv_path: str,
    dtype_overrides: Dict[str, str],
    name_mapping: Dict[str, str],
    allowed_names: List[str],
    add_null_location: bool = False,
    preview_rows: int = 20,
) -> Dict[str, object]:
    """
    Applies overrides (types, mapping) to a CSV and persists the result as a Parquet file.
    Updates the status cache upon success.

    Args:
        csv_path (str): Path to the source CSV.
        dtype_overrides (Dict[str, str]): Type overrides.
        name_mapping (Dict[str, str]): Column name/property mappings.
        allowed_names (List[str]): Allowed canonical names.
        add_null_location (bool): If True, adds a 'location' column with nulls.
        preview_rows (int): Number of rows to return in the preview.

    Returns:
        Dict[str, object]: Result dictionary with path, preview, columns, dtypes, and warnings.
    """
    df, warnings = _load_and_transform(
        csv_path,
        dtype_overrides,
        name_mapping,
        allowed_names,
        add_null_location=add_null_location,
    )

    ensure_dirs()
    paths = get_paths()
    adjusted_dir = Path(paths["adjusted_dir"])
    csv_name = Path(csv_path).name
    parquet_path = adjusted_dir / f"{safe_stem(csv_name)}.parquet"
    df.write_parquet(parquet_path, compression="zstd")

    update_status(Path(csv_path).name, True, str(parquet_path.resolve()))

    preview = df.head(preview_rows).to_dicts()
    final_columns = df.columns
    dtypes = {col: _polars_dtype_string(dtype) for col, dtype in df.schema.items()}

    return {
        "adjusted_path": str(parquet_path.resolve()),
        "preview": preview,
        "final_columns": final_columns,
        "dtypes": dtypes,
        "warnings": warnings,
    }


def read_preview_with_overrides(
    csv_path: str,
    dtype_overrides: Dict[str, str],
    name_mapping: Dict[str, str],
    allowed_names: List[str],
    n: int = 20,
) -> List[Dict[str, object]]:
    """
    Returns a preview of the processed data without persisting it to disk.
    Used for live previews in the UI.

    Args:
        csv_path (str): Path to source CSV.
        dtype_overrides (Dict[str, str]): Type overrides.
        name_mapping (Dict[str, str]): Column mappings.
        allowed_names (List[str]): Allowed columns.
        n (int): Rows to return.

    Returns:
        List[Dict[str, object]]: List of row dictionaries.
    """
    df, _ = _load_and_transform(
        csv_path,
        dtype_overrides,
        name_mapping,
        allowed_names,
    )
    return df.head(n).to_dicts()
