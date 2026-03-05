"""
Preview helpers for processed parquet outputs.
Provides functions to query and inspect the generated Parquet files using DuckDB.
"""

from __future__ import annotations

from typing import Dict, List

import duckdb


def preview_processed(out_path: str, limit: int = 20) -> List[Dict[str, object]]:
    """
    Returns a preview of the processed parquet file.

    Args:
        out_path (str): Path to the parquet file.
        limit (int): Maximum number of rows to return.

    Returns:
        List[Dict[str, object]]: A list of dictionaries representing the rows.
    """
    with duckdb.connect() as conn:
        query = "SELECT * FROM read_parquet(?) LIMIT ?"
        df = conn.execute(query, [out_path, limit]).df()
    return df.to_dict(orient="records")


def arbitrary_query_on_parquet(
    out_path: str,
    sql: str,
    limit: int = 1_000,
) -> Dict[str, object]:
    """
    Executes a limited SQL preview query on the parquet output.

    Args:
        out_path (str): Path to the parquet file.
        sql (str): The SQL query string.
        limit (int): Maximum number of rows to return.

    Returns:
        Dict[str, object]: A dictionary containing 'records' (list of dicts) and 'columns' (list of column names).

    Raises:
        ValueError: If the query is not a SELECT statement.
    """
    normalized = sql.strip().lower()
    if not normalized.startswith("select"):
        raise ValueError("Only SELECT queries are permitted.")

    # Wrap the query to use the parquet file as a CTE named 'dataset'
    query = f"WITH dataset AS (SELECT * FROM read_parquet('{out_path}')) {sql}"
    limited_query = f"{query} LIMIT {limit}"

    with duckdb.connect() as conn:
        rel = conn.execute(limited_query)
        df = rel.df()

    return {
        "records": df.to_dict(orient="records"),
        "columns": list(df.columns),
    }


def get_device_type_stats(out_path: str) -> List[Dict[str, object]]:
    """
    Returns device type statistics from the processed parquet file.
    Aggregates counts by 'device_type'.

    Args:
        out_path (str): Path to the parquet file.

    Returns:
        List[Dict[str, object]]: A list of dictionaries with 'device_type' and 'count'.
    """
    with duckdb.connect() as conn:
        query = "SELECT device_type, COUNT(*) as count FROM read_parquet(?) GROUP BY device_type ORDER BY count DESC"
        df = conn.execute(query, [out_path]).df()
    return df.to_dict(orient="records")
