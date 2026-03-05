from pathlib import Path
import shutil
from .db_utils import get_db

PROCESSED_PARQUET_PATH = "processed/processed_IoT_data.parquet"

def get_processed_path(get_paths_func):
    """
    Constructs the absolute path to the processed parquet file.

    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        str: Absolute path to the 'processed_IoT_data.parquet' file.
    """
    paths = get_paths_func()
    return str(Path(paths["processed_dir"]) / "processed_IoT_data.parquet")

def get_latest_ocel_path(get_paths_func):
    """
    Identifies the most recently modified OCEL 2.0 SQLite file in the upload directory.

    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        Path or None: Path object of the latest OCEL file, or None if no suitable file exists.
    """
    paths = get_paths_func()
    upload_dir = Path(paths["uploads_dir"])
    if not upload_dir.exists():
        return None
    
    # Looking for .sqlite, .db, .sqlite3 which are typical for OCEL 2.0 (SQLite)
    candidates = [p for p in upload_dir.glob("*") if p.suffix.lower() in {'.db', '.sqlite', '.sqlite3'}]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)

def get_integrated_ocel_path(get_paths_func):
    """
    Constructs the path to the integrated OCEL SQLite file.
    
    The integrated file is stored in a dedicated 'integrated' directory 
    sibling to the uploads directory.

    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        Path: Path object for 'integrated/integrated.sqlite'.
    """
    paths = get_paths_func()
    root_dir = Path(paths["uploads_dir"]).parent
    integrated_dir = root_dir / "integrated"
    integrated_dir.mkdir(parents=True, exist_ok=True)
    return integrated_dir / "integrated.sqlite"

def initialize_integrated_file(get_paths_func, overwrite=False):
    """
    Initializes the integrated OCEL file by copying the latest uploaded OCEL file.

    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.
        overwrite (bool): If True, replaces an existing integrated file.

    Returns:
        tuple: (bool, str) indicating success/failure and a status message.
    """
    source = get_latest_ocel_path(get_paths_func)
    if not source:
        return False, "No source OCEL file found in uploads."
        
    dest = get_integrated_ocel_path(get_paths_func)
    
    if dest.exists() and not overwrite:
        return True, "Integrated file exists. Using existing file."
        
    try:
        shutil.copy2(source, dest)
        return True, f"Copied {source.name} to integrated.sqlite"
    except Exception as e:
        return False, f"Error copying file: {e}"

def check_integrated_file_status(get_paths_func):
    """
    Checks the status of the integrated OCEL file.

    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        str: 'missing', 'exists_but_older', or 'exists'.
    """
    dest = get_integrated_ocel_path(get_paths_func)
    if not dest.exists():
        return "missing"
        
    # Check if source is newer
    source = get_latest_ocel_path(get_paths_func)
    if source and source.stat().st_mtime > dest.stat().st_mtime:
        return "exists_but_older"
        
    return "exists"

def attach_ocel_db(con, ocel_path):
    """
    Attaches the specified SQLite database to the current DuckDB connection as 'ocel_db'.

    Args:
        con (duckdb.DuckDBPyConnection): The active database connection.
        ocel_path (Path or str): Path to the SQLite database file.
    """
    # Detach if exists to avoid error or ensure fresh state
    try:
        con.execute("DETACH ocel_db")
    except:
        pass
        
    con.execute(f"ATTACH '{str(ocel_path)}' AS ocel_db (TYPE SQLITE)")

def init_integration_log(con):
    """
    Creates the integration log table if it doesn't exist.
    Tracks (device_type, ocel_type) pairs that have been successfully integrated.

    Args:
        con (duckdb.DuckDBPyConnection): The active database connection.
    """
    con.execute("CREATE TABLE IF NOT EXISTS ocel_db.integration_log (device_type VARCHAR, ocel_type VARCHAR, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(device_type, ocel_type))")

def get_interaction_levels():
    """
    Returns the distinct interaction levels available for selection.

    Returns:
        list: List of dictionaries with 'id' and 'label'.
    """
    return [
        {"id": "1", "label": "Single Event"},
        {"id": "2", "label": "Multiple Events"},
        {"id": "3", "label": "Process Level"}
    ]

def get_attribute_types():
    """
    Returns the distinct attribute types available for selection.

    Returns:
        list: List of dictionaries with 'id' and 'label'.
    """
    return [
        {"id": "1", "label": "Object Attribute"},
        {"id": "2", "label": "Event Attribute"}
    ]
