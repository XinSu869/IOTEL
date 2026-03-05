"""
Core logic for the IoT Data Integration module.
Handles the coordination between the processed IoT data (Parquet) and the OCEL 2.0 database (SQLite).
"""

from pathlib import Path
from .db_utils import get_db
from .persistence_helper import ensure_columns_exist, persist_object_attribute, persist_event_attribute
from .integration_scenarios import apply_process_logic
from .integration_utils import (
    get_processed_path,
    get_latest_ocel_path,
    get_integrated_ocel_path,
    attach_ocel_db,
    init_integration_log
)
from .integration_queries import (
    GET_UNIQUE_IOT_TYPES_QUERY,
    CREATE_SOURCE_IOT_DATA_QUERY,
    CREATE_IOT_OBJECT_MAP_QUERY,
    CREATE_IOT_EVENT_MAP_QUERY,
    REFINE_SOURCE_IOT_DATA_QUERY,
    CHECK_REMAINING_INTEGRATIONS_QUERY
)

def get_unique_iot_types(get_paths_func):
    """
    Returns a list of unique 'device_type' from the processed parquet file.
    Filters out device types that have already been fully integrated.
    
    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        list: List of device type strings.
    """
    parquet_path = get_processed_path(get_paths_func)
    con = get_db()
    try:
        if not Path(parquet_path).exists():
            return []
            
        # Ensure ocel_db is attached
        ocel_path = get_integrated_ocel_path(get_paths_func)
        
        # If no OCEL file, we can't filter, so return all found in parquet
        if not ocel_path.exists():
             query = f"SELECT DISTINCT device_type FROM read_parquet('{parquet_path}') ORDER BY device_type"
             result = con.execute(query).fetchall()
             return [row[0] for row in result if row[0] is not None]
        
        attach_ocel_db(con, ocel_path)
        init_integration_log(con)
        
        # Return device types that have AT LEAST ONE unintegrated ocel_type
        # i.e., in 'potential' but not in 'log'
        query = GET_UNIQUE_IOT_TYPES_QUERY.format(parquet_path=parquet_path)
        
        result = con.execute(query).fetchall()
        return [row[0] for row in result if row[0] is not None]
    except Exception as e:
        print(f"Error fetching IoT types: {e}")
        return []

def cache_iot_maps(get_paths_func, iot_type):
    """
    Executes Step 3.1 & 3.2: Generating iot_object_map and iot_event_map (Cached).
    Creates 'source_iot_data', 'iot_object_map', and 'iot_event_map' in the attached DB.
    
    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.
        iot_type (str): The selected IoT device type.

    Returns:
        list: List of dictionaries representing object types and their integration status.
    """
    parquet_path = get_processed_path(get_paths_func)
    con = get_db()
    
    # 1. Attach OCEL DB (INTEGRATED FILE)
    ocel_path = get_integrated_ocel_path(get_paths_func)
    
    if not ocel_path.exists():
        raise Exception("Integrated OCEL file not found. Please initialize.")
        
    attach_ocel_db(con, ocel_path)
    init_integration_log(con)

    con.execute("BEGIN TRANSACTION")
    try:
        # Create source_iot_data view/table
        con.execute("DROP TABLE IF EXISTS ocel_db.source_iot_data")
        con.execute(CREATE_SOURCE_IOT_DATA_QUERY.format(parquet_path=parquet_path), [iot_type])
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Error creating source_iot_data: {e}")
        raise e

    con.execute("BEGIN TRANSACTION")
    try:
        # Create iot_object_map (Step 3.1)
        con.execute("DROP TABLE IF EXISTS ocel_db.iot_object_map")
        con.execute(CREATE_IOT_OBJECT_MAP_QUERY)

        # Create iot_event_map (Step 3.2)
        con.execute("DROP TABLE IF EXISTS ocel_db.iot_event_map")
        con.execute(CREATE_IOT_EVENT_MAP_QUERY)
        
        # Query distinct ocel_types from the newly created iot_object_map
        res = con.execute("SELECT DISTINCT ocel_type FROM ocel_db.iot_object_map WHERE ocel_type IS NOT NULL ORDER BY ocel_type").fetchall()
        object_types = [r[0] for r in res]
        
        # Check current integration status
        final_types = []
        for ot in object_types:
            is_done = con.execute("SELECT 1 FROM ocel_db.integration_log WHERE device_type = ? AND ocel_type = ?", [iot_type, ot]).fetchone()
            final_types.append({"type": ot, "status": "integrated" if is_done else "pending"})

        con.execute("COMMIT")
        return final_types
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Error caching maps: {e}")
        raise e

def refine_iot_data(get_paths_func, iot_type, selected_ocel_type):
    """
    Step 1: Refines 'source_iot_data' to only include data for the selected object type
    OR if selected_ocel_type is "NONE", keeps all data for that device type.
    
    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.
        iot_type (str): The selected IoT device type.
        selected_ocel_type (str): The selected OCEL object type (or "NONE").

    Returns:
        bool: True if successful.
    """
    parquet_path = get_processed_path(get_paths_func)
    con = get_db()
    ocel_path = get_integrated_ocel_path(get_paths_func)
    attach_ocel_db(con, ocel_path)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DROP TABLE IF EXISTS ocel_db.source_iot_data")

        if selected_ocel_type == "NONE":
             # Case: No correlation. Use all data for this device type.
             con.execute(CREATE_SOURCE_IOT_DATA_QUERY.format(parquet_path=parquet_path), [iot_type])
        else:
             # Case: Specific object type selected.
             # Filter source_iot_data by device_ids that map to this object type.
             con.execute(REFINE_SOURCE_IOT_DATA_QUERY.format(parquet_path=parquet_path), [iot_type, selected_ocel_type])

        # RE-CREATE MAPS based on the refined data
        # This ensures Step 3/4 only see relevant objects/events
        
        con.execute("DROP TABLE IF EXISTS ocel_db.iot_object_map")
        con.execute(CREATE_IOT_OBJECT_MAP_QUERY)

        con.execute("DROP TABLE IF EXISTS ocel_db.iot_event_map")
        con.execute(CREATE_IOT_EVENT_MAP_QUERY)
        
        con.execute("COMMIT")
        return True
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Error refining data: {e}")
        raise e

def get_ocel_types(get_paths_func):
    """
    Returns dictionaries of types for Step 4 context.
    - object_types: from ocel_db.iot_object_map
    - mapped_event_types: from iot_event_map (cached)
    - valid_event_types: from ocel_db.event_map_type
    
    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        dict: various lists of types available in the OCEL.
    """
    ocel_path = get_integrated_ocel_path(get_paths_func)
    if not ocel_path or not ocel_path.exists():
        return {"object_types": [], "mapped_event_types": [], "valid_event_types": []}
        
    con = get_db()
    try:
        attach_ocel_db(con, ocel_path)
        
        # 1. Object Types (from iot_object_map)
        try:
            res_obj = con.execute("SELECT DISTINCT ocel_type FROM ocel_db.iot_object_map WHERE ocel_type IS NOT NULL ORDER BY ocel_type").fetchall()
            object_types = [r[0] for r in res_obj]
        except:
             object_types = []

        # 2. Valid Event Types (from event_map_type or event table fallback)
        try:
            res_ev = con.execute("SELECT DISTINCT ocel_type FROM ocel_db.event_map_type ORDER BY ocel_type").fetchall()
            valid_event_types = [r[0] for r in res_ev]
        except:
             res_ev = con.execute("SELECT DISTINCT ocel_type FROM ocel_db.event ORDER BY ocel_type").fetchall()
             valid_event_types = [r[0] for r in res_ev]
        
        # 3. Mapped Event Types (from iot_event_map)
        mapped_event_types = []
        try:
             res_map = con.execute("SELECT DISTINCT ocel_type FROM ocel_db.iot_event_map WHERE ocel_type IS NOT NULL ORDER BY ocel_type").fetchall()
             mapped_event_types = [r[0] for r in res_map]
        except Exception:
            mapped_event_types = []       
            
        return {
            "object_types": object_types, 
            "valid_event_types": valid_event_types,
            "mapped_event_types": mapped_event_types
        }
    except Exception as e:
        print(f"Error fetching OCEL types: {e}")
        return {"object_types": [], "mapped_event_types": [], "valid_event_types": []}

def get_iot_properties(get_paths_func):
    """
    Returns a list of distinct properties available in the source_iot_data.
    
    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        list: List of property strings.
    """
    ocel_path = get_integrated_ocel_path(get_paths_func)
    if not ocel_path or not ocel_path.exists():
        return []
        
    con = get_db()
    try:
        attach_ocel_db(con, ocel_path)
        res = con.execute("SELECT DISTINCT property FROM ocel_db.source_iot_data WHERE property IS NOT NULL ORDER BY property").fetchall()
        return [r[0] for r in res]
    except Exception as e:
        print(f"Error fetching properties: {e}")
        return []

def check_remaining_integrations(con, get_paths_func):
    """
    Checks if there are any unintegrated (device_type, ocel_type) pairs.
    
    Args:
        con (duckdb.DuckDBPyConnection): Active DB connection.
        get_paths_func (function): Function returning a dictionary of configured paths.

    Returns:
        bool: True if integrations remain, False otherwise.
    """
    parquet_path = get_processed_path(get_paths_func)
    if not Path(parquet_path).exists():
        return False

    ocel_path = get_integrated_ocel_path(get_paths_func)
    if not ocel_path or not ocel_path.exists():
        return False
        
    try:
        query = CHECK_REMAINING_INTEGRATIONS_QUERY.format(parquet_path=parquet_path)
        remaining_count = con.execute(query).fetchone()[0]
        return remaining_count > 0
        
    except Exception as e:
        print(f"Error checking remaining integrations: {e}")
        return True

def execute_integration(get_paths_func, payload):
    """
    Executes Step 5 and Persistence logic.
    Applies logic to create temporary tables and then persists them to the OCEL DB.
    
    Args:
        get_paths_func (function): Function returning a dictionary of configured paths.
        payload (dict): The payload containing configuration for integration.

    Returns:
        dict: Result status and message.
    """
    con = get_db()
    ocel_path = get_integrated_ocel_path(get_paths_func)
    if not ocel_path or not ocel_path.exists():
        raise Exception("No Integrated OCEL file found")
        
    attach_ocel_db(con, ocel_path)
    attach_ocel_db(con, ocel_path) # Double attach harmless, ensures it works
    
    try:
        # 1. Process Logic
        process_logic = payload.get("process_logic", "1")
        details = payload.get("process_details", {})
        
        # Pass context and attribute_type into details for apply_process_logic
        details["context"] = payload.get("context", {})
        details["attribute_type"] = payload.get("attribute_type", "1")
        
        apply_process_logic(con, process_logic, details)

        # Start Transaction for Persistence Phase
        con.execute("BEGIN TRANSACTION")

        # 2. Persistence
        # Copy from TEMP to OCEL_DB
        con.execute("DROP TABLE IF EXISTS ocel_db.processed_iot_payload")
        con.execute("CREATE TABLE ocel_db.processed_iot_payload AS SELECT * FROM temp_processed_payload")
        con.execute("DROP TABLE temp_processed_payload")
        
        attr_type = payload.get("attribute_type")
        context = payload.get("context", {})
        
        # Get distinctive properties to check schema
        props = [r[0] for r in con.execute("SELECT DISTINCT property FROM ocel_db.processed_iot_payload").fetchall()]
        
        stats = {'inserted': 0, 'skipped': 0}

        if attr_type == "1": # Object Attribute
            # Target derived from iot_object_map.object_table_name based on Step 4 selection
            res = con.execute("SELECT object_table_name FROM ocel_db.iot_object_map WHERE ocel_type = ? LIMIT 1", [context.get("ocel_type")]).fetchone()
            if not res:
                raise Exception(f"No target object table found for the selected object type: {context.get('ocel_type')}")
            target_table = res[0]
            
            # Migration
            ensure_columns_exist(con, f"ocel_db.{target_table}", props)
            
            # Insert with Decoupled Exists Check
            stats = persist_object_attribute(con, f"ocel_db.{target_table}", context)
            
        elif attr_type == "2": # Event Attribute
            # Target derived from event_map_type based on Step 4 selection
            res = con.execute("SELECT ocel_type_map FROM ocel_db.event_map_type WHERE ocel_type = ? LIMIT 1", [context.get("ocel_type")]).fetchone()
            if not res:
                  raise Exception(f"No target event table found for the selected event type: {context.get('ocel_type')}")
            target_table = "event_" + res[0]
            
            # Migration
            ensure_columns_exist(con, f"ocel_db.{target_table}", props)
            
            # Merge with Decoupled Exists Check
            stats = persist_event_attribute(con, f"ocel_db.{target_table}", context)

        # Log Success
        try:
            device_type = payload.get('iot_type')
            if not device_type:
                dt_res = con.execute("SELECT DISTINCT device_type FROM ocel_db.source_iot_data").fetchone()
                device_type = dt_res[0] if dt_res else "UNKNOWN"

            mapped_type = payload.get('selected_ocel_type', 'NONE')
            
            con.execute("DELETE FROM ocel_db.integration_log WHERE device_type = ? AND ocel_type = ?", [device_type, mapped_type])
            con.execute("INSERT INTO ocel_db.integration_log (device_type, ocel_type) VALUES (?, ?)", [device_type, mapped_type])
        except Exception as log_e:
            print(f"Warning: Failed to write to integration log: {log_e}")

        con.execute("COMMIT")
        
        # Check if any remaining integrations exist
        has_remaining = check_remaining_integrations(con, get_paths_func)
        
        # Construct Response
        total_inserted = stats['main_inserted'] + stats['aux_inserted']
        total_skipped = stats['main_skipped'] + stats['aux_skipped']
        
        status = "success"
        msg = "Integration completed."
        
        if total_inserted == 0 and total_skipped > 0:
            status = "warning"
            msg = "Data already exists. No new data was inserted."
        elif total_inserted > 0:
            details = []
            if stats['main_inserted'] > 0:
                details.append(f"{stats['main_inserted']} attributes inserted")
            if stats['aux_inserted'] > 0:
                details.append(f"{stats['aux_inserted']} object links created")
            if total_skipped > 0:
                details.append(f"{total_skipped} entries skipped")
            msg = f"{msg} ({', '.join(details)})."
             
        return {
            "status": status, 
            "message": msg,
            "all_done": not has_remaining
        }
            
    except Exception as e:
        try:
            con.execute("ROLLBACK")
        except:
            pass
        print(f"Integration error: {e}")
        raise e
