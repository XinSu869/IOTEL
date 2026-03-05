"""
Helper module for persisting processed IoT data into the OCEL database.
Handles schema migration (column addition) and data insertion/merging for both Object and Event attributes.
"""

import duckdb

def ensure_columns_exist(con, table_name, properties):
    """
    Checks if the specified columns exist in the target table and adds them if missing.
    Defaults new columns to DOUBLE type.
    
    Args:
        con (duckdb.DuckDBPyConnection): Active DB connection.
        table_name (str): The target table name (including schema, e.g. 'ocel_db.object_truck').
        properties (list): List of property names to check/add.
    """
    try:
        existing_cols = {r[0].lower() for r in con.execute(f"DESCRIBE {table_name}").fetchall()}
        
        # Deduplicate properties to avoid repeated attempts
        unique_props = set(properties)

        for prop in unique_props:
            if prop.lower() not in existing_cols:
                try:
                    # Add column. Default to DOUBLE.
                    # Using IF NOT EXISTS if supported by the DB version, otherwise try-except handles it.
                    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {prop} DOUBLE")
                    existing_cols.add(prop.lower())
                except Exception as col_err:
                     # Ignore "column already exists" errors explicitly if needed
                     if "already exists" not in str(col_err).lower():
                         print(f"Error adding column {prop} to {table_name}: {col_err}")
    except Exception as e:
        print(f"Schema migration error ({table_name}): {e}")

def persist_object_attribute(con, target_table, context):
    """
    Persists object attributes (Scenario 1).
    Performs an INSERT operation for new records and updates auxiliary object-object links.
    Uses a decoupled existence check to accurately report statistics.
    
    Args:
        con (duckdb.DuckDBPyConnection): Active DB connection.
        target_table (str): Data target table.
        context (dict): Context containing association definitions.

    Returns:
        dict: Statistics keys: 'main_inserted', 'aux_inserted', 'main_skipped', 'aux_skipped'.
    """
    stats = {'main_inserted': 0, 'aux_inserted': 0, 'main_skipped': 0, 'aux_skipped': 0}
    
    # 1. Pivot payload to have one row per (location, result_time)
    con.execute("DROP TABLE IF EXISTS pivoted_payload")
    con.execute("""
        CREATE TEMPORARY TABLE pivoted_payload AS
        PIVOT ocel_db.processed_iot_payload 
        ON property 
        USING first(property_value) 
        GROUP BY location, result_time
    """)
    
    # 2. Insert into Target Table
    cols = [r[0] for r in con.execute("DESCRIBE pivoted_payload").fetchall()]
    prop_cols = [c for c in cols if c not in ('location', 'result_time')]
    
    for prop in prop_cols:
        # Check existence before inserting
        
        # Count potential inserts (where property is not null)
        count_query = f"""
            SELECT COUNT(*) FROM pivoted_payload p
            WHERE {prop} IS NOT NULL
        """
        potential = con.execute(count_query).fetchone()[0]
        
        # Count duplicates (already existing in Main Table for this field/time/object)
        dup_query = f"""
            SELECT COUNT(*) FROM pivoted_payload p
            JOIN {target_table} t 
            ON t.ocel_id = p.location
            AND t.ocel_time = p.result_time
            AND t.ocel_changed_field = '{prop}'
            WHERE p.{prop} IS NOT NULL
        """
        duplicates = con.execute(dup_query).fetchone()[0]
        
        inserted = potential - duplicates
        
        # INSERT ... WHERE NOT EXISTS
        insert_query = f"""
            INSERT INTO {target_table} (ocel_id, ocel_time, ocel_changed_field, {prop})
            SELECT 
                location, 
                result_time, 
                '{prop}', 
                {prop}
            FROM pivoted_payload p
            WHERE {prop} IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM {target_table} t
                WHERE t.ocel_id = p.location
                AND t.ocel_time = p.result_time
                AND t.ocel_changed_field = '{prop}'
            )
        """
        con.execute(insert_query)
        
        stats['main_inserted'] += inserted
        stats['main_skipped'] += duplicates

    
    # 3. Aux Table: object_IoTobject (Associations)
    associations = context.get("associations") or []
    if not associations:
         q = context.get("ocel_qualifier", "")
         associations = [{"ocel_qualifier": q, "event_type": "ALL"}] 

    for assoc in associations:
        qual = assoc.get("ocel_qualifier")
        event_type = assoc.get("event_type", "ALL")
        
        # Create aux table if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS ocel_db.object_IoTobject (
                ocel_object_id VARCHAR, 
                ocel_IoT_object_id VARCHAR, 
                ocel_qualifier VARCHAR, 
                ocel_time TIMESTAMP,
                PRIMARY KEY (ocel_object_id, ocel_IoT_object_id, ocel_qualifier, ocel_time)
            )
        """)
        
        con.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_object_iot_pk 
            ON ocel_db.main.object_IoTobject (ocel_object_id, ocel_IoT_object_id, ocel_qualifier, ocel_time)
        """)

        # Determine Source Table/Query for this association
        source_query_base = ""
        if event_type != "ALL":
             source_query_base = f"""
                SELECT DISTINCT p.location, p.device_id, '{qual}', p.result_time
                FROM ocel_db.processed_iot_payload p
                JOIN (
                    SELECT DISTINCT ocel_event_id 
                    FROM ocel_db.iot_event_map 
                    WHERE ocel_type = '{event_type}'
                ) m ON p.ocel_event_id = m.ocel_event_id
             """
        else:
             source_query_base = f"""
                SELECT DISTINCT p.location, p.device_id, '{qual}', p.result_time
                FROM ocel_db.processed_iot_payload p
             """

        # Calculate potential for Aux
        aux_potential_query = f"SELECT COUNT(*) FROM ({source_query_base})"
        aux_potential = con.execute(aux_potential_query).fetchone()[0]

        # Calculate Aux duplicates
        aux_dup_query_refined = f"""
            SELECT COUNT(*)
            FROM ({source_query_base}) src
            JOIN ocel_db.object_IoTobject t
            ON t.ocel_object_id = src.location
            AND t.ocel_IoT_object_id = src.device_id
            AND t.ocel_qualifier = '{qual}'
            AND t.ocel_time = src.result_time
        """
        aux_duplicates = con.execute(aux_dup_query_refined).fetchone()[0]
        
        aux_inserted = aux_potential - aux_duplicates
        
        # Insert Link if not exists
        insert_aux_query = f"""
            INSERT INTO ocel_db.object_IoTobject (ocel_object_id, ocel_IoT_object_id, ocel_qualifier, ocel_time)
            SELECT src.location, src.device_id, '{qual}', src.result_time
            FROM ({source_query_base}) src
            WHERE NOT EXISTS (
                 SELECT 1 FROM ocel_db.object_IoTobject t
                 WHERE t.ocel_object_id = src.location
                 AND t.ocel_IoT_object_id = src.device_id
                 AND t.ocel_qualifier = '{qual}'
                 AND t.ocel_time = src.result_time
            )
        """
        con.execute(insert_aux_query)
        
        stats['aux_inserted'] += aux_inserted
        stats['aux_skipped'] += aux_duplicates
        
    return stats

def persist_event_attribute(con, target_table, context):
    """
    Persists event attributes (Scenario 2).
    Updates existing events in the target table with new attributes (merging).
    Updates auxiliary event-object links.
    
    Strategy:
    - Pivots the payload by event ID.
    - Updates the target table where the target column is NULL and the source has a value.
    - Skips update if the target column is already populated.
    
    Args:
        con (duckdb.DuckDBPyConnection): Active DB connection.
        target_table (str): Data target table.
        context (dict): Context containing qualifier definitions.

    Returns:
        dict: Statistics keys: 'main_inserted', 'aux_inserted', 'main_skipped', 'aux_skipped'.
    """
    stats = {'main_inserted': 0, 'aux_inserted': 0, 'main_skipped': 0, 'aux_skipped': 0}
    
    # 1. Pivot
    con.execute("DROP TABLE IF EXISTS pivoted_payload")
    con.execute("""
        CREATE TEMPORARY TABLE pivoted_payload AS
        PIVOT ocel_db.processed_iot_payload 
        ON property 
        USING first(property_value) 
        GROUP BY ocel_event_id, result_time
    """)
    
    # 2. Main Table Update (Decoupled)
    cols = [r[0] for r in con.execute("DESCRIBE pivoted_payload").fetchall()]
    prop_cols = [c for c in cols if c not in ('ocel_event_id', 'result_time')]
    
    # Calculate Potential: Rows in Payload that exist in Target Table.
    potential_main_query = f"""
        SELECT COUNT(*)
        FROM pivoted_payload src
        JOIN {target_table} tgt ON tgt.ocel_id = src.ocel_event_id
    """
    potential_main = con.execute(potential_main_query).fetchone()[0]

    # Calculate actual updates needed.
    # We only update if (Target is NULL AND Source is NOT NULL) for at least one column.
    or_conditions = []
    for p in prop_cols:
        or_conditions.append(f"(tgt.{p} IS NULL AND src.{p} IS NOT NULL)")
    
    where_update_needed = " OR ".join(or_conditions) if or_conditions else "0=1"
    
    main_update_count_query = f"""
        SELECT COUNT(*)
        FROM pivoted_payload src
        JOIN {target_table} tgt ON tgt.ocel_id = src.ocel_event_id
        WHERE {where_update_needed}
    """
    main_inserted = con.execute(main_update_count_query).fetchone()[0]
    main_skipped = potential_main - main_inserted
    
    # Execute Update
    # Logic: SET col = CASE WHEN tgt.col IS NOT NULL THEN tgt.col ELSE src.col END
    # This prevents overwriting existing data.
    
    set_clause_parts = []
    for p in prop_cols:
        set_clause_parts.append(f"{p} = CASE WHEN tgt.{p} IS NOT NULL THEN tgt.{p} ELSE src.{p} END")
    
    set_clause = ", ".join(set_clause_parts)
    
    if prop_cols:
        con.execute(f"""
            UPDATE {target_table} tgt
            SET {set_clause}
            FROM pivoted_payload src
            WHERE tgt.ocel_id = src.ocel_event_id
        """)
    
    stats['main_inserted'] = main_inserted
    stats['main_skipped'] = main_skipped

    # 3. Aux Table: event_IoTobject
    
    qual = context.get("ocel_qualifier", "")
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS ocel_db.event_IoTobject (
            ocel_event_id VARCHAR, 
            ocel_IoT_object_id VARCHAR, 
            ocel_qualifier VARCHAR,
            PRIMARY KEY (ocel_event_id, ocel_IoT_object_id, ocel_qualifier)
        )
    """)
    
    # Identify New Links
    aux_potential_query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT p.ocel_event_id, p.device_id
            FROM ocel_db.processed_iot_payload p
            WHERE p.ocel_event_id IS NOT NULL
        )
    """
    aux_potential = con.execute(aux_potential_query).fetchone()[0]
    
    aux_dup_query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT p.ocel_event_id, p.device_id
            FROM ocel_db.processed_iot_payload p
            WHERE p.ocel_event_id IS NOT NULL
        ) src
        JOIN ocel_db.event_IoTobject t
        ON t.ocel_event_id = src.ocel_event_id
        AND t.ocel_IoT_object_id = src.device_id
        AND t.ocel_qualifier = '{qual}'
    """
    aux_duplicates = con.execute(aux_dup_query).fetchone()[0]
    
    aux_inserted = aux_potential - aux_duplicates
    
    # Insert Links
    con.execute(f"""
        INSERT INTO ocel_db.event_IoTobject (ocel_event_id, ocel_IoT_object_id, ocel_qualifier)
        SELECT DISTINCT
            p.ocel_event_id,
            p.device_id,
            '{qual}'
        FROM ocel_db.processed_iot_payload p
        WHERE p.ocel_event_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM ocel_db.event_IoTobject t
            WHERE t.ocel_event_id = p.ocel_event_id
            AND t.ocel_IoT_object_id = p.device_id
            AND t.ocel_qualifier = '{qual}'
        )
    """)
        
    stats['aux_inserted'] = aux_inserted
    stats['aux_skipped'] = aux_duplicates
            
    return stats
