
import duckdb

def apply_process_logic(con, logic_type, details):
    """
    Generates 'processed_iot_payload' based on logic type and Context Scenarios (A, B, C).
    Uses CTEs to avoid intermediate View creation locks.
    """
    print("DEBUG: apply_process_logic start")
    
    # Extract Context
    context = details.get("context", {})
    attr_type = details.get("attribute_type", "1") # 1=Object, 2=Event
    associations = context.get("associations") or []

    # Determine Scenario
    scenario = "B"
    if attr_type == "1":
        if associations:
            scenario = "A" # Object Attribute (With Events)
        else:
            scenario = "B" # Object Attribute (No Events)
    elif attr_type == "2":
        scenario = "C" # Event Attribute

    print(f"DEBUG: Scenario {scenario}, Logic {logic_type}")

    # --- SCENARIO IMPLEMENTATIONS ---
    
    # Common Cleanup: We will create a local temp table first to avoid locking ocel_db during read/write
    con.execute("DROP TABLE IF EXISTS temp_processed_payload")

    if scenario == "A":
        # Scenario A: Object Attribute (With Events)
        
        event_types = set(a["event_type"] for a in associations)
        types_sql = ", ".join(f"'{t}'" for t in event_types)
        
        # CTE Definitions
        cte_base = "base_payload AS (SELECT * FROM ocel_db.source_iot_data)"
        
        # 1. Identify relevant tables from iot_event_map
        try:
            t_query = f"SELECT DISTINCT event_table_name FROM ocel_db.iot_event_map WHERE ocel_type IN ({types_sql})"
            target_tables = [r[0] for r in con.execute(t_query).fetchall() if r[0]]
        except Exception as e:
            print(f"Error fetching event tables: {e}")
            target_tables = []
        
        if not target_tables:
            raise Exception(f"No target event tables found for event types: {types_sql}")

        # 2. Stack tables (UNION ALL)
        union_parts = [f"SELECT ocel_id, ocel_time FROM ocel_db.{t}" for t in target_tables]
        union_query = " UNION ALL ".join(union_parts)

        cte_events = f"""
            target_events AS (
                {union_query}
            )
        """
        
        cte_joined = f"""
            scenario_a_joined AS (
                SELECT 
                    b.*,
                    e.ocel_time as event_time,
                    e.ocel_id as ocel_event_id
                FROM base_payload b
                JOIN ocel_db.iot_event_map m ON b.device_id = m.device_id 
                JOIN target_events e ON m.ocel_event_id = e.ocel_id and b.result_time = e.ocel_time
                WHERE m.ocel_type IN ({types_sql})
            )
        """
        
        # 3. Apply Logic
        if logic_type in ["1", "2"]: # No Process or Filter
            where_clause = "1=1"
            if logic_type == "2":
                where_clause = _build_filter_clause(details.get("filter", {}))

            # It's possible that there are multiple values for the same event_time
            query = f"""
                CREATE TEMPORARY TABLE temp_processed_payload AS
                WITH {cte_base}, {cte_events}, {cte_joined}
                SELECT 
                    device_id, device_type, location, property, property_value,
                    ocel_event_id,
                    event_time as result_time
                FROM scenario_a_joined
                WHERE {where_clause}
            """
            
        elif logic_type == "3": # Aggregate
            raw_func = details.get("aggregate", {}).get("func", "mean").lower()
            func = "AVG" if raw_func == "mean" else raw_func.upper()
            window = int(details.get("aggregate", {}).get("window", 0))
            
            query = f"""
                CREATE TEMPORARY TABLE temp_processed_payload AS
                WITH {cte_base}, {cte_events}, {cte_joined}
                SELECT
                    device_id, device_type, location, property,
                    ocel_event_id,
                    event_time as result_time,
                    {func}(CAST(property_value AS DOUBLE)) as property_value
                FROM scenario_a_joined
                WHERE CAST(result_time AS TIMESTAMP) BETWEEN (CAST(event_time AS TIMESTAMP) - INTERVAL {window} MINUTE) AND CAST(event_time AS TIMESTAMP)
                GROUP BY event_id, event_time, device_type, device_id, location, property
            """
            
        print("DEBUG: Executing Scenario A Query (to TEMP)")
        con.execute(query)


    elif scenario == "B":
        # Scenario B: Object Attribute (No Events)
        cte_base = "base_payload AS (SELECT * FROM ocel_db.source_iot_data)"

        if logic_type in ["1", "2"]: # No Process or Filter
            where_clause = "1=1"
            if logic_type == "2":
                where_clause = _build_filter_clause(details.get("filter", {}))
                
            query = f"""
                CREATE TEMPORARY TABLE temp_processed_payload AS
                WITH {cte_base}
                SELECT device_id, device_type, location, property, property_value, result_time
                FROM base_payload
                WHERE {where_clause}
            """
            
        elif logic_type == "3": # Aggregate
            raw_func = details.get("aggregate", {}).get("func", "mean").lower()
            func = "AVG" if raw_func == "mean" else raw_func.upper()
            window = int(details.get("aggregate", {}).get("window", 0))

            query = f"""
                CREATE TEMPORARY TABLE temp_processed_payload AS
                WITH {cte_base}
                SELECT
                    device_id, device_type, location, property,
                    MAX(result_time) as result_time,
                    time_bucket(INTERVAL {window} MINUTE, CAST(result_time AS TIMESTAMP)) as time_window,
                    {func}(CAST(property_value AS DOUBLE)) as property_value
                FROM base_payload
                GROUP BY device_type, device_id, location, property, time_window
            """

        print("DEBUG: Executing Scenario B Query (to TEMP)")
        con.execute(query)


    elif scenario == "C":
        # Scenario C: Event Attribute
        cte_base = "base_payload AS (SELECT * FROM ocel_db.source_iot_data)"
        target_type = context.get("ocel_type")
        
        # 1. Determine Target Table
        target_table_name = None
        try:
            # Check iot_event_map
            r1 = con.execute("SELECT event_table_name FROM ocel_db.iot_event_map WHERE ocel_type = ? LIMIT 1", [target_type]).fetchone()
            if r1 and r1[0]:
                target_table_name = r1[0]
                print(f"DEBUG: Target table for Scenario C: {target_table_name}")
            else:
                # Check event_map_type (for valid types not yet mapped in iot_map)
                r2 = con.execute("SELECT ocel_type_map FROM ocel_db.event_map_type WHERE ocel_type = ? LIMIT 1", [target_type]).fetchone()
                if r2 and r2[0]:
                    target_table_name = f"event_{r2[0]}"
                    print(f"DEBUG: Target table for Scenario C: {target_table_name}")
        except Exception as e:
            print(f"Error determining target table for Scenario C: {e}")

        cte_events_c = f"""
            target_events_c AS (
                SELECT ocel_id, ocel_time
                FROM ocel_db.{target_table_name}
            )
        """

        # 2. Determine Join Strategy
        is_mapped = False
        try:
             res_mapped = con.execute("SELECT 1 FROM ocel_db.iot_event_map WHERE ocel_type = ? LIMIT 1", [target_type]).fetchone()
             if res_mapped: is_mapped = True
        except:
             pass
             
        # Calculate Lookback Window
        # Case 3 (Aggregate): Use user-defined window
        # Case 1/2 (No Process/Filter): Use 60 minutes (1 Hour) default for "Closest Before"
        lookback_minutes = 60
        if logic_type == "3":
            lookback_minutes = int(details.get("aggregate", {}).get("window", 0))

        if is_mapped:
            # Map-based Join
            # Joins valid readings for the mapped device.
            cte_joined_c = f"""
                scenario_c_base AS (
                    SELECT 
                        b.device_id, b.device_type, b.location, b.property, b.property_value,
                        b.result_time as iot_source_time,
                        e.ocel_id as ocel_event_id,
                        e.ocel_time as result_time
                    FROM base_payload b
                    JOIN ocel_db.iot_event_map m ON b.device_id = m.device_id 
                    JOIN target_events_c e ON m.ocel_event_id = e.ocel_id
                    WHERE CAST(b.result_time AS TIMESTAMP) <= e.ocel_time
                    AND CAST(b.result_time AS TIMESTAMP) >= (e.ocel_time - INTERVAL {lookback_minutes} MINUTE)
                )
            """
        else:
             # Time-based Join (No Map)
             # Join readings within Lookback Window.
             cte_joined_c = f"""
                scenario_c_base AS (
                    SELECT 
                        b.device_id, b.device_type, b.location, b.property, b.property_value,
                        b.result_time as iot_source_time,
                        e.ocel_id as ocel_event_id,
                        e.ocel_time as result_time
                    FROM base_payload b, target_events_c e
                    WHERE CAST(b.result_time AS TIMESTAMP) <= e.ocel_time
                    AND CAST(b.result_time AS TIMESTAMP) >= (e.ocel_time - INTERVAL {lookback_minutes} MINUTE)
                )
            """

        # 3. Apply Filter / Aggregation
        if logic_type in ["1", "2"]:
            # Case 1/2: Select Closest Reading (One per Event)
            
            where_clause = "1=1"
            if logic_type == "2":
                where_clause = _build_filter_clause(details.get("filter", {}))
                
            query = f"""
                CREATE TEMPORARY TABLE temp_processed_payload AS
                WITH {cte_base}, {cte_events_c}, {cte_joined_c},
                scenario_c_closest AS (
                    SELECT * FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (PARTITION BY ocel_event_id, property ORDER BY iot_source_time DESC) as rn
                        FROM scenario_c_base
                    ) sub
                    WHERE rn = 1
                )
                SELECT 
                    device_id, device_type, location, property, property_value,
                    ocel_event_id, result_time
                FROM scenario_c_closest
                WHERE {where_clause}
            """
            
        elif logic_type == "3": # Aggregate
            raw_func = details.get("aggregate", {}).get("func", "mean").lower()
            func = "AVG" if raw_func == "mean" else raw_func.upper()
            window = int(details.get("aggregate", {}).get("window", 0))

            # Use Base CTE directly (already filtered by Lookback Window in cte_joined_c)
            # Note: We group by event and device specific columns to ensure correct aggregation buckets
            query = f"""
                CREATE TEMPORARY TABLE temp_processed_payload AS
                WITH {cte_base}, {cte_events_c}, {cte_joined_c}
                SELECT
                    device_id, device_type, location, property,
                    ocel_event_id,
                    result_time,
                    {func}(CAST(property_value AS DOUBLE)) as property_value
                FROM scenario_c_base
                GROUP BY ocel_event_id, result_time, device_type, device_id, location, property
            """

        print("DEBUG: Executing Scenario C Query (to TEMP)")
        con.execute(query)

    # FINAL STEP: Cleanup handled by caller (execute_integration)
    # The temp_processed_payload (TEMPORARY) table now exists and contains the results.
    print("DEBUG: apply_process_logic finished (Result in temp_processed_payload)")
    
    print("DEBUG: apply_process_logic finished")

def _build_filter_clause(f_details):
    cond_list = f_details.get("conditions", [])
    
    # Fallback to older format if 'conditions' key is missing, or return 1=1 if empty
    if not cond_list:
        lower = f_details.get("lower", {})
        upper = f_details.get("upper", {})
        
        old_conds = []
        if lower.get("val"):
            op = ">" if lower.get("op") == "gt" else ">="
            old_conds.append(f"CAST(property_value AS DOUBLE) {op} {float(lower['val'])}")
        if upper.get("val"):
            op = "<" if upper.get("op") == "lt" else "<="
            old_conds.append(f"CAST(property_value AS DOUBLE) {op} {float(upper['val'])}")
        return " AND ".join(old_conds) if old_conds else "1=1"
        
    sql_parts = []
    
    for i, cond in enumerate(cond_list):
        prop = cond.get("property")
        if not prop:
            continue
            
        logic = cond.get("logic", "OR") if i > 0 else ""
        lower = cond.get("lower", {})
        upper = cond.get("upper", {})
        
        prop_conds = []
        # Filter strictly by the requested property
        prop_conds.append(f"property = '{prop}'")
        
        if lower.get("val"):
            op = ">" if lower.get("op") == "gt" else ">="
            prop_conds.append(f"CAST(property_value AS DOUBLE) {op} {float(lower['val'])}")
        if upper.get("val"):
            op = "<" if upper.get("op") == "lt" else "<="
            prop_conds.append(f"CAST(property_value AS DOUBLE) {op} {float(upper['val'])}")
            
        # Group this property's whole condition in parentheses
        group_sql = "(" + " AND ".join(prop_conds) + ")"
        
        if i == 0:
            sql_parts.append(group_sql)
        else:
            sql_parts.append(f"{logic} {group_sql}")
            
    if sql_parts:
        return "(" + " " .join(sql_parts) + ")"
    return "1=1"
