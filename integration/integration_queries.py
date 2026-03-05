"""
SQL Queries for Integration Logic.
"""

# Query to fetch unique device types that have at least one unintegrated ocel_type
GET_UNIQUE_IOT_TYPES_QUERY = """
    SELECT DISTINCT potential.device_type 
    FROM (
        SELECT DISTINCT 
            p.device_type, 
            COALESCE(b.ocel_type, 'NONE') as ocel_type
        FROM read_parquet('{parquet_path}') p
        LEFT JOIN ocel_db.object b ON p.location = b.ocel_id
    ) potential
    LEFT JOIN ocel_db.integration_log log 
    ON potential.device_type = log.device_type 
    AND potential.ocel_type = log.ocel_type
    WHERE log.device_type IS NULL
    ORDER BY potential.device_type
"""

# Query to create 'source_iot_data' table from parquet
CREATE_SOURCE_IOT_DATA_QUERY = """
    CREATE TABLE ocel_db.source_iot_data AS 
    SELECT * FROM read_parquet('{parquet_path}') 
    WHERE device_type = ?
"""

# Query to create 'iot_object_map' table
CREATE_IOT_OBJECT_MAP_QUERY = """
    CREATE TABLE ocel_db.iot_object_map AS
    SELECT 
        a.*,
        b.ocel_type,
        c.ocel_type_map,
        'object_' || c.ocel_type_map AS object_table_name
    FROM (
        select distinct device_id, location from ocel_db.source_iot_data
    ) a
    LEFT JOIN ocel_db.object b ON a.location = b.ocel_id
    LEFT JOIN ocel_db.object_map_type c ON b.ocel_type = c.ocel_type
"""

# Query to create 'iot_event_map' table
CREATE_IOT_EVENT_MAP_QUERY = """
    CREATE TABLE ocel_db.iot_event_map AS
    SELECT 
        a.*, 
        b.ocel_event_id, 
        c.ocel_type, 
        d.ocel_type_map,
        'event_' || d.ocel_type_map AS event_table_name
    FROM (
        select distinct device_id, location from ocel_db.source_iot_data
    ) a
    LEFT JOIN ocel_db.event_object b ON a.location = b.ocel_object_id
    LEFT JOIN ocel_db.event c ON b.ocel_event_id = c.ocel_id
    LEFT JOIN ocel_db.event_map_type d ON c.ocel_type = d.ocel_type
"""

# Query to refine 'source_iot_data' based on selected object type
REFINE_SOURCE_IOT_DATA_QUERY = """
    CREATE TABLE ocel_db.source_iot_data AS 
    SELECT * FROM read_parquet('{parquet_path}') 
    WHERE device_type = ?
    AND device_id IN (
        SELECT distinct device_id FROM ocel_db.iot_object_map
        WHERE ocel_type = ?
    )
"""

# Query to check for remaining integrations
CHECK_REMAINING_INTEGRATIONS_QUERY = """
    SELECT COUNT(*) FROM (
        SELECT DISTINCT 
            p.device_type, 
            COALESCE(b.ocel_type, 'NONE') as ocel_type
        FROM read_parquet('{parquet_path}') p
        LEFT JOIN ocel_db.object b ON p.location = b.ocel_id
        -- We only care about device_type+ocel_type existence
    ) potential
    LEFT JOIN ocel_db.integration_log log 
    ON potential.device_type = log.device_type 
    AND potential.ocel_type = log.ocel_type
    WHERE log.device_type IS NULL
"""
