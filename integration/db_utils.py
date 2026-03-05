
import duckdb
from pathlib import Path
from flask import g

DB_PATH = "integration.duckdb"

def get_db():
    if 'db' not in g:
        g.db = duckdb.connect(DB_PATH)
    return g.db

def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    con = get_db()
    try:
        if args:
            cur = con.execute(query, args)
        else:
            cur = con.execute(query)
        
        # Check if the query returns rows (SELECT)
        try:
            rv = cur.fetchall()
            # Get column names
            col_names = [desc[0] for desc in cur.description] if cur.description else []
            
            # Convert to list of dicts
            results = [dict(zip(col_names, row)) for row in rv]
            
            return (results[0] if results else None) if one else results
        except (RuntimeError, duckdb.InvalidInputException):
             # For INSERT/UPDATE/DELETE/CREATE that don't return results
             return None

    except Exception as e:
        print(f"Database error: {e}")
        raise e

def init_app(app):
    app.teardown_appcontext(close_db)
