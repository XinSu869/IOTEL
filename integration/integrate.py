from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for, current_app
from pathlib import Path
from .db_utils import init_app, get_db

from .integration_logic import (
    get_unique_iot_types, 
    cache_iot_maps,
    refine_iot_data
)
from .integration_utils import (
    get_interaction_levels, 
    get_attribute_types,
    check_integrated_file_status,
    initialize_integrated_file
)

def register_integration_routes(app, get_paths):
    """
    Registers routes for the IoT integration module.

    Args:
        app (Flask): The Flask application instance.
        get_paths (function): Function returning a dictionary of configured paths.
    """
    # Initialize DB handling for the app
    init_app(app)

    @app.route("/integrate")
    def integrate():
        """
        Renders the main integration page.
        Checks for the existence of required files (Processed Parquet and OCEL).
        """
        from iot_processing.storage import is_ocel_uploaded

        paths = get_paths()
        processed_path = Path(paths["processed_dir"]) / "processed_IoT_data.parquet"
        
        missing = []
        if not processed_path.exists():
            missing.append("Processed IoT data is missing")
        if not is_ocel_uploaded():
            missing.append("OCEL event log is missing")
            
        if missing:
            flash(f"Cannot access Integration: {'; '.join(missing)}.", "error")
            return redirect(url_for("home"))

        iot_types = get_unique_iot_types(get_paths)
        return render_template(
            "integrate.html", 
            iot_types=iot_types,
            interaction_levels=get_interaction_levels(),
            attribute_types=get_attribute_types()
        )

    @app.route("/api/integrate/check-file", methods=['GET'])
    def check_file():
        """
        API Endpoint: Checks the status of the integrated OCEL file.
        Returns JSON with status: 'missing', 'exists', or 'exists_but_older'.
        """
        status = check_integrated_file_status(lambda: get_paths())
        return jsonify({"status": status})

    @app.route('/api/integrate/initialize-file', methods=['POST'])
    def initialize_file():
        """
        API Endpoint: Initializes the integrated OCEL file by copying the uploaded one.
        Accepts 'overwrite' boolean in JSON payload.
        """
        data = request.json or {}
        overwrite = data.get("overwrite", False)
        success, msg = initialize_integrated_file(lambda: get_paths(), overwrite=overwrite)
        if success:
             return jsonify({"message": msg})
        else:
             return jsonify({"error": msg}), 400

    @app.route('/api/integrate/process', methods=['POST'])
    def process():
        """
        API Endpoint: Placeholder for process logic if needed separate from submit.
        """
        return jsonify({"message": "Process endpoint hit (implementation pending)"})

    @app.route("/api/integrate/step1", methods=["POST"])
    def integrate_step1():
        """
        API Endpoint: Step 1 - Select IoT Type.
        Generates object and event maps for the selected IoT type.
        """
        data = request.json
        iot_type = data.get("iot_type")
        if not iot_type:
            return jsonify({"error": "IoT Type is required"}), 400
        
        try:
            # object_types now contains [{"type": "...", "status": "..."}]
            object_types = cache_iot_maps(get_paths, iot_type)
            return jsonify({"status": "success", "object_types": object_types})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/integrate/step1-select", methods=["POST"])
    def integrate_step1_select():
        """
        API Endpoint: Step 1 Select - Refines data based on selected OCEL type.
        """
        data = request.json
        iot_type = data.get("iot_type")
        ocel_type = data.get("ocel_type")
        
        if not iot_type or not ocel_type:
             return jsonify({"error": "IoT Type and OCEL Type are required"}), 400
             
        try:
             # Refine the data
             refine_iot_data(get_paths, iot_type, ocel_type)
             return jsonify({"status": "success"})
        except Exception as e:
             return jsonify({"error": str(e)}), 500

    @app.route("/api/integrate/context-options")
    def context_options():
        """
        API Endpoint: Step 4 - Fetch context options (Object Types, Event Types).
        """
        from .integration_logic import get_ocel_types
        types = get_ocel_types(get_paths)
        return jsonify(types)

    @app.route("/api/integrate/properties")
    def integrate_properties():
        """
        API Endpoint: Step 5 - Fetch available properties for the selected device/object context.
        """
        from .integration_logic import get_iot_properties
        props = get_iot_properties(get_paths)
        return jsonify(props)

    @app.route("/api/integrate/submit", methods=["POST"])
    def integrate_submit():
        """
        API Endpoint: Final Step - Execute Integration.
        Persists the processed data into the integrated OCEL file.
        """
        from .integration_logic import execute_integration
        try:
            result = execute_integration(get_paths, request.json)
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/integrate/download")
    def download_integrated():
        """
        API Endpoint: Download the final integrated OCEL file.
        Scrub intermediate integration tables before downloading.
        """
        from flask import send_file, after_this_request
        from .integration_utils import get_integrated_ocel_path
        import sqlite3
        import shutil
        import tempfile
        import os
        
        file_path = get_integrated_ocel_path(get_paths)
        if not file_path.exists():
            return "Integrated file not found", 404
            
        # Create a temporary file to serve
        fd, temp_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd) 
        
        try:
            # Copy original to temp
            shutil.copy2(file_path, temp_path)
            
            # Scrub intermediate tables
            # Tables to delete: iot_event_map, iot_object_map, processed_iot_payload, source_iot_data, integration_log
            con = sqlite3.connect(temp_path)
            tables_to_drop = [
                "iot_event_map", 
                "iot_object_map", 
                "processed_iot_payload", 
                "source_iot_data", 
                "integration_log"
            ]
            try:
                for table in tables_to_drop:
                    con.execute(f"DROP TABLE IF EXISTS {table}")
                con.commit()
            finally:
                con.close()
                
            # Schedule cleanup
            @after_this_request
            def remove_file(response):
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception as e:
                    print(f"Error removing temp file {temp_path}: {e}")
                return response

            return send_file(
                temp_path, 
                as_attachment=True, 
                download_name="IoT-enriched_OCEL.sqlite"
            )
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return f"Error preparing download: {str(e)}", 500
