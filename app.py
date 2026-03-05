"""
Main Flask Application for the IoT Processing and Integration Tool.
"""

from __future__ import annotations

from pathlib import Path
import json
from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from iot_processing.storage import (
    allowed_file,
    delete_upload_artifacts,
    ensure_dirs,
    get_paths,
    reset_storage,
    upload_entries,
    is_ocel_uploaded,
)
from iot_processing.profiling import build_preview_context
from iot_processing.adjust_map import (
    apply_overrides,
    read_preview_with_overrides,
)
from iot_processing.adjust_payload import (
    extract_json_payload,
    parse_overrides_payload,
)
from iot_processing.combine import (
    all_processed,
    combine_adjusted_to_parquet,
    combine_is_running,
    current_combine_state,
    initialize_combine_state,
    list_adjusted_parquet_files,
    launch_combine_thread,
    reset_combine_state,
)
from iot_processing.preview import preview_processed, arbitrary_query_on_parquet, get_device_type_stats
from iot_processing.web_utils import wants_json_response
from integration.event_log_overview import register_event_log_routes
from integration.integrate import register_integration_routes


app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["ALLOWED_EXTENSIONS"] = {"csv"}
app.config["ALLOWED_OCEL_EXTENSIONS"] = {"db", "sqlite", "sqlite3"}
app.secret_key = "change-me-please"

ensure_dirs()
register_event_log_routes(app, get_paths)
register_integration_routes(app, get_paths)
reset_storage()


def load_allowed_names():
    """
    Loads the list of allowed (canonical) column names from config.
    """
    config_path = Path(__file__).parent / "config" / "name_list.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("canonical_names", [])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading name list: {e}")
        return []

ALLOWED_NAMES = load_allowed_names()
DTYPE_OPTIONS = ["Int64", "Float64", "Utf8", "Boolean", "Datetime"]


def _resolve_csv_path(filename: str) -> Path:
    """Resolves the absolute path for an uploaded CSV file."""
    upload_dir = Path(get_paths()["uploads_dir"])
    return upload_dir / filename


def _display_path(path: Path) -> str:
    """Return a repo-relative path for display when possible."""
    base_dir = Path(get_paths()["base_dir"]).resolve()
    try:
        return str(path.resolve().relative_to(base_dir))
    except ValueError:
        return str(path.name)


@app.context_processor
def inject_combined_state():
    """Injects global state variables into templates."""
    processed_path = Path(get_paths()["processed_dir"]) / "processed_IoT_data.parquet"
    return {
        "combined_ready": processed_path.exists(),
        "ocel_ready": is_ocel_uploaded(),
    }


@app.route("/")
def home():
    """Renders the home page."""
    return render_template("home.html")


@app.route("/upload", methods=["POST"])
def upload():
    """
    Handles file uploads (CSV for IoT data, SQLite for OCEL).
    Redirects to appropriate overview page on success.
    """
    iot_files = [file for file in request.files.getlist("files") if file.filename]
    ocel_file = request.files.get("file")

    if not iot_files and (not ocel_file or not ocel_file.filename):
        flash("No files submitted.", "error")
        return redirect(url_for("home"))

    ensure_dirs()
    upload_dir = Path(get_paths()["uploads_dir"])

    if iot_files:
        # Validate CSV files
        invalid = [
            file.filename
            for file in iot_files
            if file.filename and not allowed_file(file.filename, app.config["ALLOWED_EXTENSIONS"])
        ]
        if invalid:
            invalid_names = ", ".join(invalid)
            flash(f"Upload rejected. Only CSV files are accepted: {invalid_names}", "error")
            return redirect(url_for("home"))

        saved = 0
        for file in iot_files:
            filename = secure_filename(file.filename)
            if not filename:
                continue
            destination = upload_dir / filename
            file.save(destination)
            saved += 1

        if not saved:
            flash("No CSV files were uploaded.", "error")
            return redirect(url_for("home"))

        flash(f"Uploaded {saved} CSV file(s) successfully.", "success")
        return redirect(url_for("iot_files"))

    if ocel_file and ocel_file.filename:
        # Validate OCEL file
        if not allowed_file(ocel_file.filename, app.config["ALLOWED_OCEL_EXTENSIONS"]):
            flash("Upload rejected. Only SQLite (.db, .sqlite, .sqlite3) files are accepted for OCEL 2.0 uploads.", "error")
            return redirect(url_for("home"))

        filename = secure_filename(ocel_file.filename)
        if not filename:
            flash("Upload rejected. Invalid filename.", "error")
            return redirect(url_for("home"))

        destination = upload_dir / filename
        ocel_file.save(destination)
        flash("Uploaded OCEL 2.0 SQLite file successfully.", "success")
        return redirect(url_for("event_log_overview"))

    flash("No files submitted.", "error")
    return redirect(url_for("home"))


@app.route("/iot_files")
def iot_files():
    """Renders the IoT Files overview page."""
    entries = upload_entries()
    paths = get_paths()
    combine_ready = bool(entries) and all(entry["processed"] for entry in entries)
    if entries:
        combine_ready = combine_ready and all_processed(paths["uploads_dir"], paths["adjusted_dir"])
    return render_template("iot_files.html", files=entries, combine_enabled=combine_ready)


@app.route("/iot_files/<filename>/delete", methods=["POST"])
def delete_iot_file(filename: str):
    """Deletes an IoT file and its artifacts."""
    wants_json = wants_json_response(request)
    entries = upload_entries()
    available = {entry["name"] for entry in entries}
    if filename not in available:
        message = f"File {filename} was not found."
        if wants_json:
            return jsonify({"error": message}), 404
        flash(message, "error")
        return redirect(url_for("iot_files"))

    if combine_is_running():
        message = "Cannot delete files while a combine job is running."
        if wants_json:
            return jsonify({"error": message}), 409
        flash(message, "error")
        return redirect(url_for("iot_files"))

    paths = get_paths()
    processed_path = Path(paths["processed_dir"]) / "processed_IoT_data.parquet"

    try:
        deletion = delete_upload_artifacts(filename)
    except OSError as exc:  # noqa: BLE001
        message = f"Failed to delete {filename}: {exc}"
        if wants_json:
            return jsonify({"error": message}), 500
        flash(message, "error")
        return redirect(url_for("iot_files"))

    combined_reset = False
    combined_error = None
    if processed_path.exists():
        try:
            processed_path.unlink()
            reset_combine_state()
            combined_reset = True
        except OSError as exc:  # noqa: BLE001
            combined_error = str(exc)

    if combined_error:
        message = f"Deleted {filename}, but failed to clear combined dataset: {combined_error}"
        if wants_json:
            return jsonify({"error": message}), 500
        flash(message, "warning")
        return redirect(url_for("iot_files"))

    details = []
    if deletion.get("adjusted_deleted"):
        details.append("adjusted parquet removed")
    if deletion.get("status_removed"):
        details.append("status cache updated")
    if combined_reset:
        details.append("combined dataset cleared")

    message = f"Deleted {filename}."
    if details:
        message = f"{message} ({'; '.join(details)})"

    if wants_json:
        return jsonify(
            {
                "message": message,
                "deleted": deletion,
                "combined_reset": combined_reset,
            }
        )

    flash(message, "success")
    return redirect(url_for("iot_files"))


@app.route("/profile/<filename>")
def profile(filename: str):
    """Renders the data profile page for a specific CSV file."""
    csv_path = _resolve_csv_path(filename)
    if not csv_path.exists():
        flash(f"File {filename} was not found on disk.", "error")
        return redirect(url_for("iot_files"))

    try:
        preview = build_preview_context(str(csv_path))
    except FileNotFoundError:
        flash(f"File {filename} was not found on disk.", "error")
        return redirect(url_for("iot_files"))

    file_meta = next((entry for entry in upload_entries() if entry["name"] == filename), None)

    return render_template(
        "profile.html",
        filename=filename,
        schema=preview["schema"],
        summary=preview["summary"],
        headers=preview["headers"],
        rows=preview["rows"],
        file_meta=file_meta,
    )


@app.route("/adjust/<filename>")
def adjust(filename: str):
    """Renders the column mapping/adjustment page."""
    entries = upload_entries()
    available = [entry["name"] for entry in entries]
    if filename not in available:
        flash(f"File {filename} is not available for adjustment.", "error")
        return redirect(url_for("iot_files"))

    try:
        preview = build_preview_context(str(_resolve_csv_path(filename)))
    except FileNotFoundError:
        flash(f"File {filename} is not available on disk.", "error")
        return redirect(url_for("iot_files"))

    columns = []
    for column in preview["schema"]["columns"]:
        columns.append(
            {
                "name": column["name"],
                "proposed_dtype": column["proposed_dtype"],
                "selected_dtype": column["proposed_dtype"],
                "selected_mapping": "",
            }
        )

    return render_template(
        "adjust_map.html",
        uploads=entries,
        filename=filename,
        columns=columns,
        preview_headers=preview["headers"],
        preview_rows=[dict(row) for row in preview["summary"]["head"]],
        allowed_names=ALLOWED_NAMES,
        dtype_options=DTYPE_OPTIONS,
    )


@app.route("/adjust/<filename>/apply", methods=["POST"])
def adjust_apply(filename: str):
    """Applies column mappings and type adjustments to the file."""
    csv_path = _resolve_csv_path(filename)
    if not csv_path.exists():
        flash(f"File {filename} was not found.", "error")
        return redirect(url_for("iot_files"))

    payload = extract_json_payload(request)
    overrides = parse_overrides_payload(payload)
    mapped_targets = {value for value in overrides["name_mapping"].values() if value}
    required_targets = [name for name in ALLOWED_NAMES if name != "location"]
    missing_required = [name for name in required_targets if name not in mapped_targets]
    if missing_required:
        missing_list = ", ".join(missing_required)
        flash(
            f"Missing required mappings: {missing_list}. Please map each before applying.",
            "error",
        )
        return redirect(url_for("adjust", filename=filename))

    add_null_location = False
    if "location" not in mapped_targets:
        force_null = payload.get("force_null_location")
        add_null_location = str(force_null).lower() in {"1", "true", "yes", "on"}
        if not add_null_location:
            flash(
                "Location is not mapped. Confirm adding a null location column or map a column to location.",
                "error",
            )
            return redirect(url_for("adjust", filename=filename))

    try:
        result = apply_overrides(
            str(csv_path),
            overrides["dtype_overrides"],
            overrides["name_mapping"],
            ALLOWED_NAMES,
            add_null_location=add_null_location,
        )
    except Exception as exc:  # noqa: BLE001
        flash(f"Failed to apply overrides: {exc}", "error")
        return redirect(url_for("adjust", filename=filename))

    warnings = list(result.get("warnings") or [])
    mapped_targets = {
        value for value in overrides["name_mapping"].values() if value
    }
    missing_targets = sorted(
        target for target in mapped_targets if target not in result["final_columns"]
    )
    if missing_targets:
        missing_list = ", ".join(missing_targets)
        warnings.append(
            f"Some mapped columns were missing and have been dropped: {missing_list}."
        )

    for message in warnings:
        flash(message, "warning")

    flash(f"Data processing applied for {filename}.", "success")
    return redirect(url_for("iot_files"))


@app.route("/adjust/<filename>/preview", methods=["POST"])
def adjust_preview(filename: str):
    """API Endpoint: Previews the file with current override settings."""
    csv_path = _resolve_csv_path(filename)
    if not csv_path.exists():
        return jsonify({"error": "File not found."}), 404

    payload = extract_json_payload(request)
    overrides = parse_overrides_payload(payload)

    try:
        preview = read_preview_with_overrides(
            str(csv_path),
            overrides["dtype_overrides"],
            overrides["name_mapping"],
            ALLOWED_NAMES,
            n=payload.get("limit", 20),
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 400

    columns = list(preview[0].keys()) if preview else []
    return jsonify({"rows": preview, "columns": columns})


@app.route("/adjust/<filename>/back", methods=["POST"])
def adjust_back(filename: str):
    """Handles back navigation from adjustment page."""
    return redirect(url_for("iot_files"))


@app.route("/combine", methods=["POST"])
def combine():
    """
    Handles the combination of processed IoT files into a single Parquet dataset.
    Supports asynchronous execution via threading.
    """
    wants_json = wants_json_response(request)
    paths = get_paths()
    upload_dir = paths["uploads_dir"]
    adjusted_dir = paths["adjusted_dir"]

    if not all_processed(upload_dir, adjusted_dir):
        message = "Not all uploaded files have been processed."
        if wants_json:
            return jsonify({"state": "error", "message": message}), 400
        flash(message, "error")
        response = redirect(url_for("iot_files"))
        return response, 403

    out_path = Path(paths["processed_dir"]) / "processed_IoT_data.parquet"

    if wants_json:
        parquet_files = list_adjusted_parquet_files(adjusted_dir)
        if not parquet_files:
            return (
                jsonify(
                    {
                        "state": "error",
                        "message": "No adjusted parquet files were found. Process files before combining.",
                    }
                ),
                400,
            )
        if combine_is_running():
            payload = current_combine_state()
            return jsonify(payload), 202
        initialize_combine_state(len(parquet_files))
        launch_combine_thread(adjusted_dir, out_path)
        payload = current_combine_state()
        return jsonify(payload), 202

    try:
        result = combine_adjusted_to_parquet(adjusted_dir, str(out_path))
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(url_for("iot_files"))
    except Exception as exc:  # noqa: BLE001
        flash(f"Failed to combine parquet files: {exc}", "error")
        return redirect(url_for("files"))

    flash(f"Combined dataset created with {result['row_count']} rows.", "success")
    reset_combine_state()
    return redirect(url_for("review"))


@app.route("/combine/status")
def combine_status():
    """Returns the status of the background combine job."""
    payload = current_combine_state()
    if payload.get("state") == "completed":
        payload.setdefault("redirect_url", url_for("review"))
    return jsonify(payload)


@app.route("/review")
def review():
    """Renders the Review & Export page."""
    paths = get_paths()
    out_path = Path(paths["processed_dir"]) / "processed_IoT_data.parquet"
    if not out_path.exists():
        flash("No combined dataset found. Process files first.", "error")
        return redirect(url_for("iot_files"))

    preview_rows = preview_processed(str(out_path), limit=20)
    headers = list(preview_rows[0].keys()) if preview_rows else []
    
    device_stats = []
    try:
        device_stats = get_device_type_stats(str(out_path))
    except Exception as e:
        print(f"Error fetching stats: {e}")

    return render_template(
        "review_export.html",
        headers=headers,
        rows=preview_rows,
        output_path=_display_path(out_path),
        device_stats=device_stats,
    )


@app.route("/export")
def export():
    """Initiates download of the processed parquet dataset."""
    out_path = Path(get_paths()["processed_dir"]) / "processed_IoT_data.parquet"
    if not out_path.exists():
        return (
            "Processed dataset not found. Combine files before exporting.",
            404,
        )
    return send_file(out_path, as_attachment=True, download_name="processed_IoT_data.parquet")


@app.route("/api/query", methods=["POST"])
def api_query():
    """Executes arbitrary SQL queries on the processed dataset (for preview/debug)."""
    payload = extract_json_payload(request)
    out_path = payload.get("path") or str(Path(get_paths()["processed_dir"]) / "processed_IoT_data.parquet")
    sql = payload.get("sql")
    if not sql:
        return jsonify({"error": "SQL query is required."}), 400

    try:
        result = arbitrary_query_on_parquet(out_path, sql, limit=payload.get("limit", 1000))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 400
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
