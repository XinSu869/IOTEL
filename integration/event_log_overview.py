from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

import pm4py
from flask import flash, redirect, render_template, url_for

GetPathsFn = Callable[[], Dict[str, str]]


def _parse_counter(counter_str: str) -> Dict[str, int]:
    counter_str = counter_str.strip('Counter()')
    pairs = counter_str.split(',')
    result = {}
    for pair in pairs:
        if pair.strip():
            key, value = pair.rsplit(':', 1)
            key = key.strip().strip("'")
            value = value.strip().rstrip('}')
            value = int(value)
            result[key] = value
    return result


def parse_summary(summary_str: str) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
    # Parse the overview numbers
    events = int(re.search(r'number of events: (\d+)', summary_str, re.IGNORECASE).group(1))
    objects = int(re.search(r'number of objects: (\d+)', summary_str, re.IGNORECASE).group(1))
    activities = int(re.search(r'number of activities: (\d+)', summary_str, re.IGNORECASE).group(1))
    object_types = int(re.search(r'number of object types: (\d+)', summary_str, re.IGNORECASE).group(1))
    relationships = int(re.search(r'events-objects relationships: (\d+)', summary_str, re.IGNORECASE).group(1))

    # Parse activities occurrences
    activities_match = re.search(r'Activities occurrences: Counter\(\{(.*?)\)', summary_str)
    activities_str = activities_match.group(1)
    activities_dict = _parse_counter(activities_str)

    # Parse object types occurrences
    object_types_match = re.search(r'Object types occurrences \(number of objects\): Counter\(\{(.*?)\)', summary_str)
    object_types_str = object_types_match.group(1)
    object_types_dict = _parse_counter(object_types_str)

    # Parse unique activities per object type
    activities_per_match = re.search(r'Unique activities per object type: Counter\(\{(.*?)\)', summary_str)
    activities_per_str = activities_per_match.group(1)
    activities_per_dict = _parse_counter(activities_per_str)

    overview = {
        "events": events,
        "objects": objects,
        "activities": activities,
        "object_types": object_types,
        "relationships": relationships,
    }
    return overview, activities_dict, object_types_dict, activities_per_dict



def discover_business_process_diagram(ocel_path: Path, static_dir: Path) -> Tuple[Optional[Path], Optional[str]]:
    """
    Generate a business process diagram SVG from the OCEL file using pm4py.

    Returns the output path and an optional error message.
    """
    try:
        from pm4py.visualization.ocel.ocdfg import visualizer as ocdfg_visualizer
    except Exception as exc:  # noqa: BLE001
        return None, f"pm4py visualization module unavailable: {exc}"

    diagrams_dir = static_dir / "diagrams"
    diagrams_dir.mkdir(parents=True, exist_ok=True)
    output_path = diagrams_dir / "ocel_process.svg"

    # Reuse an existing diagram if it is newer than the OCEL file
    try:
        if output_path.exists() and output_path.stat().st_mtime >= ocel_path.stat().st_mtime:
            return output_path, None
    except OSError:
        # Fall through to regeneration if we cannot compare times
        pass

    try:
        ocel = pm4py.read_ocel2_sqlite(str(ocel_path))
        ocdfg = pm4py.discover_ocdfg(ocel)
        variant = getattr(ocdfg_visualizer.Variants, "FREQUENCY", ocdfg_visualizer.Variants.CLASSIC)
        gviz = ocdfg_visualizer.apply(
            ocdfg,
            variant=variant,
            parameters={"format": "svg"},
        )
        ocdfg_visualizer.save(gviz, str(output_path))
    except Exception as exc:  # noqa: BLE001
        return None, f"Failed to generate process diagram: {exc}"

    return output_path, None


def register_event_log_routes(app, get_paths: GetPathsFn):
    def _latest_ocel_upload() -> Optional[Path]:
        upload_dir = Path(get_paths()["uploads_dir"])
        if not upload_dir.exists():
            return None
        allowed_suffixes = {f".{ext.lower()}" for ext in app.config["ALLOWED_OCEL_EXTENSIONS"]}
        candidates = [
            path
            for path in upload_dir.iterdir()
            if path.is_file() and path.suffix.lower() in allowed_suffixes
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda path: path.stat().st_mtime)

    @app.route("/event_log_overview")
    def event_log_overview():
        ocel_path = _latest_ocel_upload()
        if not ocel_path:
            flash("No OCEL 2.0 SQLite upload found. Upload an event log on the home page first.", "error")
            return redirect(url_for("home"))

        try:
            ocel_log = pm4py.read_ocel2_sqlite(str(ocel_path))
            summary_raw = ocel_log.get_summary()
            summary_str = summary_raw if isinstance(summary_raw, str) else str(summary_raw)
            overview_stats, activities_dict, object_types_dict, activities_per_dict = parse_summary(summary_str)
        except Exception as exc:  # noqa: BLE001
            flash(f"Failed to open or summarize the OCEL log: {exc}", "error")
            return redirect(url_for("home"))

        diagram_url: Optional[str] = None
        diagram_error: Optional[str] = None
        static_dir = Path(app.static_folder or "static")
        
        diagram_path, diagram_error = discover_business_process_diagram(ocel_path, static_dir)
        if diagram_path:
            try:
                diagram_url = f"diagrams/{diagram_path.name}"
            except Exception:
                diagram_url = None

        return render_template(
            "event_log_overview.html",
            overview_stats=overview_stats,
            activities_dict=activities_dict,
            object_types_dict=object_types_dict,
            activities_per_dict=activities_per_dict,
            ocel_filename=ocel_path.name,
            diagram_url=diagram_url,
            diagram_error=diagram_error,
        )

    return app
