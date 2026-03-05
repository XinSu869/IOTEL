"""
Helpers for parsing adjustment and mapping payloads from Flask requests.
Handles extraction of JSON/Form data and coercion of mapping dictionaries.
"""

from __future__ import annotations

import json
from typing import Dict

from flask import Request


def extract_json_payload(flask_request: Request) -> Dict[str, object]:
    """
    Extracts JSON or form payload data from a Flask request.
    
    Args:
        flask_request (Request): The incoming Flask request object.

    Returns:
        Dict[str, object]: A dictionary of payload data.
    """
    data = flask_request.get_json(silent=True)
    if data is not None:
        return data
    if flask_request.form:
        return flask_request.form.to_dict(flat=True)
    return {}


def _coerce_mapping(value: object) -> Dict[str, str]:
    """
    Normalizes mapping payloads from JSON strings or dictionaries.

    Args:
        value (object): The input value (dict or json string).

    Returns:
        Dict[str, str]: A clean dictionary of strings.
    """
    if isinstance(value, dict):
        return {str(key): str(val) for key, val in value.items() if val not in (None, "")}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(key): str(val) for key, val in parsed.items() if val not in (None, "")}
    return {}


def _coerce_form_names(data: Dict[str, object], prefix: str) -> Dict[str, str]:
    """
    Extracts mapping-style data from flat form fields like 'types[column]'.

    Args:
        data (Dict[str, object]): Flat form data.
        prefix (str): Prefix to look for (e.g., 'types' or 'mapping').

    Returns:
        Dict[str, str]: Extracted dictionary.
    """
    result: Dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(value, (str, int, float, bool)):
            continue
        if key.startswith(f"{prefix}[") and key.endswith("]"):
            column = key[len(prefix) + 1 : -1]
            str_value = str(value)
            if str_value and str_value != "(leave as-is)":
                result[column] = str_value
    return result


def parse_overrides_payload(data: Dict[str, object]) -> Dict[str, Dict[str, str]]:
    """
    Normalizes dtype and name mapping overrides from incoming payloads.
    Handles both JSON structure and form-encoded structure.

    Args:
        data (Dict[str, object]): Raw payload data.

    Returns:
        Dict[str, Dict[str, str]]: A dict containing 'dtype_overrides' and 'name_mapping'.
    """
    dtype_overrides = _coerce_mapping(data.get("dtype_overrides"))
    name_mapping = _coerce_mapping(data.get("name_mapping"))

    if not dtype_overrides:
        dtype_overrides = _coerce_mapping(data.get("types"))
    if not name_mapping:
        name_mapping = _coerce_mapping(data.get("mapping"))

    if not dtype_overrides:
        dtype_overrides = _coerce_form_names(data, "types")
    if not name_mapping:
        name_mapping = _coerce_form_names(data, "mapping")

    # Remove "leave as-is" placeholder values often sent by UI selectors
    name_mapping = {k: v for k, v in name_mapping.items() if v != "(leave as-is)"}

    return {"dtype_overrides": dtype_overrides, "name_mapping": name_mapping}
