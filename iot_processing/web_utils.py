"""
Web-facing helpers used by the Flask application.
"""

from __future__ import annotations

from flask import Request


def wants_json_response(flask_request: Request) -> bool:
    """
    Determines if the client prefers a JSON response over HTML.
    Checks 'X-Requested-With' header and 'Accept' header.

    Args:
        flask_request (Request): The incoming Flask request.

    Returns:
        bool: True if JSON is preferred.
    """
    if flask_request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True
    best = flask_request.accept_mimetypes.best_match(["application/json", "text/html"])
    if not best:
        return False
    if best == "application/json":
        return flask_request.accept_mimetypes[best] >= flask_request.accept_mimetypes["text/html"]
    return False
