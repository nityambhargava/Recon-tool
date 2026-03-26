"""
app.py  -  ReconTool Flask application
In-memory file processing. No files saved to disk permanently.

Local run:
    python app.py  ->  http://127.0.0.1:5000

Render deploy:
    Build:  pip install -r requirements.txt
    Start:  gunicorn app:app
"""

import io
import json
import uuid
from dataclasses import asdict

from flask import (Flask, render_template, request,
                   redirect, url_for, session, send_file)

import pandas as pd
from ingestion.loader import load_from_bytes, get_date_range
from modules.engine import compute_dashboard, build_actionables, CHANNELS
from modules.parser import convert_txt_to_tsv
from modules.return_logic import (
    DEFAULT_CLAIM_WINDOWS,
    process_report,
    putaway_to_df,
    not_deliv_to_df,
)

app = Flask(__name__)
app.secret_key = "recon-secret-change-in-prod"

# Server-side store for large binary results (zip downloads)
_ZIP_STORE: dict = {}

ALLOWED_RECON  = {".csv", ".xlsx", ".xls"}
ALLOWED_PARSER = {".txt", ".zip"}


def _ext(filename):
    from pathlib import Path
    return Path(filename).suffix.lower()


# ---------------------------------------------------------------------------
# Dashboard — Reconciliation
# ---------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    error = session.pop("upload_error", None)
    return render_template("index.html", error=error)


@app.route("/upload", methods=["POST"])
def upload():
    f = request.files.get("file")
    if not f or not f.filename:
        session["upload_error"] = "No file selected."
        return redirect(url_for("index"))
    if _ext(f.filename) not in ALLOWED_RECON:
        session["upload_error"] = "Unsupported file type. Please upload CSV or Excel."
        return redirect(url_for("index"))
    try:
        df   = load_from_bytes(f.read(), f.filename)
        data = compute_dashboard(df, get_date_range(df))
        session["dashboard_data"] = data
        session["filename"]       = f.filename
    except Exception as exc:
        session["upload_error"] = str(exc)
        return redirect(url_for("index"))
    return redirect(url_for("dashboard"))


@app.route("/dashboard", methods=["GET"])
def dashboard():
    data     = session.get("dashboard_data")
    filename = session.get("filename")
    if not data:
        return redirect(url_for("index"))
    active_channel = request.args.get("channel", CHANNELS[0])
    if active_channel not in data["channels"]:
        active_channel = CHANNELS[0]
    channel_data = data["channels"][active_channel]
    actionables  = (build_actionables(channel_data["overall"], active_channel)
                    if channel_data["totalOrders"] > 0 else [])
    return render_template(
        "dashboard.html",
        data=data,
        active_channel=active_channel,
        actionables=actionables,
        filename=filename,
    )


# ---------------------------------------------------------------------------
# Dashboard — Return TAT
# ---------------------------------------------------------------------------

@app.route("/return-dashboard", methods=["GET"])
def return_dashboard():
    return render_template("return_dashboard.html")


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

@app.route("/parser", methods=["GET"])
def parser():
    error  = session.pop("parser_error", None)
    result = session.pop("parser_result", None)
    return render_template("parser.html", error=error, result=result)


@app.route("/parser/convert", methods=["POST"])
def parser_convert():
    uploaded = request.files.getlist("files")
    if not uploaded or all(f.filename == "" for f in uploaded):
        session["parser_error"] = "No files selected."
        return redirect(url_for("parser"))
    invalid = [f.filename for f in uploaded
               if f.filename and _ext(f.filename) not in ALLOWED_PARSER]
    if invalid:
        session["parser_error"] = (
            f"Only .txt and .zip files are supported. "
            f"Invalid: {', '.join(invalid)}"
        )
        return redirect(url_for("parser"))
    files_data = [(f.filename, f.read()) for f in uploaded if f.filename]
    try:
        tsv_bytes, count, errors = convert_txt_to_tsv(files_data)
        if count == 0:
            session["parser_error"] = "No .txt files found to convert."
            return redirect(url_for("parser"))
        tsv_key = str(uuid.uuid4())
        _ZIP_STORE[tsv_key] = tsv_bytes
        session["parser_tsv_key"] = tsv_key
        session["parser_result"]  = {"count": count, "errors": errors}
    except Exception as exc:
        session["parser_error"] = str(exc)
    return redirect(url_for("parser"))


@app.route("/parser/download", methods=["GET"])
def parser_download():
    tsv_key   = session.get("parser_tsv_key")
    tsv_bytes = _ZIP_STORE.get(tsv_key) if tsv_key else None
    if not tsv_bytes:
        return redirect(url_for("parser"))
    return send_file(
        io.BytesIO(tsv_bytes),
        mimetype="text/tab-separated-values",
        as_attachment=True,
        download_name="merged_output.tsv",
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n  ReconTool")
    print("  ────────────────────────────")
    print("  http://127.0.0.1:5000")
    print("  Ctrl+C to stop\n")
    app.run(debug=True, port=5000)
