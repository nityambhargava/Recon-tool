"""
modules/parser.py
TXT to TSV converter.

Accepts multiple .txt files or a single .zip containing .txt files.
All files are merged into ONE output .tsv file.

Merge rules:
  - Header row is taken from the first file only.
  - Headers from all subsequent files are stripped.
  - All data rows are concatenated in the order files are processed.
"""

import io
import os
import zipfile
import pandas as pd


def convert_txt_to_tsv(files: list) -> tuple[bytes, int, list]:
    """
    files: list of (filename, file_bytes) tuples

    Returns:
        tsv_bytes : bytes of the merged .tsv file
        count     : number of files successfully merged
        errors    : list of (filename, error_message) for failed files
    """
    txt_files = _extract_txt_files(files)

    merged_frames = []
    errors        = []

    for fname, data in txt_files:
        try:
            df = _parse_txt(fname, data)
            merged_frames.append(df)
        except Exception as exc:
            errors.append((fname, str(exc)))

    if not merged_frames:
        return b"", 0, errors

    # Concatenate all frames — pandas aligns on column names automatically.
    # The first file's columns are used as the reference header.
    merged = pd.concat(merged_frames, ignore_index=True)

    tsv_bytes = merged.to_csv(sep="\t", index=False).encode("utf-8")
    return tsv_bytes, len(merged_frames), errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_txt_files(files: list) -> list:
    """
    If a single .zip is uploaded, extract .txt files from it (sorted by name).
    Otherwise return all uploaded .txt files directly.
    """
    txt_files = []

    if len(files) == 1 and files[0][0].lower().endswith(".zip"):
        fname, data = files[0]
        with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
            members = sorted(
                [m for m in zf.namelist()
                 if m.lower().endswith(".txt") and not m.endswith("/")]
            )
            for member in members:
                base = os.path.basename(member)
                txt_files.append((base, zf.read(member)))
    else:
        for fname, data in files:
            if fname.lower().endswith(".txt"):
                txt_files.append((fname, data))

    return txt_files


def _parse_txt(fname: str, data: bytes) -> pd.DataFrame:
    """
    Try to auto-detect delimiter (tab, comma, pipe, etc).
    Falls back to a single-column DataFrame if detection fails.
    """
    try:
        df = pd.read_csv(
            io.BytesIO(data),
            sep=None,
            engine="python",
            dtype=str,
        )
        if df.shape[1] > 1:
            return df
    except Exception:
        pass

    # Fallback: treat each line as a single text column
    text  = data.decode("utf-8", errors="ignore")
    lines = [ln.rstrip("\n") for ln in text.splitlines()]
    return pd.DataFrame({"text": lines})
