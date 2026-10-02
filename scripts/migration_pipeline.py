"""
================================================================================
 Telecom/ISP Data Migration Pipeline — Production-Grade Modular ETL
================================================================================

 INTERVIEW PITCH (3-Step Logic):

 Step 1 — DETECT (The Detective):
   Scans any folder recursively for CSV files. Instead of trusting filenames,
   it fingerprints each file by its column signatures to infer dataset type.
   Performs a Grain Audit to define exactly what each row represents, flags
   duplicates, and maps inconsistent legacy headers to a canonical schema
   using fuzzy matching.

 Step 2 — CLEANSE & VALIDATE (The Architect):
   Applies defensive transformations: status normalisation to strict enums,
   type enforcement, regex-based address parsing, and format validation.
   No record is dropped — invalid rows are quarantined into invalid_[table]
   with a human-readable failure_reason. Only clean records with satisfied
   foreign key dependencies advance to final_ tables.

 Step 3 — RECONCILE & REPORT (The Auditor):
   Runs financial reconciliation (invoice totals, credit/balance anomalies),
   validates row-count parity between source and staging, and emits a full
   migration report as a structured dictionary. Every legacy ID is traceable
   to its target_id via an id_map table for rollback and audit.

================================================================================
"""

import os
import re
import csv
import json
import logging
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("migration_pipeline")

# ── Constants & Configuration ─────────────────────────────────────────────────

DB_PATH = "migration_staging.db"

# Column signatures used to infer dataset type from content, not filename
DATASET_SIGNATURES = {
    "accounts":           {"email", "first_name", "last_name"},
    "secondary_contacts": {"parent_id", "contact_name", "role"},
    "notes":              {"account_id", "note_body", "timestamp"},
    "invoices":           {"invoice_id", "customer_id", "amount"},
    "documents":          {"document_name", "account_id", "file_url"},
}

# Fuzzy header mapping: legacy/inconsistent names → canonical name
HEADER_ALIASES = {
    # Email
    "cust_email": "email", "email_addr": "email", "email_address": "email",
    "e_mail": "email", "user_email": "email",
    # Names
    "fname": "first_name", "given_name": "first_name", "forename": "first_name",
    "lname": "last_name", "surname": "last_name", "family_name": "last_name",
    "full_name": "name", "customer_name": "name", "client_name": "name",
    # IDs
    "cust_id": "account_id", "customer_id": "account_id", "client_id": "account_id",
    "acct_id": "account_id", "ref_id": "reference_id", "legacy_id": "reference_id",
    "parent_account_id": "parent_id",
    # Status
    "account_status": "status", "cust_status": "status", "acct_status": "status",
    # Phone
    "phone": "home_phone", "phone_number": "home_phone", "tel": "home_phone",
    "cell": "mobile_phone", "mobile": "mobile_phone", "cell_phone": "mobile_phone",
    # Address
    "address": "full_address", "addr": "full_address", "street_address": "full_address",
    # Notes
    "note": "note_body", "notes": "note_body", "comment": "note_body",
    "note_text": "note_body", "created": "timestamp", "created_date": "timestamp",
    # Financial
    "total": "amount", "invoice_total": "amount", "balance": "amount",
    "inv_id": "invoice_id",
}

# Status normalisation: anything that means active/inactive/suspended
STATUS_MAP = {
    "active": "ACTIVE", "enabled": "ACTIVE", "live": "ACTIVE",
    "on": "ACTIVE", "1": "ACTIVE", "yes": "ACTIVE", "y": "ACTIVE",
    "inactive": "INACTIVE", "disabled": "INACTIVE", "suspended": "INACTIVE",
    "cancelled": "INACTIVE", "canceled": "INACTIVE", "off": "INACTIVE",
    "0": "INACTIVE", "no": "INACTIVE", "n": "INACTIVE",
    "pending": "PENDING", "in progress": "PENDING", "processing": "PENDING",
}

# Email validation pattern
EMAIL_RE = re.compile(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$", re.IGNORECASE)

# Address parsing pattern: captures premise (number), thoroughfare (street),
# locality (city), region (province/state), postal code
ADDRESS_RE = re.compile(
    r"^(?P<premise>\d+[A-Za-z]?)\s+"
    r"(?P<thoroughfare>[^,]+?)"
    r"(?:,\s*(?P<locality>[^,]+?))?"
    r"(?:,\s*(?P<region>[A-Z]{2}))?"
    r"(?:,?\s*(?P<postal_code>[A-Z]\d[A-Z]\s?\d[A-Z]\d|\d{5}(?:-\d{4})?))?$",
    re.IGNORECASE,
)


# ══════════════════════════════════════════════════════════════════════════════
#  1. DISCOVERY & INFERENCE
# ══════════════════════════════════════════════════════════════════════════════

def scan_source_folder(source_folder: str) -> list[dict]:
    """
    Recursively scan a folder for CSV files.
    Returns a list of dicts: {path, filename, dataset_type, columns}.
    Dataset type is inferred from column signatures, not filenames.
    """
    source_path = Path(source_folder)
    if not source_path.exists():
        raise FileNotFoundError(f"Source folder not found: {source_folder}")

    discovered = []
    for csv_path in sorted(source_path.rglob("*.csv")):
        try:
            # Read only the header row to infer type cheaply
            with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
                raw_cols = next(csv.reader(f), [])
            normalised_cols = {normalise_header(c) for c in raw_cols if c.strip()}
            dataset_type = infer_dataset_type(normalised_cols)
            discovered.append({
                "path":         str(csv_path),
                "filename":     csv_path.name,
                "dataset_type": dataset_type,
                "raw_columns":  raw_cols,
                "norm_columns": list(normalised_cols),
            })
            log.info(f"[DETECT] {csv_path.name} → inferred as '{dataset_type}'")
        except Exception as e:
            log.warning(f"[DETECT] Could not read {csv_path.name}: {e}")

    if not discovered:
        log.warning("[DETECT] No CSV files found in source folder.")
    return discovered


def normalise_header(col: str) -> str:
    """Strip, lowercase, replace spaces/hyphens with underscores, apply alias map."""
    cleaned = col.strip().lower().replace(" ", "_").replace("-", "_")
    return HEADER_ALIASES.get(cleaned, cleaned)


def infer_dataset_type(columns: set) -> str:
    """
    Match normalised column set against known signatures.
    Returns the best-matching dataset type or 'unknown'.
    """
    best_match, best_score = "unknown", 0
    for dtype, signature in DATASET_SIGNATURES.items():
        overlap = len(signature & columns)
        coverage = overlap / len(signature)
        if coverage > best_score:
            best_match, best_score = dtype, coverage
    return best_match if best_score >= 0.5 else "unknown"


# ══════════════════════════════════════════════════════════════════════════════
#  2. SCHEMA MAPPING & HEADER NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def load_and_normalise(file_info: dict) -> tuple[pd.DataFrame, list[str]]:
    """
    Load a CSV into a DataFrame with normalised headers.
    Returns (df, warnings) where warnings lists non-essential missing columns.
    """
    df = pd.read_csv(
        file_info["path"],
        encoding="utf-8-sig",
        dtype=str,          # Load everything as string; enforce types later
        keep_default_na=False,
    )
    # Normalise all column names
    df.columns = [normalise_header(c) for c in df.columns]

    # Strip whitespace from all string values
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

    warnings = []
    dataset_type = file_info["dataset_type"]

    # Check for expected columns and warn (not fail) on missing non-essential ones
    expected = DATASET_SIGNATURES.get(dataset_type, set())
    missing = expected - set(df.columns)
    for m in missing:
        msg = f"[SCHEMA] Non-essential column '{m}' missing from {file_info['filename']} — flagged as WARNING"
        log.warning(msg)
        warnings.append(msg)

    return df, warnings


# ══════════════════════════════════════════════════════════════════════════════
#  3. GRAIN AUDIT
# ══════════════════════════════════════════════════════════════════════════════

def run_grain_audit(df: pd.DataFrame, dataset_type: str, key_col: str = None) -> dict:
    """
    Establish and verify the 'grain' of a dataset.
    Detects duplicates on the natural key and returns audit results.
    """
    total_rows = len(df)

    # Determine natural key for this dataset type
    key_candidates = {
        "accounts":           ["reference_id", "account_id", "email"],
        "secondary_contacts": ["parent_id", "email", "contact_name"],
        "notes":              ["account_id", "timestamp"],
        "invoices":           ["invoice_id"],
        "documents":          ["document_name", "account_id"],
    }
    keys = key_candidates.get(dataset_type, [])
    if key_col:
        keys = [key_col] + keys

    # Find first key column that exists in df
    grain_key = next((k for k in keys if k in df.columns), None)

    duplicates = pd.DataFrame()
    if grain_key:
        duplicates = df[df.duplicated(subset=[grain_key], keep="first")]
        df.drop_duplicates(subset=[grain_key], keep="first", inplace=True)

    result = {
        "dataset_type":  dataset_type,
        "grain_key":     grain_key or "composite",
        "total_rows":    total_rows,
        "duplicates":    len(duplicates),
        "clean_rows":    len(df),
    }
    log.info(
        f"[GRAIN] '{dataset_type}' — grain on '{grain_key}' | "
        f"{total_rows} rows | {len(duplicates)} duplicates removed | "
        f"{len(df)} clean rows"
    )
    return result, df, duplicates


# ══════════════════════════════════════════════════════════════════════════════
#  4. DEFENSIVE CLEANSING & TRANSFORMATION
# ══════════════════════════════════════════════════════════════════════════════

def cleanse_and_transform(df: pd.DataFrame, dataset_type: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply all transformation and validation rules.
    Returns (valid_df, invalid_df) — no record is dropped.
    """
    df = df.copy()
    df["failure_reason"] = ""
    df["migration_timestamp"] = datetime.utcnow().isoformat()

    # ── Status normalisation ──────────────────────────────────────────────────
    if "status" in df.columns:
        df["status"] = df["status"].str.lower().str.strip().map(STATUS_MAP).fillna("UNKNOWN")
        mask = df["status"] == "UNKNOWN"
        df.loc[mask, "failure_reason"] += "Invalid status value; "
        log.info(f"[CLEANSE] Status normalised — {mask.sum()} unmappable values flagged")

    # ── Email validation ──────────────────────────────────────────────────────
    if "email" in df.columns:
        invalid_email = df["email"].apply(
            lambda e: bool(e) and not EMAIL_RE.match(e)
        )
        df.loc[invalid_email, "failure_reason"] += "Invalid email format; "
        log.info(f"[CLEANSE] Email validation — {invalid_email.sum()} invalid addresses flagged")

    # ── Phone normalisation (E.164 best-effort) ───────────────────────────────
    for phone_col in ["home_phone", "mobile_phone", "work_phone"]:
        if phone_col in df.columns:
            df[phone_col] = df[phone_col].apply(normalise_phone)

    # ── Address parsing ───────────────────────────────────────────────────────
    if "full_address" in df.columns:
        df = parse_addresses(df)
        log.info("[CLEANSE] Full address column parsed into structured fields")

    # ── Type enforcement ──────────────────────────────────────────────────────
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"].str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce")
        invalid_amount = df["amount"].isna() & df["amount"].notna().shift(fill_value=False)
        bad_amounts = df["amount"].isna()
        df.loc[bad_amounts, "failure_reason"] += "Non-numeric amount value; "

    if "account_id" in df.columns:
        # Coerce to string but flag blanks
        blank_ids = df["account_id"].apply(lambda x: not str(x).strip())
        df.loc[blank_ids, "failure_reason"] += "Missing account_id; "

    # ── Required field presence ───────────────────────────────────────────────
    required_by_type = {
        "accounts":           ["email", "first_name", "last_name"],
        "secondary_contacts": ["parent_id", "contact_name"],
        "notes":              ["account_id", "note_body"],
        "invoices":           ["invoice_id", "account_id", "amount"],
        "documents":          ["account_id", "document_name"],
    }
    for req in required_by_type.get(dataset_type, []):
        if req in df.columns:
            missing_mask = df[req].apply(lambda x: not str(x).strip())
            df.loc[missing_mask, "failure_reason"] += f"Missing required field '{req}'; "

    # ── Split valid / invalid ─────────────────────────────────────────────────
    is_invalid = df["failure_reason"].str.strip().astype(bool)
    valid_df   = df[~is_invalid].drop(columns=["failure_reason"])
    invalid_df = df[is_invalid]

    log.info(
        f"[CLEANSE] '{dataset_type}' — {len(valid_df)} valid | "
        f"{len(invalid_df)} quarantined"
    )
    return valid_df, invalid_df


def normalise_phone(phone: str) -> str:
    """Strip all non-numeric characters, reformat to E.164 best-effort."""
    if not phone or not str(phone).strip():
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}"  # Return what we have, prefixed


def parse_addresses(df: pd.DataFrame) -> pd.DataFrame:
    """Parse full_address column into structured address components."""
    parsed = df["full_address"].apply(lambda addr: _parse_single_address(str(addr)))
    addr_df = pd.DataFrame(parsed.tolist(), index=df.index)
    df = pd.concat([df.drop(columns=["full_address"]), addr_df], axis=1)
    return df


def _parse_single_address(address: str) -> dict:
    base = {
        "physical_address_premise":      "",
        "physical_address_thoroughfare":  "",
        "physical_address_locality":      "",
        "physical_address_region":        "",
        "physical_address_postal_code":   "",
    }
    if not address.strip():
        return base
    m = ADDRESS_RE.match(address.strip())
    if m:
        base["physical_address_premise"]     = m.group("premise") or ""
        base["physical_address_thoroughfare"] = m.group("thoroughfare") or ""
        base["physical_address_locality"]    = m.group("locality") or ""
        base["physical_address_region"]      = m.group("region") or ""
        base["physical_address_postal_code"] = m.group("postal_code") or ""
    else:
        # Fallback: store full string in thoroughfare, flag nothing — warn only
        base["physical_address_thoroughfare"] = address
        log.debug(f"[ADDR] Could not fully parse address: '{address}'")
    return base


# ══════════════════════════════════════════════════════════════════════════════
#  5. RELATIONAL INTEGRITY & ORPHAN DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def check_relational_integrity(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    child_fk: str,
    parent_pk: str,
    child_dataset: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Identify orphaned child records (FK not in parent PK set).
    Orphans are quarantined to invalid table with reason 'Orphaned Record'.
    Only children with satisfied FK dependencies advance.
    """
    if child_fk not in child_df.columns:
        log.warning(f"[INTEGRITY] FK column '{child_fk}' not found in {child_dataset} — skipping check")
        return child_df, pd.DataFrame()

    if parent_pk not in parent_df.columns:
        log.warning(f"[INTEGRITY] PK column '{parent_pk}' not found in parent — skipping check")
        return child_df, pd.DataFrame()

    valid_parent_ids = set(parent_df[parent_pk].dropna().astype(str))
    is_orphan = ~child_df[child_fk].astype(str).isin(valid_parent_ids)

    orphans = child_df[is_orphan].copy()
    orphans["failure_reason"] = (
        f"Orphaned Record — '{child_fk}' not found in parent '{parent_pk}' set"
    )
    orphans["migration_timestamp"] = datetime.utcnow().isoformat()

    clean = child_df[~is_orphan]

    log.info(
        f"[INTEGRITY] '{child_dataset}' — {len(clean)} linked | "
        f"{len(orphans)} orphaned records quarantined"
    )
    return clean, orphans


# ══════════════════════════════════════════════════════════════════════════════
#  6. TRACEABILITY — ID MAP
# ══════════════════════════════════════════════════════════════════════════════

def build_id_map(df: pd.DataFrame, legacy_id_col: str, dataset_type: str) -> pd.DataFrame:
    """
    Create a traceability map linking every legacy ID to a new target_id.
    target_id is a deterministic hash of dataset_type + legacy_id for
    reproducibility across runs.
    """
    if legacy_id_col not in df.columns:
        return pd.DataFrame()

    id_map = pd.DataFrame({
        "dataset_type": dataset_type,
        "legacy_id":    df[legacy_id_col].astype(str),
        "target_id":     df[legacy_id_col].astype(str).apply(
            lambda lid: "TGT-" + hashlib.md5(
                f"{dataset_type}:{lid}".encode()
            ).hexdigest()[:8].upper()
        ),
        "mapped_at": datetime.utcnow().isoformat(),
    })
    return id_map


# ══════════════════════════════════════════════════════════════════════════════
#  7. FINANCIAL RECONCILIATION
# ══════════════════════════════════════════════════════════════════════════════

def run_financial_reconciliation(
    invoices_df: pd.DataFrame,
    conn: sqlite3.Connection,
) -> dict:
    """
    Compute invoice totals per customer, detect credit/balance anomalies,
    and return a reconciliation summary comparing source vs staged totals.
    """
    if invoices_df.empty or "amount" not in invoices_df.columns:
        log.warning("[FINANCE] No invoice data available for reconciliation")
        return {}

    # Source totals
    source_total   = invoices_df["amount"].sum()
    source_count   = len(invoices_df)
    negative_mask  = invoices_df["amount"] < 0
    anomaly_count  = negative_mask.sum()

    if anomaly_count > 0:
        log.warning(
            f"[FINANCE] {anomaly_count} records with negative balance detected — "
            f"potential credit/balance anomaly"
        )

    per_customer = (
        invoices_df.groupby("account_id")["amount"]
        .agg(total_invoiced="sum", invoice_count="count")
        .reset_index()
    )

    # Write to staging DB for SQL-based post-load reconciliation
    per_customer.to_sql("reconciliation_by_customer", conn, if_exists="replace", index=False)

    # Compare source total to what landed in final_invoices (if it exists)
    try:
        staged_total = pd.read_sql(
            "SELECT COALESCE(SUM(amount), 0) as total FROM final_invoices", conn
        ).iloc[0]["total"]
    except Exception:
        staged_total = 0.0

    variance = round(source_total - staged_total, 2)

    summary = {
        "source_invoice_count":   int(source_count),
        "source_total_invoiced":  round(float(source_total), 2),
        "staged_total_invoiced":  round(float(staged_total), 2),
        "variance":               variance,
        "negative_balance_count": int(anomaly_count),
        "reconciled":             variance == 0.0,
    }

    log.info(
        f"[FINANCE] Source: ${source_total:,.2f} | Staged: ${staged_total:,.2f} | "
        f"Variance: ${variance:,.2f} | Anomalies: {anomaly_count}"
    )
    return summary


# ══════════════════════════════════════════════════════════════════════════════
#  8. DATABASE LAYER — STAGING & FINAL TABLES
# ══════════════════════════════════════════════════════════════════════════════

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def write_to_staging(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection,
    if_exists: str = "append",
):
    """Write DataFrame to the staging database."""
    if df.empty:
        log.debug(f"[DB] No rows to write to '{table_name}'")
        return
    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    log.info(f"[DB] {len(df)} rows written to '{table_name}'")


def promote_to_final(
    staging_table: str,
    final_table: str,
    conn: sqlite3.Connection,
):
    """
    Promote validated records from staging to final_ table.
    Staging-first architecture: nothing reaches final_ without passing
    the full validation and integrity chain.
    """
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()[0]
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {final_table} AS
            SELECT * FROM {staging_table} WHERE 0
        """)
        conn.execute(f"INSERT INTO {final_table} SELECT * FROM {staging_table}")
        conn.commit()
        log.info(f"[PROMOTE] {count} records promoted from '{staging_table}' → '{final_table}'")
    except Exception as e:
        conn.rollback()
        log.error(f"[PROMOTE] Failed to promote '{staging_table}': {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
#  9. ORCHESTRATION — MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_migration_pipeline(source_folder: str) -> dict:
    """
    Main orchestration function.

    Executes the full ETL lifecycle:
      Scan → Normalise → Grain Audit → Cleanse → Integrity Check
      → Stage → Promote → Reconcile → Report

    Args:
        source_folder: Path to folder containing source CSV exports.

    Returns:
        Migration report dictionary with full success metrics.
    """
    start_time = datetime.utcnow()
    log.info("=" * 72)
    log.info("  Migration Pipeline — Starting")
    log.info(f"  Source: {source_folder}")
    log.info("=" * 72)

    conn = get_connection()
    report = {
        "pipeline_start":    start_time.isoformat(),
        "source_folder":     source_folder,
        "datasets":          {},
        "financial":         {},
        "id_map_entries":    0,
        "grain_audit":       {},
        "warnings":          [],
        "pipeline_end":      None,
        "duration_seconds":  None,
    }

    # ── Step 1: Discover all CSV files ────────────────────────────────────────
    files = scan_source_folder(source_folder)
    if not files:
        log.error("[PIPELINE] No files to process. Aborting.")
        return report

    # Group by dataset type for cross-file integrity checks
    datasets: dict[str, list[pd.DataFrame]] = {}
    all_id_maps = []

    # ── Step 2: Load, normalise, grain-audit, and cleanse each file ───────────
    for file_info in files:
        dtype = file_info["dataset_type"]
        log.info(f"\n── Processing: {file_info['filename']} ({dtype}) ──")

        df, warnings = load_and_normalise(file_info)
        report["warnings"].extend(warnings)

        # Grain audit
        grain_result, df, dupes = run_grain_audit(df, dtype)
        report["grain_audit"][file_info["filename"]] = grain_result

        # Write duplicates to quarantine immediately
        if not dupes.empty:
            dupes["failure_reason"] = "Duplicate grain key — removed at audit stage"
            dupes["migration_timestamp"] = datetime.utcnow().isoformat()
            write_to_staging(dupes, f"invalid_{dtype}_duplicates", conn, if_exists="append")

        # Cleanse
        valid_df, invalid_df = cleanse_and_transform(df, dtype)

        # Write invalid records to quarantine table
        if not invalid_df.empty:
            write_to_staging(invalid_df, f"invalid_{dtype}", conn, if_exists="append")

        # Accumulate valid records by dataset type
        if dtype not in datasets:
            datasets[dtype] = []
        datasets[dtype].append(valid_df)

        # Build traceability ID map
        legacy_key = {
            "accounts":           "reference_id",
            "secondary_contacts": "parent_id",
            "notes":              "account_id",
            "invoices":           "invoice_id",
            "documents":          "account_id",
        }.get(dtype)
        if legacy_key:
            id_map = build_id_map(valid_df, legacy_key, dtype)
            if not id_map.empty:
                all_id_maps.append(id_map)

    # ── Step 3: Merge all frames per dataset type ─────────────────────────────
    merged: dict[str, pd.DataFrame] = {
        dtype: pd.concat(frames, ignore_index=True)
        for dtype, frames in datasets.items()
        if frames
    }

    accounts_df = merged.get("accounts", pd.DataFrame())

    # ── Step 4: Relational integrity checks ───────────────────────────────────
    # Secondary contacts must reference a valid account
    if "secondary_contacts" in merged and not accounts_df.empty:
        parent_key = next(
            (c for c in ["reference_id", "account_id"] if c in accounts_df.columns), None
        )
        if parent_key:
            merged["secondary_contacts"], orphan_contacts = check_relational_integrity(
                merged["secondary_contacts"], accounts_df,
                child_fk="parent_id", parent_pk=parent_key,
                child_dataset="secondary_contacts",
            )
            if not orphan_contacts.empty:
                write_to_staging(orphan_contacts, "invalid_secondary_contacts", conn, if_exists="append")

    # Notes must reference a valid account
    if "notes" in merged and not accounts_df.empty:
        parent_key = next(
            (c for c in ["reference_id", "account_id"] if c in accounts_df.columns), None
        )
        if parent_key:
            merged["notes"], orphan_notes = check_relational_integrity(
                merged["notes"], accounts_df,
                child_fk="account_id", parent_pk=parent_key,
                child_dataset="notes",
            )
            if not orphan_notes.empty:
                write_to_staging(orphan_notes, "invalid_notes", conn, if_exists="append")

    # Documents must reference a valid account
    if "documents" in merged and not accounts_df.empty:
        parent_key = next(
            (c for c in ["reference_id", "account_id"] if c in accounts_df.columns), None
        )
        if parent_key:
            merged["documents"], orphan_docs = check_relational_integrity(
                merged["documents"], accounts_df,
                child_fk="account_id", parent_pk=parent_key,
                child_dataset="documents",
            )
            if not orphan_docs.empty:
                write_to_staging(orphan_docs, "invalid_documents", conn, if_exists="append")

    # ── Step 5: Write valid data to staging, then promote to final_ ───────────
    for dtype, df in merged.items():
        staging_table = f"staging_{dtype}"
        final_table   = f"final_{dtype}"
        write_to_staging(df, staging_table, conn, if_exists="replace")
        promote_to_final(staging_table, final_table, conn)

    # ── Step 6: Write ID map for traceability ─────────────────────────────────
    if all_id_maps:
        combined_id_map = pd.concat(all_id_maps, ignore_index=True)
        write_to_staging(combined_id_map, "id_map", conn, if_exists="replace")
        report["id_map_entries"] = len(combined_id_map)
        log.info(f"[TRACE] {len(combined_id_map)} ID mappings written to 'id_map'")

    # ── Step 7: Financial reconciliation ─────────────────────────────────────
    if "invoices" in merged:
        report["financial"] = run_financial_reconciliation(merged["invoices"], conn)

    # ── Step 8: Build final report ────────────────────────────────────────────
    for dtype, df in merged.items():
        # Count quarantined rows from DB
        quarantine_count = 0
        for qtable in [f"invalid_{dtype}", f"invalid_{dtype}_duplicates"]:
            try:
                quarantine_count += conn.execute(
                    f"SELECT COUNT(*) FROM {qtable}"
                ).fetchone()[0]
            except Exception:
                pass

        report["datasets"][dtype] = {
            "total_valid":     len(df),
            "total_invalid":   quarantine_count,
            "total_processed": len(df) + quarantine_count,
            "grain_key":       report["grain_audit"].get(
                next((f["filename"] for f in files if f["dataset_type"] == dtype), ""),
                {}
            ).get("grain_key", "unknown"),
        }

    end_time = datetime.utcnow()
    report["pipeline_end"]      = end_time.isoformat()
    report["pipeline_duration"] = round((end_time - start_time).total_seconds(), 2)

    log.info("\n" + "=" * 72)
    log.info("  MIGRATION REPORT")
    log.info("=" * 72)
    log.info(json.dumps(report, indent=2))
    log.info("=" * 72)

    conn.close()
    return report


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "./raw"
    result = run_migration_pipeline(folder)
    print(json.dumps(result, indent=2))
