"""
================================================================================
 gaiia Live Session — EDA Script v2
 Rebuilt to be ACTIONABLE, RELATIONAL, and PERSISTENT

 Run: python eda.py <folder/>          ← profiles all CSVs + cross-file checks
      python eda.py <file.csv>         ← profiles a single file

 Output: terminal + eda_audit_<timestamp>.json
================================================================================
"""

import os
import re
import sys
import json
import logging
import hashlib
import warnings
import pandas as pd
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("eda")

# ── Constants ─────────────────────────────────────────────────────────────────

EMAIL_RE = re.compile(r"^[\w\.\+\-]+@[\w\-]+\.[a-z]{2,}$", re.IGNORECASE)
PHONE_RE = re.compile(r"^[\d\s\+\-\(\)\.]{7,15}$")

# Fuzzy header aliases → canonical names
HEADER_ALIASES = {
    "cust_email": "email", "email_addr": "email", "email_address": "email",
    "e_mail": "email", "user_email": "email",
    "fname": "first_name", "given_name": "first_name", "forename": "first_name",
    "lname": "last_name", "surname": "last_name", "family_name": "last_name",
    "full_name": "name", "customer_name": "name",
    "cust_id": "account_id", "customer_id": "account_id", "client_id": "account_id",
    "acct_id": "account_id", "ref_id": "reference_id", "legacy_id": "reference_id",
    "parent_account_id": "parent_id",
    "phone": "home_phone", "phone_number": "home_phone", "tel": "home_phone",
    "cell": "mobile_phone", "mobile": "mobile_phone",
    "address": "full_address", "addr": "full_address",
    "note": "note_body", "notes": "note_body", "comment": "note_body",
    "created": "timestamp", "created_date": "timestamp", "created_at": "timestamp",
    "total": "amount", "invoice_total": "amount", "balance": "amount",
}

# Column signature → dataset type inference
DATASET_SIGNATURES = {
    "accounts":           {"email", "first_name", "last_name"},
    "secondary_contacts": {"parent_id", "contact_name", "role"},
    "notes":              {"account_id", "note_body", "timestamp"},
    "invoices":           {"invoice_id", "account_id", "amount"},
    "documents":          {"document_name", "account_id"},
}

# Semantic role hints for display + targeted checks
ROLE_HINTS = {
    "email":     ("📧 email",     ["email", "mail"]),
    "id":        ("🔑 identifier", ["id", "ref", "reference", "key"]),
    "date":      ("📅 date/time",  ["date", "created", "timestamp", "activated"]),
    "phone":     ("📞 phone",      ["phone", "mobile", "tel", "cell"]),
    "status":    ("🚦 status",     ["status"]),
    "address":   ("📍 address",    ["address", "street", "thoroughfare"]),
    "financial": ("💰 financial",  ["amount", "balance", "invoice", "total", "revenue"]),
    "name":      ("👤 name",       ["name"]),
    "note":      ("📝 text/note",  ["note", "comment", "description", "body"]),
}

CHUNK_SIZE = 50_000   # rows per chunk for large file profiling
SEP_WIDTH  = 72


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def sep(title=""):
    if title:
        pad = max(0, (SEP_WIDTH - len(title) - 2)) // 2
        print(f"\n{'─' * pad} {title} {'─' * (SEP_WIDTH - pad - len(title) - 2)}")
    else:
        print(f"\n{'─' * SEP_WIDTH}")


def normalise_header(col: str) -> str:
    cleaned = col.strip().lower().replace(" ", "_").replace("-", "_")
    return HEADER_ALIASES.get(cleaned, cleaned)


def infer_role(col: str) -> str:
    c = col.lower()
    for role, (label, keywords) in ROLE_HINTS.items():
        if any(k in c for k in keywords):
            return label
    return "   field"


def infer_dataset_type(columns: set) -> str:
    best, best_score = "unknown", 0
    for dtype, sig in DATASET_SIGNATURES.items():
        score = len(sig & columns) / len(sig)
        if score > best_score:
            best, best_score = dtype, score
    return best if best_score >= 0.5 else "unknown"


def file_fingerprint(path: str) -> str:
    """MD5 of first 8KB — fast identity check without reading full file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        h.update(f.read(8192))
    return h.hexdigest()[:12]


# ══════════════════════════════════════════════════════════════════════════════
#  LOAD — chunked for large files
# ══════════════════════════════════════════════════════════════════════════════

def load_csv(filepath: str) -> pd.DataFrame:
    """
    Load CSV with chunking for large files.
    Falls back to full load if file is small.
    Handles encoding issues gracefully.
    """
    path = Path(filepath)
    size_mb = path.stat().st_size / (1024 * 1024)

    try:
        if size_mb > 50:
            log.info(f"[LOAD] Large file ({size_mb:.1f} MB) — reading in chunks")
            chunks = pd.read_csv(
                filepath, dtype=str, keep_default_na=False,
                encoding="utf-8-sig", chunksize=CHUNK_SIZE,
            )
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_csv(
                filepath, dtype=str, keep_default_na=False,
                encoding="utf-8-sig",
            )
    except UnicodeDecodeError:
        log.warning(f"[LOAD] UTF-8 failed for {path.name} — retrying with latin-1")
        df = pd.read_csv(
            filepath, dtype=str, keep_default_na=False, encoding="latin-1"
        )

    # Normalise headers
    original = list(df.columns)
    df.columns = [normalise_header(c) for c in df.columns]
    renamed = {o: n for o, n in zip(original, df.columns) if o != n}

    # Strip whitespace from all values
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

    return df, renamed, size_mb


# ══════════════════════════════════════════════════════════════════════════════
#  DECISION LAYER — the key missing piece
# ══════════════════════════════════════════════════════════════════════════════

def make_decision(row_flags: dict) -> tuple[str, str]:
    """
    Given a dict of flags for a field, return:
      - decision:  VALID | QUARANTINE | REVIEW
      - action:    what to do with this record in the migration

    Decision hierarchy:
      QUARANTINE → structural failure, cannot load without intervention
      REVIEW     → data quality concern, can load but needs attention
      VALID      → clean, ready to load
    """
    if row_flags.get("missing_required"):
        return "QUARANTINE", "Missing required field — must be resolved before load"
    if row_flags.get("invalid_email"):
        return "QUARANTINE", "Invalid email format — move to invalid_accounts table"
    if row_flags.get("orphaned"):
        return "QUARANTINE", "Orphaned record — FK has no parent, flag for client review"
    if row_flags.get("duplicate"):
        return "QUARANTINE", "Duplicate on grain key — keep first occurrence, quarantine rest"
    if row_flags.get("invalid_phone"):
        return "REVIEW", "Non-standard phone format — attempt E.164 normalisation"
    if row_flags.get("bad_date"):
        return "REVIEW", "Unparseable date — default to NULL, flag for client confirmation"
    if row_flags.get("unknown_status"):
        return "REVIEW", "Unmapped status value — map to PENDING, confirm with client"
    if row_flags.get("negative_amount"):
        return "REVIEW", "Negative financial value — potential credit, confirm intent"
    if row_flags.get("missing_optional"):
        return "REVIEW", "Missing optional field — load with NULL, log warning"
    return "VALID", "Ready to load"


def apply_decisions(df: pd.DataFrame, issues: dict) -> pd.DataFrame:
    """
    Tag every row with a DECISION and ACTION based on detected issues.
    Returns the df with two new columns appended.
    """
    decisions = []
    actions   = []

    for i in range(len(df)):
        row_flags = {k: (i in v) for k, v in issues.items() if isinstance(v, set)}
        decision, action = make_decision(row_flags)
        decisions.append(decision)
        actions.append(action)

    df = df.copy()
    df["_decision"] = decisions
    df["_action"]   = actions
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  COLUMN PROFILING
# ══════════════════════════════════════════════════════════════════════════════

def profile_column(series: pd.Series, col: str) -> dict:
    """
    Profile a single column. Returns structured results and issue index sets.
    Using index sets (not row copies) avoids memory duplication on large files.
    """
    total   = len(series)
    empty   = series.apply(lambda x: not str(x).strip())
    role    = infer_role(col)
    issues  = {}
    notes   = []

    null_count  = empty.sum()
    null_pct    = null_count / total * 100 if total else 0
    unique_ct   = series[~empty].nunique()
    top_vals    = series[~empty].value_counts().head(5).to_dict()

    # ── Targeted checks by role ──
    if "email" in role:
        bad_idx = set(series.index[
            ~empty & ~series.apply(lambda x: bool(EMAIL_RE.match(str(x))))
        ])
        if bad_idx:
            sample = series[list(bad_idx)[:1]].values[0]
            notes.append(f"⚠️  {len(bad_idx)} invalid email(s) — e.g. '{sample}'")
            issues["invalid_email"] = bad_idx

    if "phone" in role:
        bad_idx = set(series.index[
            ~empty & ~series.apply(lambda x: bool(PHONE_RE.match(str(x))))
        ])
        if bad_idx:
            sample = series[list(bad_idx)[:1]].values[0]
            notes.append(f"⚠️  {len(bad_idx)} non-standard phone(s) — e.g. '{sample}'")
            issues["invalid_phone"] = bad_idx

    if "status" in role:
        known = {"active","inactive","suspended","pending","enabled","disabled","1","0"}
        unmapped = set(series[~empty].str.lower().unique()) - known
        notes.append(f"ℹ️  Values: {sorted(series[~empty].str.lower().unique().tolist())}")
        if unmapped:
            notes.append(f"⚠️  Unknown status values: {unmapped} → will map to PENDING")
            issues["unknown_status"] = set(series.index[
                ~empty & ~series.str.lower().isin(known)
            ])

    if "financial" in role:
        numeric = pd.to_numeric(
            series.str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce"
        )
        neg_idx = set(numeric.index[numeric < 0])
        nan_idx = set(numeric.index[numeric.isna() & ~empty])
        if neg_idx:
            notes.append(f"⚠️  {len(neg_idx)} negative value(s) — credit/balance anomaly")
            issues["negative_amount"] = neg_idx
        if nan_idx:
            notes.append(f"⚠️  {len(nan_idx)} non-numeric value(s)")
            issues["non_numeric"] = nan_idx

    if "date" in role:
        parsed  = pd.to_datetime(series, errors="coerce")
        bad_idx = set(parsed.index[parsed.isna() & ~empty])
        if bad_idx:
            notes.append(f"⚠️  {len(bad_idx)} unparseable date(s) → will default to NULL")
            issues["bad_date"] = bad_idx

    return {
        "null_count":  int(null_count),
        "null_pct":    round(null_pct, 1),
        "unique":      int(unique_ct),
        "top_values":  top_vals,
        "role":        role,
        "notes":       notes,
        "issues":      {k: list(v) for k, v in issues.items()},  # serialisable
    }, issues


# ══════════════════════════════════════════════════════════════════════════════
#  DUPLICATE & GRAIN AUDIT
# ══════════════════════════════════════════════════════════════════════════════

def grain_audit(df: pd.DataFrame, dataset_type: str) -> tuple[dict, set]:
    """
    Identify natural key and find duplicates.
    Returns audit result and set of duplicate row indices.
    """
    key_candidates = {
        "accounts":           ["reference_id", "account_id", "email"],
        "secondary_contacts": ["contact_id", "email"],
        "notes":              ["note_id"],
        "invoices":           ["invoice_id"],
        "documents":          ["doc_id"],
    }
    candidates = key_candidates.get(dataset_type, [])
    grain_key  = next((k for k in candidates if k in df.columns), None)

    dupe_idx = set()
    if grain_key:
        dupe_mask = df.duplicated(subset=[grain_key], keep="first")
        dupe_idx  = set(df.index[dupe_mask])

    return {
        "grain_key":  grain_key or "none detected",
        "duplicates": len(dupe_idx),
    }, dupe_idx


# ══════════════════════════════════════════════════════════════════════════════
#  RELATIONAL INTEGRITY — cross-file orphan detection
# ══════════════════════════════════════════════════════════════════════════════

def check_orphans(
    child_df:   pd.DataFrame,
    parent_df:  pd.DataFrame,
    child_fk:   str,
    parent_pk:  str,
    child_name: str,
) -> dict:
    """
    Cross-file FK → PK check. Returns structured result with orphan indices.
    Does NOT require both files to be loaded in full — uses value sets only.
    """
    if child_fk not in child_df.columns:
        return {"error": f"FK column '{child_fk}' not in {child_name}"}
    if parent_pk not in parent_df.columns:
        return {"error": f"PK column '{parent_pk}' not in parent"}

    parent_ids = set(parent_df[parent_pk].dropna().str.strip())
    child_vals = child_df[child_fk].astype(str).str.strip()
    orphan_idx = set(child_df.index[~child_vals.isin(parent_ids)])

    return {
        "child_dataset":  child_name,
        "child_fk":       child_fk,
        "parent_pk":      parent_pk,
        "total_child":    len(child_df),
        "orphan_count":   len(orphan_idx),
        "orphan_pct":     round(len(orphan_idx) / len(child_df) * 100, 1) if child_df.size else 0,
        "sample_orphans": list(child_df.loc[list(orphan_idx)[:3], child_fk]) if orphan_idx else [],
        "decision":       "QUARANTINE" if orphan_idx else "VALID",
        "action":         "Orphaned records → invalid_table. Raise with client before load."
                          if orphan_idx else "All child records have a valid parent ✅",
        "orphan_indices": list(orphan_idx),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  FINANCIAL RECONCILIATION
# ══════════════════════════════════════════════════════════════════════════════

def financial_reconciliation(df: pd.DataFrame) -> dict:
    if "amount" not in df.columns:
        return {}

    numeric = pd.to_numeric(
        df["amount"].str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce"
    )
    return {
        "total_source":      round(float(numeric.sum()), 2),
        "record_count":      len(df),
        "negative_count":    int((numeric < 0).sum()),
        "non_numeric_count": int(numeric.isna().sum()),
        "max_amount":        round(float(numeric.max()), 2) if not numeric.isna().all() else None,
        "min_amount":        round(float(numeric.min()), 2) if not numeric.isna().all() else None,
        "note": (
            "⚠️  Negative balances present — confirm credit intent with client"
            if (numeric < 0).any() else "✅ No anomalies"
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  SINGLE FILE EDA
# ══════════════════════════════════════════════════════════════════════════════

def run_eda(filepath: str) -> dict:
    path = Path(filepath)
    if not path.exists():
        print(f"❌  File not found: {filepath}")
        return {}

    print(f"\n{'═' * SEP_WIDTH}")
    print(f"  FILE : {path.name}")
    print(f"{'═' * SEP_WIDTH}")

    df, renamed, size_mb = load_csv(filepath)
    dataset_type = infer_dataset_type(set(df.columns))

    # ── 1. Grain ──────────────────────────────────────────────────────────────
    sep("1 · GRAIN & DATASET TYPE")
    print(f"  Inferred type : {dataset_type}")
    print(f"  Rows          : {len(df):,}")
    print(f"  Columns       : {len(df.columns)}")
    print(f"  File size     : {size_mb:.2f} MB")
    if renamed:
        print(f"  Headers normalised: {renamed}")

    grain_result, dupe_idx = grain_audit(df, dataset_type)
    print(f"  Grain key     : '{grain_result['grain_key']}'")
    print(f"  Duplicates    : {grain_result['duplicates']}", end="")
    print(" ⚠️" if grain_result["duplicates"] else " ✅")

    # ── 2. Column Inventory ───────────────────────────────────────────────────
    sep("2 · COLUMN INVENTORY")
    print(f"  {'COLUMN':<30} {'ROLE':<18} {'NULL%':>6}  {'UNIQUE':>7}  {'FLAG'}")
    print(f"  {'──────':<30} {'────':<18} {'─────':>6}  {'──────':>7}  {'────'}")

    all_issues = {"duplicate": dupe_idx}
    col_profiles = {}

    for col in df.columns:
        profile, col_issues = profile_column(df[col].astype(str), col)
        col_profiles[col]   = profile
        for k, v in col_issues.items():
            all_issues.setdefault(k, set()).update(v)

        flag = ""
        if profile["null_pct"] > 30: flag = "🔴 HIGH NULL"
        elif profile["null_pct"] > 10: flag = "🟡 MOD NULL"
        if col_issues: flag += (" " if flag else "") + "⚠️"

        print(f"  {col:<30} {profile['role']:<18} {profile['null_pct']:>5.1f}%  {profile['unique']:>7}  {flag}")

    # ── 3. Format & Quality Issues ────────────────────────────────────────────
    sep("3 · FORMAT & QUALITY ISSUES")
    any_issues = False
    for col, profile in col_profiles.items():
        if profile["notes"]:
            any_issues = True
            print(f"\n  [{col}]")
            for note in profile["notes"]:
                print(f"    {note}")
    if not any_issues:
        print("  ✅  No format issues detected")

    # ── 4. Decision Layer ─────────────────────────────────────────────────────
    sep("4 · MIGRATION DECISIONS")
    df_decided  = apply_decisions(df, all_issues)
    decision_vc = df_decided["_decision"].value_counts()

    valid_n      = decision_vc.get("VALID",      0)
    review_n     = decision_vc.get("REVIEW",     0)
    quarantine_n = decision_vc.get("QUARANTINE", 0)

    print(f"  ✅  VALID      : {valid_n:>4}  → Ready to load to final_{dataset_type}")
    print(f"  🟡  REVIEW     : {review_n:>4}  → Load with warnings, flag for client sign-off")
    print(f"  🔴  QUARANTINE : {quarantine_n:>4}  → Move to invalid_{dataset_type}, do not load")

    if quarantine_n:
        sample_q = df_decided[df_decided["_decision"] == "QUARANTINE"][["_action"]].head(3)
        print(f"\n  Sample quarantine reasons:")
        for _, row in sample_q.iterrows():
            print(f"    • {row['_action']}")

    # ── 5. FK Candidates ─────────────────────────────────────────────────────
    sep("5 · REFERENTIAL INTEGRITY HINTS")
    fk_cols = [c for c in df.columns if "account_id" in c or "parent_id" in c]
    if fk_cols:
        for fk in fk_cols:
            print(f"  FK candidate '{fk}': {df[fk].nunique():,} unique values")
            print(f"  → Run cross-file orphan check against parent accounts file")
    else:
        print("  No FK columns detected — this may be the parent/accounts file")

    # ── 6. Financial Reconciliation ───────────────────────────────────────────
    if "amount" in df.columns:
        sep("6 · FINANCIAL RECONCILIATION")
        fin = financial_reconciliation(df)
        print(f"  Source total       : ${fin['total_source']:>12,.2f}")
        print(f"  Record count       : {fin['record_count']:>12,}")
        print(f"  Negative balances  : {fin['negative_count']:>12,}")
        print(f"  Non-numeric values : {fin['non_numeric_count']:>12,}")
        print(f"  {fin['note']}")

    # ── 7. Sample Rows ────────────────────────────────────────────────────────
    sep("7 · SAMPLE ROWS (first 3 — decision-tagged)")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 160)
    pd.set_option("display.max_colwidth", 28)
    display_cols = [c for c in df.columns if not c.startswith("_")] + ["_decision"]
    print(df_decided[display_cols].head(3).to_string(index=False))

    # ── 8. Summary & Recommendations ─────────────────────────────────────────
    sep("8 · SUMMARY & NEXT STEPS")
    high_null = [c for c in df.columns if col_profiles[c]["null_pct"] > 10]
    print(f"  Rows total     : {len(df):,}  ← reconciliation target")
    print(f"  Ready to load  : {valid_n:,}")
    print(f"  Need review    : {review_n:,}")
    print(f"  Quarantined    : {quarantine_n:,}")
    print(f"  High-null cols : {high_null if high_null else 'None'}")

    print(f"\n  Recommended next steps:")
    if quarantine_n:
        print(f"    1. Resolve {quarantine_n} quarantined records before scheduling load")
    if review_n:
        print(f"    2. Get client sign-off on {review_n} review-flagged records")
    if fk_cols:
        print(f"    3. Run orphan check: check_orphans(this_df, accounts_df, '{fk_cols[0]}', 'reference_id', '{dataset_type}')")
    print(f"    4. Set reconciliation target: {len(df):,} rows in → {len(df):,} rows accounted for at load end\n")

    return {
        "file":         path.name,
        "dataset_type": dataset_type,
        "rows":         len(df),
        "grain":        grain_result,
        "decisions": {
            "valid":      int(valid_n),
            "review":     int(review_n),
            "quarantine": int(quarantine_n),
        },
        "col_profiles": col_profiles,
        "df_decided":   df_decided,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  CROSS-FILE RELATIONAL REPORT
# ══════════════════════════════════════════════════════════════════════════════

def run_relational_checks(results: dict) -> list:
    """
    After all files are profiled, run cross-file orphan checks.
    Uses the inferred dataset types to determine parent-child relationships.
    """
    checks = []
    accounts_df = next(
        (r["df_decided"] for r in results.values() if r.get("dataset_type") == "accounts"),
        None,
    )
    if accounts_df is None:
        log.warning("[RELATIONAL] No accounts file detected — skipping cross-file checks")
        return checks

    parent_pk = next(
        (c for c in ["reference_id", "account_id"] if c in accounts_df.columns), None
    )
    if not parent_pk:
        log.warning("[RELATIONAL] No PK column found in accounts — skipping")
        return checks

    child_fk_map = {
        "secondary_contacts": "parent_id",
        "notes":              "account_id",
        "documents":          "account_id",
        "invoices":           "account_id",
    }

    for fname, result in results.items():
        dtype = result.get("dataset_type")
        if dtype in child_fk_map:
            child_fk = child_fk_map[dtype]
            child_df = result.get("df_decided")
            if child_df is not None and child_fk in child_df.columns:
                check = check_orphans(child_df, accounts_df, child_fk, parent_pk, dtype)
                checks.append(check)

    return checks


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIT TRAIL — persist results to JSON
# ══════════════════════════════════════════════════════════════════════════════

def save_audit(audit: dict, folder: str = "."):
    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = Path(folder) / f"eda_audit_{ts}.json"

    # Strip non-serialisable objects (DataFrames) before saving
    clean = {}
    for fname, result in audit.items():
        clean[fname] = {k: v for k, v in result.items() if k != "df_decided"}

    with open(out_path, "w") as f:
        json.dump(clean, f, indent=2, default=str)

    print(f"\n  📄 Audit trail saved → {out_path}")
    return str(out_path)



# ══════════════════════════════════════════════════════════════════════════════
#  OUTPUT FILE SAVING — split processed data into valid / review / quarantine
# ══════════════════════════════════════════════════════════════════════════════

def save_output_files(all_results: dict, staging_folder: str):
    """
    For every processed dataset, write three CSVs to an /output subfolder:
      valid_<dataset>.csv       — clean records ready to load
      review_<dataset>.csv      — records that need client sign-off before load
      quarantine_<dataset>.csv  — records that must NOT be loaded without fix

    The _decision and _action columns are included in review/quarantine files
    so every file is self-documenting.
    """
    output_dir = Path(staging_folder) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    files_written = []

    for fname, result in all_results.items():
        df    = result.get("df_decided")
        dtype = result.get("dataset_type", "unknown")

        if df is None or df.empty:
            continue

        splits = {
            "valid":      df[df["_decision"] == "VALID"].drop(columns=["_decision", "_action"]),
            "review":     df[df["_decision"] == "REVIEW"],
            "quarantine": df[df["_decision"] == "QUARANTINE"],
        }

        for bucket, bucket_df in splits.items():
            if bucket_df.empty:
                continue
            out_name = f"{bucket}_{dtype}.csv"
            out_path = output_dir / out_name
            bucket_df.to_csv(out_path, index=False)
            files_written.append(str(out_path))

    # Print output manifest
    print("\n" + "2500" * SEP_WIDTH)
    print(f"  OUTPUT FILES SAVED → {output_dir}")
    print("2500" * SEP_WIDTH)
    for f in files_written:
        name      = Path(f).name
        row_count = sum(1 for _ in open(f)) - 1
        bucket    = name.split("_")[0]
        icon      = {"valid": "✅", "review": "🟡", "quarantine": "🔴"}.get(bucket, "  ")
        print(f"  {icon}  {name:<45} {row_count:>4} rows")

    print(f"\n  Total files written : {len(files_written)}")
    print(f"  Location            : {output_dir}\n")
    return str(output_dir)

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python eda.py <file.csv>  OR  python eda.py <folder/>")
        sys.exit(1)

    target = Path(sys.argv[1])
    all_results = {}

    if target.is_dir():
        csv_files = sorted(target.rglob("*.csv"))
        if not csv_files:
            print(f"No CSV files found in {target}")
            sys.exit(1)
        print(f"\nFound {len(csv_files)} CSV file(s) in '{target}'")
        for f in csv_files:
            result = run_eda(str(f))
            if result:
                all_results[f.name] = result

        # Cross-file relational checks
        if len(all_results) > 1:
            print(f"\n{'═' * SEP_WIDTH}")
            print(f"  CROSS-FILE RELATIONAL INTEGRITY")
            print(f"{'═' * SEP_WIDTH}")
            checks = run_relational_checks(all_results)
            for check in checks:
                if "error" in check:
                    print(f"\n  ⚠️  {check['error']}")
                    continue
                icon = "🔴" if check["orphan_count"] else "✅"
                print(f"\n  {icon} {check['child_dataset']}.{check['child_fk']} → accounts.{check['parent_pk']}")
                print(f"     Orphans   : {check['orphan_count']:,} / {check['total_child']:,} ({check['orphan_pct']}%)")
                print(f"     Decision  : {check['decision']}")
                print(f"     Action    : {check['action']}")
                if check["sample_orphans"]:
                    print(f"     Samples   : {check['sample_orphans']}")

            # Save audit trail
            audit_path = save_audit(all_results, str(target))

            # Save split output files
            save_output_files(all_results, str(target))

            # Final pipeline summary
            print(f"\n{'═' * SEP_WIDTH}")
            print(f"  PIPELINE SUMMARY")
            print(f"{'═' * SEP_WIDTH}")
            total_rows = sum(r["rows"] for r in all_results.values())
            total_valid = sum(r["decisions"]["valid"] for r in all_results.values())
            total_review = sum(r["decisions"]["review"] for r in all_results.values())
            total_q = sum(r["decisions"]["quarantine"] for r in all_results.values())
            print(f"  Files processed  : {len(all_results)}")
            print(f"  Total rows       : {total_rows:,}  ← reconciliation baseline")
            print(f"  ✅ Valid          : {total_valid:,}")
            print(f"  🟡 Review         : {total_review:,}")
            print(f"  🔴 Quarantine     : {total_q:,}")
            print(f"  Audit saved to   : {audit_path}\n")

    elif target.is_file():
        result = run_eda(str(target))
        if result:
            save_audit({target.name: result}, str(target.parent))
            save_output_files({target.name: result}, str(target.parent))
    else:
        print(f"❌  Path not found: {target}")
        sys.exit(1)
