# telecom-migration-toolkit

A modular, production-grade data migration toolkit for telecom and ISP CSV onboarding.
Built for real-world migration work — messy source data, relational integrity checks,
and the need for a complete audit trail before anything touches a production system.

---

## What's in here

| File | Purpose |
|------|---------|
| `eda.py` | EDA & decision script — run this first on any incoming data |
| `migration_pipeline.py` | Full ETL pipeline — transforms, validates, and loads clean data |

---

## eda.py — Exploratory Data Analysis & Decision Script

Point it at any folder of CSV files and it profiles everything automatically.

**What it does:**
- Infers dataset type from column signatures — not filenames
- Normalises inconsistent headers via a fuzzy alias map
- Runs a grain audit — identifies the natural key and flags duplicates
- Profiles every column: null rate, unique count, semantic role, format issues
- Applies a decision layer to every row: `VALID` / `REVIEW` / `QUARANTINE`
- Runs cross-file orphan detection (notes, contacts, documents vs accounts)
- Saves split output CSVs: `valid_*`, `review_*`, `quarantine_*`
- Writes a timestamped audit JSON after every run

**Run it:**
```bash
python eda.py data/staging/        # all CSVs in folder
python eda.py data/staging/accounts.csv   # single file
```

**Output structure:**
```
staging/
├── output/
│   ├── valid_accounts.csv
│   ├── review_accounts.csv
│   ├── quarantine_accounts.csv
│   ├── valid_notes.csv
│   └── ...
└── eda_audit_20260101_120000.json
```

---

## migration_pipeline.py — Production-Grade ETL Pipeline

Full end-to-end pipeline. Takes raw CSV exports and moves clean, validated
data through staging into final tables — with full traceability.

**What it does:**
- Scans any folder recursively for CSV files
- Detects dataset types from column signatures
- Maps inconsistent legacy headers to canonical names
- Cleanses and transforms data (status enums, phone normalisation, address parsing)
- Checks relational integrity — orphaned child records are quarantined, not dropped
- Builds an ID map linking every legacy ID to a new target system ID
- Promotes clean data: `staging_*` → `final_*` tables in SQLite
- Runs financial reconciliation — totals, negative balance detection, variance
- Outputs a full JSON migration report

**Run it:**
```bash
python migration_pipeline.py ./data/raw
```

Or in Python:
```python
from migration_pipeline import run_migration_pipeline
report = run_migration_pipeline("./data/raw")
print(report)
```

---

## Design principles

**Staging-first**
Raw source files are never modified. All work happens on a copy.
Nothing reaches final tables until it has passed every validation gate.

**No data left behind**
Invalid records are quarantined to `invalid_[table]` with a human-readable
`failure_reason`. Nothing is silently dropped — every record is accounted for.

**Full traceability**
Every legacy ID is mapped to a target system ID. Rollback and reconciliation
are always possible. Every run produces a timestamped audit trail.

**Decision over observation**
The EDA script doesn't just flag problems — it assigns every row a decision
and a specific recommended action. The output tells you what to do, not just
what's wrong.

---

## Folder structure

```
project/
├── data/
│   ├── raw/          ← received files land here, never touched
│   ├── staging/      ← working copies, all processing happens here
│   └── output/       ← split output files (valid / review / quarantine)
├── scripts/
│   ├── eda.py
│   └── migration_pipeline.py
├── README.md
└── requirements.txt
```

---

## Setup

```bash
python -m venv venv
source venv/bin/activate       # Mac/Linux
venv\Scripts\activate          # Windows

pip install -r requirements.txt
```

**requirements.txt**
```
pandas>=2.0.0
openpyxl>=3.1.0
```

---

## Use case

Designed for telecom and ISP account migrations — works on any structured
CSV export with account, contact, notes, document, and invoice entities.
Handles the most common real-world issues: inconsistent headers, mixed status
vocabularies, freeform phone formats, orphaned child records, and Excel
serial date formats.

---

## Author

Olu Ogundipe
Data Migration Specialist
