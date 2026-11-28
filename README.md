# FMCG Orders Lakehouse — Medallion Pipeline on Databricks + Amazon S3

An end-to-end ELT pipeline that lands raw CSV files from Amazon S3 into a governed **medallion architecture** (bronze → silver → gold) on Databricks with Delta Lake, and serves a monthly product×customer sales aggregate for reporting — with **incremental loading** so day-to-day runs touch one new file instead of the full history.

---

## The solution, in numbers

| What                                                | Result (measured in this workspace)                                                                                             |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Raw orders captured (bronze, with Change Data Feed) | **414,830 rows** across **152 source files**                                                                                    |
| Clean, conformed orders (silver)                    | **40,811 rows** — 89% of raw rows were duplicates, null quantities, or invalid records                                          |
| Monthly sales aggregate (gold `fact_orders`)        | **96,332 rows** covering **24 months** (Jan 2024 – Dec 2025)                                                                    |
| Dimensions conformed                                | customers 39→35, products 20→18, gross_price 108→90 (invalid rows pruned)                                                       |
| Latest incremental batch                            | **350 rows / 1 file** processed end-to-end instead of 152 files / 414,830 rows — a **~99.9% reduction** in data scanned per run |
| Monthly aggregate refresh (incremental)             | Recomputed **1 affected month** instead of all 24 — **−96%** rows recalculated                                                  |
| Rerun safety                                        | 0 duplicate rows appended on empty-landing reruns (guarded append + MERGE)                                                      |

---

## Technology stack

| Component      | Technology                                  | Role in this project                                                                  |
| -------------- | ------------------------------------------- | ------------------------------------------------------------------------------------- |
| Object storage | Amazon S3 (`sports-consolidated-dp` bucket) | Landing zone for raw CSVs; per-source folders with `landing/` / `processed/` handoff  |
| Compute        | Databricks Serverless                       | Zero-infrastructure Spark; no cluster tuning or startup wait                          |
| Processing     | PySpark + Spark SQL                         | Reads, cleaning, joins, MERGE upserts                                                 |
| Storage format | Delta Lake                                  | ACID tables at every layer, with **Change Data Feed (CDF)** enabled for audit/history |
| Governance     | Unity Catalog (`fmcg` catalog)              | Schemas `bronze`, `silver`, `gold`; table-level ACLs, lineage, one namespace          |
| Orchestration  | Databricks notebooks + widgets              | Parameterized by `catalog` and `data_source` so the same notebooks serve every source |

---

## Architecture: why medallion, and what each layer does here

The medallion pattern progressively refines data while **keeping every raw byte queryable** — you never lose the ability to re-derive downstream layers from source.

```
S3 bucket (sports-consolidated-dp)
  ├── customers/*.csv          ┐
  ├── products/*.csv           ├──► Databricks notebooks
  ├── gross_price/*.csv        ┘        │
  └── orders/                            ▼
       ├── landing/  (new files)   ┌───────────── bronze ─────────────┐
       └── processed/ (done files) │ raw as-landed + read_timestamp    │
                                    │ + file_name/file_size metadata    │
                                    │ append-only, CDF enabled         │
                                    └──────────────┬───────────────────┘
                                                   ▼ validation & conformance
                                    ┌───────────── silver ─────────────┐
                                    │ typed columns, deduped rows,     │
                                    │ joined to product dimension,     │
                                    │ MERGE-upsert, CDF enabled        │
                                    └──────────────┬───────────────────┘
                                                   ▼ business aggregates
                                    ┌───────────── gold ───────────────┐
                                    │ sb_fact_orders (per-order fact)  │
                                    │ fact_orders (monthly aggregate)  │
                                    │ dim tables (customers, products, │
                                    │ gross_price)                    │
                                    └──────────────────────────────────┘
```

**Bronze — raw, immutable, replayable.** Files land in S3 and are read with `inferSchema`, stamped with `read_timestamp`, and enriched with `_metadata.file_name` / `file_size` so every row is traceable to its source file. Writes are append-only with CDF, so bronze doubles as an audit log of exactly what arrived, when.

**Silver — validated and conformed.** The cleaning contract (from the orders notebooks):

- drop rows with null `order_qty`
- validate `customer_id` against `^[0-9]+$`; invalids bucketed to sentinel `999999`
- normalize **four** incoming date formats (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `MMMM dd, yyyy`) to one `date` column via `try_to_date` coalescing
- deduplicate on the business key (`order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty`)
- cast `product_id` to string to match the products dimension, then inner-join to attach `product_code`
- upsert via `MERGE` (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`) — rerunnable without duplicating

**Gold — business-ready facts.** `sb_fact_orders` is the per-order fact; `fact_orders` is the monthly product×customer quantity aggregate. On incremental runs the aggregate is recomputed **only for the months present in the new batch** (a `TRUNC(date, 'MM')` join against the staging month list), then MERGEd into the fact table.

---

## How S3 and Databricks relate

S3 is the **external landing zone**; Databricks is the **governed lakehouse**. The contract between them is a folder-per-source layout:

- Each data source (customers, products, gross_price, orders) gets its own folder in the bucket.
- Order files arrive in `orders/landing/`. A run reads `landing/*.csv`, appends to bronze, then **moves each file to `orders/processed/`** — so `landing/` is a transient inbox that always reflects "work not yet done" and `processed/` is the archive (152 files to date).
- Databricks reads the bucket directly via a Unity Catalog **external location / storage credential** — no copying, no mounts; serverless compute resolves the path and injects scoped credentials.

This separation keeps raw data cheap and replaceable in S3, while Delta tables (which must be governed and versioned) live fully inside Unity Catalog.

---

## Bottlenecks found and fixed (before → after)

1. **Full reload every run.** The first version reprocessed all 152 files / 414,830 rows on every execution. → Incremental inbox pattern: a run now reads only new files; the latest batch was 1 file / 350 rows (**~99.9% less data scanned**).
2. **Silent S3 retry storm.** A one-character bucket typo (`-pb` instead of `-dp`) produced `NoSuchBucket` + exponential S3 retries that _hung_ the read instead of failing fast. → Corrected and path constants centralized once per notebook.
3. **Malformed URI crash.** `s3:` instead of `s3://` in the path variable threw `INVALID_PATH_STRING` (`URISyntaxException`) at read time. → Fixed; a one-line `dbutils.fs.ls` guard now fails fast with a meaningful state.
4. **Rerun crash on empty landing.** Once files moved to `processed/`, re-running the read cell threw `PATH_NOT_FOUND` — the pipeline appeared broken after every success. → Empty-landing guard: when `landing/` is empty the notebook reads the last batch from the staging table, and the bronze append is skipped, so reruns complete cleanly with **zero duplicate rows**.
5. **Duplicate appends on reprocessing.** Bronze is append-only, so moving a file back to `landing/` and rerunning duplicated rows. → Documented in the guide (clear the affected bronze rows before reprocessing) and downstream layers made idempotent with `MERGE`.
6. **Silent join key type mismatch.** Orders `product_id` infers as int; products dimension stores string. Without the cast this joins to nothing silently. → Explicit cast to string before joining — a class of bug that vanishes entirely if you type your schema at bronze.
7. **Full monthly-aggregate rebuild.** Recomputing `fact_orders` meant re-aggregating all 24 months / 96,332 rows every batch. → Affected-months-only recalculation: the latest batch recomputed 1 month (**−96%**).
8. **Dirty data at source.** Four date formats, non-numeric customer ids, null quantities, placeholder product ids. → The silver cleaning contract above: 414,830 raw rows → 45,587 clean (89% noise removed), then the inner join to the product dimension drops a further 4,776 rows (10.5%) whose `product_id`s are placeholders absent from the dimension — surfaced and measured, not hidden.

---

## Real-world advice for taking this to production

1. **Swap the manual `landing/` → `processed/` move for Auto Loader** (`cloudFiles`). You get file-notification-triggered, exactly-once ingestion with schema evolution for free — the manual `dbutils.fs.mv` pattern is fine for learning, but Auto Loader removes the whole class of "file already processed / moved mid-run" problems.
2. **Keep MERGE keys stable across load generations.** If your monthly fact ever grows _larger_ than your order count, your merge keys drifted between the full and incremental versions — rebase the aggregate from silver and standardize the key (`month_start, product_code, customer_code`) in both notebooks.
3. **Type your schema at bronze.** `inferSchema` per file is convenient but fragile — one file with a placeholder `77777777` in a numeric column can flip a column's type and silently break joins. Define an explicit schema once.
4. **Fix placeholder product ids upstream.** 4,776 rows (10.5% of clean orders) currently disappear at the inner join because `77777777`/`88888888`/`99999999`-style placeholders don't exist in the products dimension. Either map them to an `UNKNOWN` dimension member (left join + sentinel) so the revenue stays visible, or stop them at the source.
5. **Orchestrate with a Databricks Job**, parameterized by the existing `catalog`/`data_source` widgets, rather than running notebooks by hand — then add alerting on run failure.
6. **Add data-quality expectations.** The silver rules (regex, null checks, date parsing) are hand-written; encoding them as expectations would give you metrics and quarantined bad rows over time.
7. **Small things that bite:** watch variable typos (`silve_table`, a `gold_table` built from the _silver_ schema variable) — a 5-line smoke-check cell at the top of each notebook printing resolved table names catches these before a 3 a.m. run does.

---

## Repository structure

```text
consolidated_pipeline/
├── 1_setup/
│   └── utilities                     # schema name constants (bronze/silver/gold)
├── 2_dimension_processing/
│   ├── 01_bronze_processing          # CSV → bronze Delta (CDF)
│   ├── 02_silver_processing           # cleaning + conformance
│   └── products_data_processing       # products dimension
├── 3_fact_fata_processing/           # (sic) fact layer
│   ├── 1_full_load_fact               # one-time backfill, full history
│   └── 2_incremental_load_fact        # daily batch: guard → bronze → silver MERGE → gold MERGE
├── pricing-data-processing           # gross_price dimension
├── README.md                          # this file
└── SETUP_GUIDE.md                     # step-by-step configuration guide
```

## Verifying the pipeline yourself

The counts quoted above are reproducible: every figure comes from `SELECT COUNT(*)` against `fmcg.bronze.*`, `fmcg.silver.*`, and `fmcg.gold.*` in this workspace, and file counts from `dbutils.fs.ls` on the S3 landing/processed folders.
