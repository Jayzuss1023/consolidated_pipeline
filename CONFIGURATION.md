# CONFIGURATION GUIDE - Step by step

A step-by-step walkthrough of everything that must exist, in order, for the `consolidated_pipeline` notebooks to run: first on the **Amazon S3** side, then on the **Databricks** side. Written for someone configuring this stack for the first time — every step says what to do and why you're doing it.

> **The big picture:** files appear in an S3 "landing" folder → Databricks reads them into a raw `bronze` table → cleans/joins them into `silver` → aggregates them into `gold` → moves the files to a `processed` folder so the next run only picks up new ones.

---

## Part 1 — Configure the Amazon S3 side

1. **Create an S3 bucket.** In this project it's `sports-consolidated-dp`. The name matters — it appears verbatim in every notebook path, and a one-character typo here caused a silent multi-minute retry hang (S3 retries `NoSuchBucket` errors). Copy the name exactly wherever you use it.
2. **Create one folder per data source** at the bucket root:
   ```text
   sports-consolidated-dp/
   ├── customers/
   ├── products/
   ├── gross_price/
   └── orders/
   ```
3. **Inside `orders/`, create the two working folders** that make incremental loading possible:
   ```text
   orders/
   ├── landing/    ← new files arrive here (the "inbox")
   └── processed/  ← the pipeline moves them here when done (the "archive")
   ```
   Why two folders? So that "what still needs processing" and "what has already been processed" are physically separated. A run simply reads `landing/*.csv` and never re-reads history.
4. **Upload your CSV files** (with a header row) into `landing/` (for orders) or the source folder (for dimensions). File naming is free — the pipeline records each file's name in the data itself via `_metadata.file_name`, so you can always trace a row back to its file.
5. **Give Databricks read/write access to the bucket.** This is the step people forget. In the Databricks workspace, a catalog admin creates:
   1. a **storage credential** (an IAM role that Databricks can assume against your AWS account), and
   2. an **external location** pointing at `s3://sports-consolidated-dp/`, granted to the users/groups who will run the pipeline.
      Serverless compute then resolves `s3://…` paths automatically through the external location — no mounts, no copying, no access keys in notebooks.
6. **Test the connection before building anything.** In a notebook, run `dbutils.fs.ls('s3://sports-consolidated-dp/orders/landing')` — you should see your files listed. If this fails or hangs, fix access now; nothing downstream will work.

---

## Part 2 — Configure the Databricks side

1. **Create the Unity Catalog container objects.** One catalog, three schemas — one per medallion layer:
   ```sql
   CREATE CATALOG IF NOT EXISTS fmcg;
   CREATE SCHEMA IF NOT EXISTS fmcg.bronze;   -- raw data
   CREATE SCHEMA IF NOT EXISTS fmcg.silver;   -- cleaned data
   CREATE SCHEMA IF NOT EXISTS fmcg.gold;     -- business aggregates
   ```
   All notebooks resolve tables through these names, e.g. `fmcg.bronze.orders`.
2. **Pick your compute.** These notebooks run on **serverless** compute — select a serverless warehouse/cluster in the notebook's compute picker. No sizing to do.
3. **Run `1_setup/utilities` once.** It defines the three schema-name constants (`bronze`, `silver`, `gold`) that every other notebook picks up through `%run`. Tiny file, big job: change the schema names here and nowhere else.
4. **Load the dimensions first** (they must exist before the orders join):
   1. `2_dimension_processing/01_bronze_processing` — CSVs → `fmcg.bronze.<source>`
   2. `2_dimension_processing/02_silver_processing` — cleaning → `fmcg.silver.<source>`
   3. `products_data_processing` and `pricing-data-processing` — the products and gross_price dimensions
5. **Do the one-time full load:** run `3_fact_fata_processing/1_full_load_fact`.
   - Set the widgets at the top: `catalog = fmcg`, `data_source = orders`.
   - What it does, in order: reads **all** `landing/*.csv` → appends to `fmcg.bronze.orders` (Delta + Change Data Feed) → moves every file to `processed/` → cleans (nulls, customer_id regex, date formats, dedup) → joins the products dimension → MERGEs into `fmcg.silver.orders` → builds the gold fact tables.
6. **Daily/incremental runs:** drop new CSVs into `orders/landing/` and run `3_fact_fata_processing/2_incremental_load_fact`.
   - It guards against an empty landing folder (falls back to the last batch from staging instead of crashing), appends **only new files** to bronze, MERGEs the cleaned batch into silver, recomputes the monthly gold aggregate **only for the months in the batch**, and cleans up its staging tables when done.
7. **Verify it worked.** Run these and sanity-check the numbers:
   ```sql
   SELECT COUNT(*) FROM fmcg.bronze.orders;   -- raw
   SELECT COUNT(*) FROM fmcg.silver.orders;   -- clean (smaller)
   SELECT COUNT(*), MIN(date), MAX(date) FROM fmcg.gold.fact_orders;  -- monthly
   ```
   Expect silver < bronze (duplicates and invalid rows removed) and the gold date range to cover your history.
8. **Automate it (recommended).** Create a Databricks Job that runs the incremental notebook on a schedule (e.g. every morning), with the widget values set as job parameters. Add alerting so a failed run tells you, rather than you discovering stale data a week later.

---

## Troubleshooting — every error below actually happened in this project

| Symptom                                               | What it really means                                                                                                  | Fix                                                                                                                      |
| ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `INVALID_PATH_STRING` … `URISyntaxException`          | The path variable is malformed — e.g. `s3:` instead of `s3://`                                                        | Use `s3://bucket/...` with the double slash                                                                              |
| `PATH_NOT_FOUND` on `landing/*.csv` when re-running   | The pipeline already moved the files to `processed/` — landing is empty **by design**                                 | Nothing is broken; the incremental notebook now handles this via its empty-landing guard                                 |
| The read "hangs" for minutes then fails               | Usually a wrong bucket name (typo) — S3 retries `NoSuchBucket` silently                                               | Check the bucket name character by character; `dbutils.fs.ls` on the root first                                          |
| Row counts in bronze double after reprocessing a file | Bronze is append-only — reprocessed files get appended again                                                          | Before reprocessing, remove that file's rows from bronze (filter on `file_name`), or never move files back to `landing/` |
| A join returns far fewer rows than expected           | Join key type mismatch (e.g. int `product_id` vs string) — rows silently don't match                                  | Cast both sides to the same type before joining                                                                          |
| ~10% of orders missing in silver                      | Placeholder `product_id`s (77777777, 88888888, 99999999) don't exist in the products dimension, and the join is inner | Expected today; map them to an `UNKNOWN` dimension member or filter them upstream                                        |
| Dates come out null                                   | The incoming date format wasn't one the parser tries                                                                  | Add the format to the `try_to_date` coalesce chain in the silver cleaning cell                                           |

---

## Glossary (30-second versions)

- **Medallion architecture** — bronze (raw, as-landed) → silver (validated, conformed) → gold (business aggregates). Each layer is fully queryable, and downstream layers can always be rebuilt from upstream.
- **Delta Lake** — the table format used for every layer: ACID transactions, time travel, and versioned history.
- **Change Data Feed (CDF)** — a Delta option that records what changed at every write; enabled on all bronze/silver/gold tables here for auditability.
- **MERGE (upsert)** — "update the row if the key exists, insert it if it doesn't." Makes reruns safe: no duplicates.
- **Unity Catalog** — Databricks's governance layer: catalogs → schemas → tables, with permissions and lineage. `fmcg` is this project's catalog.
- **External location** — the Unity Catalog object that connects `s3://` paths to an IAM credential, so notebooks can read S3 without keys or mounts.
- **Landing / processed** — the S3 inbox/archive folder pair that turns "full reload" into "only new files."
- **Staging table** — a temporary per-batch snapshot used inside a run, dropped when the run finishes.
