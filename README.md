# Overview
This repository contains a scalable, production-ready data ingestion framework for extracting, transforming, and loading International Monetary Fund (IMF) datasets into an Amazon S3–based data platform.
 
The pipeline is designed to be reusable and configuration-driven, allowing new IMF datasets (such as BOP, IMTS, CPI, and future releases) to be onboarded by updating the orchestration configuration rather than modifying core pipeline code. This keeps the system easy to maintain and safe to extend.
 
To support both small and very large datasets, the framework uses streaming HTTP ingestion and streaming XML parsing, enabling it to process multi-million-row IMF datasets without exceeding memory limits. This makes the pipeline reliable under Airflow and suitable for long-running, high-volume data loads.
 
Built-in structured logging and run manifests provide full visibility into every execution, including what data was processed, where it was stored, and whether each step succeeded or failed. Manifests are written both centrally and alongside each dataset, supporting auditability, data lineage, and operational debugging.
 
Overall, this project provides a robust foundation for ingesting IMF data at scale and can be easily adapted for other SDMX-compliant or large API-based data sources.

-------
 
 <img width="1536" height="1024" alt="ChatGPT Image Jan 7, 2026, 04_26_00 PM" src="https://github.com/user-attachments/assets/6cab050f-22d2-4029-a33d-ea3c720bce48" />

-------



How it works
 
At a high level, the pipeline runs as a set of independent **(flow × year)** tasks. Each task downloads IMF data for a single year of a selected flow, stream-parses the XML response into rows, writes a compressed CSV, uploads it to S3, and records a structured result. At the end of the run, the pipeline writes manifests for traceability (central + per-flow).
 
 
## Step-by-step flow
 
### 1) Orchestration (Airflow)
 
* The Airflow DAG stays lightweight and focuses on **scheduling + selecting flows**.
* It calls a single Python entry point (e.g., `run_pipeline()`), passing which IMF flow(s) to run (e.g., `BOP`, `IMTS`).
 
**Why:** keeps the DAG simple and makes the pipeline reusable across datasets.
 
 
### 2) Partitioned processing (flow × year)
 
The pipeline processes data in small independent units:
 
* **flow** (e.g., `BOP`, `IMTS`)
* **year** (e.g., `2001`)
 
Each unit becomes one execution of:
 
* `process_flow_year(cfg, flow_name, flowref, year, run_id)`
 
**Why:** improves reliability and makes retries/resume easy.
 
 
### 3) Safe extraction (HTTP client + retries + rate limiting)
 
Each task calls the IMF API using:
 
* a pooled HTTP session (better performance)
* retries for transient errors (429/5xx)
* global rate limiting to avoid API throttling
 
**Why:** production safety (fewer failures, fewer bans, more predictable throughput).
 
 
### 4) Memory-safe transformation (stream parsing)
 
Instead of loading the full XML into memory, the pipeline:
 
* streams the HTTP response (`stream=True`)
* uses `ElementTree.iterparse()` to parse incrementally
* yields one row at a time (`Series.attrib + Obs.attrib`)
* clears XML elements as it goes to prevent OOM
 
**Why:** supports large flows (e.g., IMTS) without Airflow worker crashes.
 
 
### 5) Streaming output + optional compression
 
Rows are written to:
 
* a `SpooledTemporaryFile` (stays in memory until size threshold, then spills to disk)
* optionally wrapped with gzip for smaller files and faster uploads
 
**Why:** scalable output writing without holding full datasets in RAM.
 
 
### 6) Load to S3 (idempotent)
 
Before uploading, the pipeline can check if the target object already exists:
 
* if `skip_if_exists=True` and object exists → mark task as `skipped_exists`
* otherwise upload the new file
 
Files are written into existing flow folders, e.g.:
 
* `.../IMF.STA.BOP.21.0.0/BOP_2001.csv.gz`
* `.../IMF.STA.IMTS.1.0.0/IMTS_2001.csv.gz`
 
**Why:** safe reruns and clean storage organisation.
 
 
### 7) Observability (structured logs + manifests)
 
Every task emits a structured JSON log entry containing:
 
* flow, year, status, rows, runtime, S3 URI, run_id
 
At the end of the run, manifests are written:
 
* **Central run manifest**: includes all results
* **Per-flow manifests**: stored inside each flow folder
 
**Why:** auditability, lineage, debugging, and easy “what happened?” answers.
 
 
 
-------
 
 
## Tools & Technologies Used
 
### Development & Version Control
 
* **Visual Studio Code (VS Code)** – Local development, debugging, and code organisation.
* **Git & GitHub** – Version control, collaboration, and change tracking across pipeline iterations.
 
### Orchestration & Infrastructure
 
* **Apache Airflow** – Workflow orchestration, scheduling, retries, and operational monitoring.
* **Docker** – Containerised Airflow environment for consistent local and cloud execution.
* **Amazon EC2** – Compute environment hosting the Airflow workers and pipeline execution.
 
### Data Processing
 
* **Python** – Core language used for ingestion, transformation, and pipeline orchestration.
 
 
### Storage & Cloud Services
 
* **Amazon S3** – Durable object storage for processed datasets, manifests, and run artefacts.
 
 
### Why this stack?
 
The chosen tools emphasise:
 
* **Scalability** (streaming ingestion, S3-based storage)
* **Reliability** (Airflow orchestration, retries, idempotency)
* **Maintainability** (clean separation of concerns, configuration-driven design)
* **Reproducibility** (manifests, version control, containerisation)
 
 
 
-------
 
### N.B
 
The pipeline is optimised for streaming ingestion and lightweight transformations. If future transformation requirements become CPU- or memory-intensive, the architecture allows these steps to be scaled independently (e.g. using Spark or distributed processing) without redesigning the ingestion or storage layers.
