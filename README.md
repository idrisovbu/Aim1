
# Aim 1 Analysis Pipeline

**Author:** Bulat Idrisov  
This repository contains scripts for analyzing Medicare claims data for HIV-positive beneficiaries. The workflow follows a structured pipeline for launching jobs, processing data, and aggregating results for descriptive and modeled cost estimates.

---

## 📁 Folder Structure

```
aim1_scripts/
├── 01_launcher/         # Scripts that start job executions (batch submissions)
├── 02_workers/          # Scripts that run analysis tasks (data processing)
└── 03_aggregator/       # Scripts that summarize and consolidate results
```
---

## ⚙️ Execution Workflow

### Step 1: Start Data Processing Jobs

Launcher scripts submit jobs to process Medicare data.

- `01_launcher_disease_counts_F2T.R` → Launches jobs to count diseases in F2T (non-pharmaceutical) data.
- `01_launcher_two_part_model_F2T.R` → Launches jobs to run two-part regression for F2T cost data.
- `01_launcher_disease_counts_RX.R` → Launches jobs to count diseases in RX (pharmaceutical) data.
- `01_launcher_two_part_model_RX.R` → Launches jobs to run two-part regression for RX (pharmacy) data.

**Order of execution:**

1. Run: `01_launcher_disease_counts_F2T.R` and RX scripts  
2. Then run: `01_launcher_two_part_model_F2T.R` and `01_launcher_two_part_model_RX.R`

---

### Step 2: Process Data in Worker Scripts

Worker scripts are executed automatically after launcher jobs are submitted.

- `02_worker_disease_counts_F2T.R` → Summary stats in F2T data.
- `02_worker_two_part_model_F2T.R` → Runs a two-part cost model on F2T data.
- `02_worker_disease_counts_RX.R` → Summary stats in RX data.
- `02_worker_two_part_model_RX.R` → Runs a two-part cost model on RX data.

---

### Step 3: Aggregate and Summarize Results

After workers finish, run these scripts manually to generate final tables and plots:

- `03_aggregator_HIV_comorbidities.R` → Aggregates disease summary statistics.
- `03_aggregator_two_part_model.R` → Aggregates bootstrapped regression estimates.
- 03_figures script

---

## 📂 Output Directory Structure

All outputs are saved under `/output_aim1/YYYYMMDD/` by execution date.

```
