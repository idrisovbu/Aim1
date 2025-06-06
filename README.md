
# Aim 1 Analysis Pipeline

**Author:** Bulat Idrisov  
This repository contains scripts for analyzing Medicare claims data for HIV-positive beneficiaries. The workflow follows a structured pipeline for launching jobs, processing data, and aggregating results for descriptive and modeled cost estimates.

---

## 📁 Folder Structure

```
a) Data preparation
├── A1_launcher/         # Scripts that start job executions (batch submissions)
├── A2_workers/          # Scripts that run analysis tasks (data processing)
b) analysis 
aim1_scripts/
├── B1_launcher/         # Scripts that start job executions (batch submissions)
├── B2_workers/          # Scripts that run analysis tasks (data processing)

```
---

## ⚙️ Execution Workflow

### Step 1: Start Data Processing Jobs

Launcher scripts submit jobs to process Medicare data.

In A folder


---

### Step 2: Process Data in Worker Scripts

Launch B folder scripts 
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
