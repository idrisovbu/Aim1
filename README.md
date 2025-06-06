# Aim 1 Analysis Pipeline

This repository contains R scripts for analyzing Medicare claims for HIV-positive beneficiaries. The workflow runs on a high-performance cluster where launchers submit array jobs and worker scripts process and model the data.

## Repository Layout

```
aim1_scripts/
├── A_data_preparation/
│   ├── A1_launcher/   # Submits batch jobs that discover data and dispatch workers
│   └── A2_worker/     # Reads raw Parquet files and compiles yearly datasets
├── B_analysis/
│   ├── B1_launcher/   # Launches analysis jobs (descriptive counts, models)
│   ├── B2_worker/     # Performs heavy analysis such as two‑part models
│   └── B3_aggregator/ # Collects worker outputs and builds final summaries
└── Z_utilities/       # Miscellaneous or in-progress scripts
```

## Execution Overview

1. **Prepare Data** – Run launchers in `A_data_preparation/A1_launcher` to create job arrays that scan available folders and call the workers in `A2_worker`.
2. **Run Analyses** – Use `B_analysis/B1_launcher` scripts to submit jobs for descriptive statistics or the two‑part cost model. Each job reads the compiled data and runs the code in `B2_worker`.
3. **Aggregate Results** – After workers finish, execute the scripts under `B3_aggregator` to merge CSV outputs, adjust for inflation, and generate tables and figures.

All outputs are saved under `/output_aim1/<DATE>/`, where `<DATE>` reflects the run date.

## Key Concepts

- **Array Jobs** – Launchers use `SUBMIT_ARRAY_JOB` to distribute work across many cluster tasks with specific memory and time requirements.
- **Parquet and Arrow** – Workers operate on Parquet files for efficient I/O using the `arrow` package.
- **Two‑Part Modeling** – The main model fits a logistic regression for any cost and a gamma regression for positive costs, bootstrapping estimates by disease, race, HIV status, and more.

## Getting Started

New contributors should:

- Review a launcher script alongside its worker counterpart to understand how parameters pass via array indices.
- Explore `B2_worker_two_part_model.R` for the modeling approach.
- Look at `B3_descriptive_tables_and_figures.R` to see how final outputs are produced.

This structure allows the project to scale across large claims datasets while keeping each piece of the pipeline modular.
