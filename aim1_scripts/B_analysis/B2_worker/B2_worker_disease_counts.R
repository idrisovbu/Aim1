##----------------------------------------------------------------
##' Title: B2_worker_disease_counts.R
##' Description: Creates summary statistics and summary model outputs
##' Author:      Bulat Idrisov
##' Date:        2025-05-04
##' Outputs: /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/01.Summary_Statistics/<date>
##'          /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/02.Regression_Estimates/<date>
##'          /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/03.Meta_Statistics/<date>
##' ----------------------------------------------------------------

rm(list = ls())
pacman::p_load(
  dplyr, tibble, broom, readr, stringr, purrr,
  openxlsx, RMySQL, data.table, ini, DBI
)

library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if ("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))

# Set drive paths
if (Sys.info()["sysname"] == 'Linux'){
  j <- "/home/j/"
  h <- paste0("/ihme/homes/", Sys.info()[7], "/")
  l <- '/ihme/limited_use/'
} else if (Sys.info()["sysname"] == 'Darwin'){
  j <- "/Volumes/snfs"
  h <- paste0("/Volumes/", Sys.info()[7], "/")
  l <- '/Volumes/limited_use'
} else {
  j <- "J:/"
  h <- "H:/"
  l <- 'L:/'
}

##----------------------------------------------------------------
# 0. Read in data from SLURM job submission
##----------------------------------------------------------------

if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_F2T_data_2019_age80.parquet"
  df_input <- open_dataset(path) %>% collect() %>% as.data.table() 
  year_id <- df_input$year_id[1]
  file_type <- if (str_detect(path, "F2T")) "F2T" else "RX"
  age_group_years_start <- df_input$age_group_years_start[1]
  
} else {
  # Read job args from SUBMIT_ARRAY_JOB
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input <- args[1]
  
  # Identify row using SLURM array task ID
  array_job_number <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message("SLURM Job ID: ", array_job_number)
  
  df_parameters <- fread(fp_parameters_input)
  df_job <- df_parameters[array_job_number, ]
  
  fp_input <- df_job$directory
  file_type <- df_job$file_type
  year_id <- df_job$year_id
  
  # Load data (can switch to open_dataset() if needed)
  df_input <- read_parquet(fp_input) %>% as.data.table()
  age_group_years_start <- df_input$age_group_years_start[1]
}

##----------------------------------------------------------------
## 0.1. Functions
##----------------------------------------------------------------

# Utility function to create directories recursively if not already present
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Function to generate output filenames based on file type and year
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_", file_type, "_year", year_id, "_age", age_group_years_start, extension)
}

##----------------------------------------------------------------
## 1. Create output folders
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder <- format(Sys.time(), "%Y%m%d")

# Define subfolders for output types
summary_stats_folder <- file.path(base_output_dir, "01.Summary_Statistics/", date_folder)
regression_estimates_folder <- file.path(base_output_dir, "02.Regression_Estimates/", date_folder)
meta_stats_folder <- file.path(base_output_dir, "03.Meta_Statistics/", date_folder)

# Define log folder
log_folder <- file.path(base_output_dir, "logs")

# Create all necessary directories
ensure_dir_exists(summary_stats_folder)
ensure_dir_exists(regression_estimates_folder)
ensure_dir_exists(meta_stats_folder)
ensure_dir_exists(log_folder)

# Construct log file path
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
if (is.na(task_id)) task_id <- "interactive"
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file <- file.path(log_folder, paste0("error_log_", task_id, "_", timestamp, ".txt"))

##----------------------------------------------------------------
## 1. Metadata summary
##----------------------------------------------------------------

meta_stats_2019 <- df_input[, .(
  year_id = unique(year_id),
  file_type = unique(file_type),
  total_unique_bene = uniqueN(bene_id),
  total_bene_acause_combo = uniqueN(.SD[, .(bene_id, acause_lvl2)]),
  mean_any_cost = mean(has_cost, na.rm = TRUE),
  hiv_unique_bene = uniqueN(bene_id[has_hiv == 1]),
  sud_unique_bene = uniqueN(bene_id[has_sud == 1]),
  hepc_unique_bene = uniqueN(bene_id[has_hepc == 1]),
  hiv_and_sud_unique_bene = uniqueN(bene_id[has_hiv == 1 & has_sud == 1]),      
  hiv_and_hepc_unique_bene = uniqueN(bene_id[has_hiv == 1 & has_hepc == 1]),    
  sud_and_hepc_unique_bene = uniqueN(bene_id[has_sud == 1 & has_hepc == 1]),    
  hiv_sud_hepc_unique_bene = uniqueN(bene_id[has_hiv == 1 & has_sud == 1 & has_hepc == 1]) 
), by = .(toc, age_group_years_start)]



# Save metadata
file_out_meta <- generate_filename("summary_meta", ".csv")
out_path_meta <- file.path(meta_stats_folder, file_out_meta)
fwrite(meta_stats_2019, out_path_meta)

cat("Meta statistics saved to:", out_path_meta, "\n")

##----------------------------------------------------------------
## 2. Summary statistics by disease group
##----------------------------------------------------------------

# Cross-tab: count of beneficiaries per group
cross_tab_dt <- df_input[, .(n_benes_per_group = .N),
                         by = .(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start)
]

# Utilization summary
acause_utilization_dt <- df_input[, .(
  avg_encounters_per_bene = mean(unique_encounters),
  sum_encounters_per_group = sum(unique_encounters)
), by = .(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start)]

# Cost summary
acause_cost_dt <- df_input[, .(
  avg_cost_per_bene = mean(tot_pay_amt, na.rm = TRUE),
  avg_cost_per_encounter = sum(tot_pay_amt) / sum(unique_encounters),
  # Winsorized mean (cutting at 99.5%)
  avg_cost_per_bene_winsorized = {
    cutoff <- quantile(tot_pay_amt, probs = 0.995, na.rm = TRUE)
    mean(pmin(tot_pay_amt, cutoff), na.rm = TRUE)
  },
  max_cost_per_bene = max(tot_pay_amt, na.rm = TRUE),
  quantile_99_cost_per_bene = quantile(tot_pay_amt, probs = 0.99, na.rm = TRUE),
  sum_cost_per_group = sum(tot_pay_amt, na.rm = TRUE)
), by = .(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start)]

# Merge summaries
summary_dt <- merge(acause_cost_dt, cross_tab_dt,
                    by = c("acause_lvl2", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start"),
                    all = TRUE)

summary_dt <- merge(summary_dt, acause_utilization_dt,
                    by = c("acause_lvl2", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start"),
                    all = TRUE)

# Add metadata
summary_dt[, `:=`(year_id = year_id, file_type = file_type)]

# Set column order
setcolorder(summary_dt, c(
  "acause_lvl2", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start",
  "avg_cost_per_bene", "avg_cost_per_encounter","avg_cost_per_bene_winsorized","max_cost_per_bene", "quantile_99_cost_per_bene", "sum_cost_per_group",
  "n_benes_per_group", "avg_encounters_per_bene", "sum_encounters_per_group",
  "year_id", "file_type"
))

# Save
file_out_summary <- generate_filename("summary_stats", ".csv")
out_path_summary <- file.path(summary_stats_folder, file_out_summary)
fwrite(summary_dt, out_path_summary)

cat("Summary statistics saved to:", out_path_summary, "\n")
