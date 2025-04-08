##----------------------------------------------------------------
##' Title: 02_worker_disease_counts_F2T.R
##'
##' Purpose:
##' This script processes Medicare F2T claims data to generate summary 
##' statistics on disease categories, healthcare costs, and the presence 
##' of HIV. It performs the following tasks:
##'
##'   (1) Identifies unique beneficiaries and disease cases.
##'   (2) Categorizes diseases into Cardiometabolic, SUD_mental, and Other.
##'   (3) Computes proportions of HIV+ beneficiaries within each category.
##'   (4) Summarizes healthcare costs, including average and total costs.
##'   (5) Computes the proportion of beneficiaries with zero costs.
##'   (6) Generates a cross-tab of diseases by HIV status.
##'   (7) Runs two regression models (logistic & gamma) to analyze cost 
##'       variations and saves regression estimates separately.
##'
##' Inputs:
##'   - Medicare F2T dataset (ICD-coded claims).
##'   - Metadata (run ID, year, disease categories, etc.).
##'
##' Outputs:
##'   - `summary_stats.csv`: Descriptive statistics on diseases and costs.
##'   - `regression_results.csv`: Coefficients and p-values from regressions.
##'
##' Author:      Bulat Idrisov
##' Date:        2025-03-06
##' Version:     3.0
##' ----------------------------------------------------------------



rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx, readr, purrr)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if ("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

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

if (interactive()) {
  # Manually specify the permutation of interest to load in data
  input_run_id <- "77"
  input_toc <- "ED"
  input_year_id <- "2019"
  input_code_system <- "icd10"
  input_age_group_years_start <- "80"
  fp_input <- paste0("/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_", input_run_id, 
                     "/F2T/data/toc=", input_toc,
                     "/year_id=", input_year_id,
                     "/code_system=", input_code_system,
                     "/age_group_years_start=", input_age_group_years_start)
  
  # load data
  df_input <- open_dataset(fp_input) %>%
    collect() %>%
    as.data.frame()
  
} else { 
  # Read in args from SUBMIT_ARRAY_JOBS(), read in .csv containing permutations, subset to row based on task_id
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input <- args[1] # fp_parameters_input <- fp_parameters
  
  # use Task id to identify params for this run
  array_job_number <- Sys.getenv("SLURM_ARRAY_TASK_ID") 
  print(paste0("job number:", array_job_number))
  df_parameters <- as.data.table(fread(fp_parameters_input))
  df_parameters <- df_parameters[as.integer(array_job_number), ]
  
  fp_input <- df_parameters$directory[1]
  
  # set parameters used in output
  input_run_id <- df_parameters$runid[1]
  input_toc <- df_parameters$toc[1]
  input_year_id <- df_parameters$year_id[1]
  input_code_system <- df_parameters$code_system[1]
  input_age_group_years_start <- df_parameters$age_group_years_start[1]
  
  # load data
  df_input <- open_dataset(fp_input) %>%
    collect() %>%
    as.data.frame()
  
}

# #####
# # Define output folder (if not already defined) using today's date
# folder_timestamp <- format(Sys.Date(), "%Y%m%d")
# output_directory <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"
# output_folder_dxcount <- file.path(output_directory, folder_timestamp, "diseases_count")
# output_folder <- file.path(output_directory, date_folder)
# reg_folder <- file.path(output_folder, "Regression_Estimates")
# ensure_dir_exists <- function(dir_path) {
#   if (!dir.exists(dir_path)) {
#     dir.create(dir_path, recursive = TRUE)
#   }
# }
# 
# ensure_dir_exists(reg_folder)
# if (!dir.exists(output_folder_dxcount)) {
#   dir.create(output_folder_dxcount, recursive = TRUE)
# }
# 
# # Create a logs subfolder within the output folder
# log_folder <- file.path(output_folder_dxcount, "logs")
# if (!dir.exists(log_folder)) {
#   dir.create(log_folder, recursive = TRUE)
# }
# 
# # Get the SLURM array task ID; if not available, set to "interactive"
# task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
# if (is.na(task_id)) {
#   task_id <- "interactive"
# }
# 
# # Create a unique log file name using the task ID and a timestamp
# timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
# log_file <- file.path(log_folder, paste0("error_log_", task_id, "_", timestamp, ".txt"))
# 
# 
# # Function to generate consistent filenames with metadata
# generate_filename <- function(prefix, extension) {
#   paste0(prefix, "_run", input_run_id, 
#          "_toc", input_toc, 
#          "_year", input_year_id, 
#          "_", input_code_system,
#          "_age", input_age_group_years_start, extension)
# }
# 

##----------------------------------------------------------------
## 0. Create output folders
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Define subfolders for output types
summary_stats_folder <- file.path(output_folder, "01.Summary_Statistics")
regression_estimates_folder <- file.path(output_folder, "02.Regression_Estimates")

# Utility function to create directories recursively if not already present
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Create all necessary directories
ensure_dir_exists(output_folder)
ensure_dir_exists(summary_stats_folder)
ensure_dir_exists(regression_estimates_folder)

# Optional: create logs subfolder inside summary stats
log_folder <- file.path(summary_stats_folder, "logs")
ensure_dir_exists(log_folder)

# Construct log file path (optional)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
if (is.na(task_id)) task_id <- "interactive"
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file <- file.path(log_folder, paste0("error_log_", task_id, "_", timestamp, ".txt"))

# Function to generate consistent filenames with metadata
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_run", input_run_id, 
         "_toc", input_toc, 
         "_year", input_year_id, 
         "_", input_code_system,
         "_age", input_age_group_years_start, extension)
}

####--------------------------------------------------------------------
####  Create HIV flag and preprocess input data
####--------------------------------------------------------------------

# hiv_beneficiaries <- df_input %>%
#   filter(acause == "hiv") %>%
#   distinct(bene_id) %>%
#   mutate(hiv_flag = 1)
# 
# if (nrow(hiv_beneficiaries) == 0) {
#   error_message <- paste(
#     "hiv_beneficiaries has no data! Need to check this permutation:",
#     "run_id:", input_run_id,
#     "year_id:", input_year_id,
#     "code_system:", input_code_system,
#     "toc:", input_toc,
#     "age_group_years_start:", input_age_group_years_start
#   )
#   cat(error_message, "\n", file = log_file, append = TRUE)
#   stop(error_message)
# }

# Create HIV flag by identifying beneficiaries with at least one HIV-coded diagnosis
hiv_beneficiaries <- df_input %>%
  filter(acause == "hiv") %>%
  distinct(bene_id) %>%
  mutate(hiv_flag = 1)

# Subset relevant variables and merge HIV flag
df <- df_input %>%
  select(bene_id, encounter_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt) %>%
  left_join(hiv_beneficiaries, by = "bene_id") %>%
  mutate(
    hiv_flag = if_else(is.na(hiv_flag), 0L, hiv_flag),
    race_cd = as.character(race_cd)
  ) %>%
  filter(race_cd %in% c("WHT", "BLCK", "HISP"))

# Collapse granular diagnosis codes into three broader categories
df <- df %>%
  mutate(acause = case_when(
    grepl("^cvd_", acause) | acause %in% c(
      "diabetes_typ2", "ckd", "renal_failure", "cirrhosis", "hepatitis_c", "rf_obesity"
    ) ~ "Cardiometabolic",
    grepl("^mental_", acause) | acause == "rf_tobacco" ~ "SUD_mental",
    TRUE ~ "other"
  ))

# Promote all SUD_mental rows to primary cause using temporary flag = 3
df <- df %>%mutate(primary_cause = if_else(acause == "SUD_mental", 3L, primary_cause))

# For each encounter, retain only one row, prioritizing SUD_mental and then existing primary cause
df <- df %>%
  group_by(encounter_id) %>%
  arrange(desc(primary_cause == 3), desc(primary_cause == 1)) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(primary_cause = if_else(primary_cause == 3L, 1L, primary_cause))

# Tag remaining rows as one encounter each for counting purposes
df <- df %>%mutate(encounter_flag = 1L)

# Collapse dataset to beneficiary × disease category level
df <- df %>%
  group_by(bene_id, acause) %>%
  summarise(
    race_cd = first(race_cd),
    sex_id = first(sex_id),
    hiv_flag = first(hiv_flag),
    number_of_encounters = sum(encounter_flag),
    tot_pay_amt = sum(tot_pay_amt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(any_cost = if_else(tot_pay_amt > 0, 1, 0))

# Compute total number of encounters per beneficiary (all causes, full dataset)
total_encounters_per_bene <- df_input %>%
  group_by(bene_id) %>%
  summarise(total_encounters = n_distinct(encounter_id), .groups = "drop")

# Merge total encounters to main summary dataset (for adjustment or stratification)
df <- df %>%left_join(total_encounters_per_bene, by = "bene_id")



####--------------------------------------------------------------------
####  Compute statistics and combine into a single table
####--------------------------------------------------------------------

# Sample sizes (global metadata)
meta_stats <- data.frame(
  run_id = input_run_id,
  toc = input_toc,
  year_id = input_year_id,
  code_system = input_code_system,
  age_group = input_age_group_years_start,
  total_unique_bene = length(unique(df$bene_id)),
  total_bene_acause_combo = nrow(unique(df[, c("bene_id", "acause")])),
  mean_any_cost = mean(df$any_cost),
  hiv_unique_bene = length(unique(df$bene_id[df$hiv_flag == 1]))
)

# Cross-tab of acause by HIV flag and race
cross_tab_df <- df %>%
  count(acause, hiv_flag, race_cd, name = "count") %>%
  arrange(acause, hiv_flag, race_cd)

# Utilization summary by acause, hiv_flag, and race
acause_utilization <- df %>%
  group_by(acause, hiv_flag, race_cd) %>%
  summarise(
    avg_encounters_per_bene = mean(number_of_encounters),
    total_encounters = sum(number_of_encounters),
    .groups = "drop"
  )

# Cost summary by acause, hiv_flag, and race
acause_cost <- df %>%
  group_by(acause, hiv_flag, race_cd) %>%
  summarise(
    avg_cost_per_bene = mean(tot_pay_amt, na.rm = TRUE),
    max_cost_per_bene = max(tot_pay_amt, na.rm = TRUE),
    quant99 = quantile(tot_pay_amt, probs = 0.99, na.rm = TRUE),
    total_cost = sum(tot_pay_amt, na.rm = TRUE),
    .groups = "drop"
  )

# Merge all summaries
summary_df <- acause_cost %>%
  left_join(cross_tab_df, by = c("acause", "hiv_flag", "race_cd")) %>%
  left_join(acause_utilization, by = c("acause", "hiv_flag", "race_cd"))

# Add metadata and finalize columns
summary_df <- summary_df %>%
  mutate(
    total_unique_bene = meta_stats$total_unique_bene,
    total_bene_acause_combo = meta_stats$total_bene_acause_combo,
    mean_any_cost = meta_stats$mean_any_cost,
    hiv_unique_bene = meta_stats$hiv_unique_bene,
    run_id = input_run_id,
    toc = input_toc,
    year_id = input_year_id,
    code_system = input_code_system,
    age_group = input_age_group_years_start
  ) %>%
  select(acause, hiv_flag, race_cd,
         avg_cost_per_bene, max_cost_per_bene, quant99, total_cost,
         count, avg_encounters_per_bene, total_encounters,
         total_unique_bene, total_bene_acause_combo, mean_any_cost, hiv_unique_bene,
         run_id, toc, year_id, code_system, age_group)




# Save
file_out_summary <- generate_filename("summary_stats", ".csv")
out_path_summary <- file.path(summary_stats_folder, file_out_summary)
write.csv(summary_df, out_path_summary, row.names = FALSE)

cat("Summary statistics saved to:", out_path_summary, "\n")


####--------------------------------------------------------------------
#### Run regressions and save results separately
####--------------------------------------------------------------------

# Logistic regression
mod_logit <- glm(
  any_cost ~ acause*hiv_flag + race_cd + sex_id,
  data = df,
  family = binomial(link="logit")
)

# Gamma regression for positive costs
mod_gamma <- glm(
  tot_pay_amt ~ acause*hiv_flag + race_cd + sex_id,
  data = filter(df, tot_pay_amt > 0),
  family = Gamma(link="log")
)

# Extract coefficients and p-values
extract_coefs <- function(model) {
  coefs <- summary(model)$coefficients
  coefs <- as.data.frame(coefs)
  coefs <- coefs[!rownames(coefs) %in% "(Intercept)", ] # Remove intercept
  coefs <- coefs %>%
    select(Estimate, `Pr(>|z|)` = 4) %>%
    rename(p_value = `Pr(>|z|)`)
  coefs[is.na(coefs)] <- "NA" # Ensure NA values are explicitly stated
  coefs$variable <- rownames(coefs)
  return(coefs)
}

logit_coefs <- extract_coefs(mod_logit)
gamma_coefs <- extract_coefs(mod_gamma)

# Merge logistic and gamma regression results
regression_results <- merge(logit_coefs, gamma_coefs, by="variable", all=TRUE, suffixes=c("_logit", "_gamma"))

# Add metadata to each row at the end
regression_results_metadata <- regression_results  %>%
  mutate(
    run_id = input_run_id,
    toc = input_toc,
    year_id = input_year_id,
    code_system = input_code_system,
    age_group = input_age_group_years_start
  ) 

# Save regression results separately
file_out_regression <- generate_filename("regression_results", ".csv")
out_path_regression <- file.path(regression_estimates_folder, file_out_regression)
write.csv(regression_results_metadata, out_path_regression, row.names = FALSE)

cat("Regression results saved to:", out_path_regression, "\n")
