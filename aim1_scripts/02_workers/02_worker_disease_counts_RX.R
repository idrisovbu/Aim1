##----------------------------------------------------------------
##' Title: 02_worker_disease_counts_RX.R
##'
##' Purpose:
##' This script processes Medicare RX claims data to generate summary 
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
##'   - Medicare RX dataset.
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

# Clear environment and set library paths
rm(list = ls())
pacman::p_load(arrow, dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,glmnet, fs)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

# Set option for more detailed error logging (enables line number in error log)
options(error = function() {
  print(sys.calls())
  q(status = 1)
})

# Set drive paths
if (Sys.info()["sysname"] == 'Linux'){
  j <- "/home/j/"
  h <- paste0("/ihme/homes/",Sys.info()[7],"/")
  l <- '/ihme/limited_use/'
} else if (Sys.info()["sysname"] == 'Darwin'){
  j <- "/Volumes/snfs"
  h <- paste0("/Volumes/",Sys.info()[7],"/")
  l <- '/Volumes/limited_use'
} else {
  j <- "J:/"
  h <- "H:/"
  l <- 'L:/'
}


###simulating non-interactive code

if (interactive()) {
  # Manually specify the permutation of interest
  input_run_id <- "77"
  input_toc <- "RX"
  input_year_id <- "2010"
  input_state_id <- "WY"
  input_age_group_years_start <- "65"
  input_code_system <- "icd10"  # <- FIXED: ensure code system is set
  
  # Define base input path (excluding sex_id)
  fp_input_base <- paste0("/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_", input_run_id, 
                          "/CAUSEMAP/data/carrier=false/toc=", input_toc,
                          "/year_id=", input_year_id,
                          "/st_resi=", input_state_id,
                          "/age_group_years_start=", input_age_group_years_start)
  
  # Load both sexes
  df_input_males <- open_dataset(paste0(fp_input_base, "/sex_id=1")) %>% collect() %>% as.data.frame()
  df_input_females <- open_dataset(paste0(fp_input_base, "/sex_id=2")) %>% collect() %>% as.data.frame()
  
  df_input_males$sex_id <- 1
  df_input_females$sex_id <- 2
  
  # Combine
  df_input <- bind_rows(df_input_males, df_input_females)
  
  # Optional: Read part_c and non-part_c files from male folders only (or adapt this logic)
  df_c <- open_dataset(paste0(fp_input_base, "/sex_id=1/rx_part_c_part0.parquet")) %>% collect() %>% as.data.frame()
  df_no_c <- open_dataset(paste0(fp_input_base, "/sex_id=1/rx_part0.parquet")) %>% collect() %>% as.data.frame()
} else { 
  # Read in args from SUBMIT_ARRAY_JOBS(), read in .csv containing permutations, subset to row based on task_id
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input_full_directories <- args[1] # fp_parameters_input_full_directories <- fp_parameters_full
  fp_parameters_input_unique <- args[2] # fp_parameters_input_unique <- fp_parameters_unique
  
  # use Task id to identify params for this run (NEED TO EDIT THIS SECTION THROUGH fp_input TO PROPERLY RUN NON-INTERACTIVELY)
  # array_job_number <- 3 #comment out when done
  array_job_number <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  print(paste0("job number:", array_job_number))
  df_parameters_full <- as.data.table(fread(fp_parameters_input_full_directories)) 
  df_parameters_unique <- as.data.table(fread(fp_parameters_input_unique)) 
  
  # subset the df with full directories to the directories of interest
  subset_year_id <- df_parameters_unique[array_job_number, year_id]
  subset_age_group <- df_parameters_unique[array_job_number, age_group_years_start]
  df_parameters_subset <- df_parameters_full %>% filter(year_id == subset_year_id) %>% filter(age_group_years_start == subset_age_group)
  
  # set parameters used in output
  input_run_id <- df_parameters_subset$runid[1]
  input_toc <- "RX"
  input_year_id <- subset_year_id
  input_code_system <- "icd10"
  input_age_group_years_start <- subset_age_group
  
  # create list of male & female directory list and read in each collection of folders to label the correct sex_id for the datasets
  df_list_males <- list(df_parameters_subset %>% filter(sex_id == 1) %>% select(directory)) %>% unlist()
  df_list_females <- list(df_parameters_subset %>% filter(sex_id == 2) %>% select(directory)) %>% unlist()
  
  # make list of parquet files to open
  parquet_files_males <- dir_ls(df_list_males, recurse = TRUE, glob = "*.parquet")
  parquet_files_females <- dir_ls(df_list_females, recurse = TRUE, glob = "*.parquet")
  
  # load data
  df_input_males <- open_dataset(parquet_files_males) %>%
    collect() %>%
    as.data.frame()
  
  df_input_females <- open_dataset(parquet_files_females) %>%
    collect() %>%
    as.data.frame()
  
  # remove unnecessary columns
  desired_columns <- c("acause", "tot_chg_amt", "tot_pay_amt", "race_cd", "bene_id", "claim_id", "primary_cause")
  df_input_males <- df_input_males %>% select(all_of(desired_columns))
  df_input_females <- df_input_females %>% select(all_of(desired_columns))
  
  # add sex_id columns
  df_input_males$sex_id <- 1
  df_input_females$sex_id <- 2
  
  # combine dataframes
  df_input <- bind_rows(df_input_males, df_input_females)
  
  # drop individual male / female dataframes
  rm(df_input_males)
  rm(df_input_females)
}

##----------------------------------------------------------------
## 1. create dir foldes 
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Define subfolders for output types
summary_stats_folder <- file.path(output_folder, "01.RX_Summary_Statistics")
regression_estimates_folder <- file.path(output_folder, "02.RX_Regression_Estimates")

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
log_folder <- file.path(summary_stats_folder, "RX_logs")
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
         "_ICD10",# Hardcode for RX
         "_age", input_age_group_years_start, extension)
}


####--------------------------------------------------------------------
####  Create HIV flag and preprocess input data
####--------------------------------------------------------------------


# Create HIV flag by identifying beneficiaries with at least one HIV-coded diagnosis
hiv_beneficiaries <- df_input %>%
  filter(acause == "hiv") %>%
  distinct(bene_id) %>%
  mutate(hiv_flag = 1)

#colnames(df)

# Note, encounter_id is swapped with claim_id and tot_chg_amt added
#Subset relevant variables and merge HIV flag 
df <- df_input %>%
  select(bene_id, claim_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt, tot_chg_amt) %>%
  left_join(hiv_beneficiaries, by = "bene_id") %>%
  mutate(
    hiv_flag = if_else(is.na(hiv_flag), 0L, hiv_flag),
    race_cd = as.character(race_cd)
  ) %>%
  filter(race_cd %in% c("WHT", "BLCK", "HISP"))

##----------------------------------------------------------------
## Collapse detailed causes into 3 analytic categories
##----------------------------------------------------------------
df <- df %>%
  mutate(acause = case_when(
    grepl("^cvd_", acause) | acause %in% c(
      "diabetes_typ2", "ckd", "renal_failure", "cirrhosis", "hepatitis_c", "rf_obesity"
    ) ~ "Cardiometabolic",
    grepl("^mental_", acause) | acause == "rf_tobacco" ~ "SUD_mental",
    TRUE ~ "other"
  ))

##----------------------------------------------------------------
## Promote mental health-related rows to "primary" (temporarily set to 3)
##----------------------------------------------------------------
df <- df %>%mutate(primary_cause = if_else(acause == "SUD_mental", 3L, primary_cause))


# For each encounter, retain only one row, prioritizing SUD_mental and then existing primary cause
df <- df %>%
  group_by(claim_id) %>%
  arrange(desc(primary_cause == 3), desc(primary_cause == 1)) %>%
  slice(1) %>%
  ungroup() %>%
  mutate(primary_cause = if_else(primary_cause == 3L, 1L, primary_cause))

# Tag remaining rows as one encounter each for counting purposes
df <- df %>%mutate(encounter_flag = 1L)

#colnames(df)


# Collapse dataset to beneficiary × disease category level
df <- df %>%
  group_by(bene_id, acause) %>%
  summarise(
    race_cd = first(race_cd),
    sex_id = first(sex_id),
    hiv_flag = first(hiv_flag),
    number_of_encounters = sum(encounter_flag),
    tot_chg_amt = sum(tot_chg_amt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(any_cost = if_else(tot_chg_amt > 0, 1, 0))

# Compute total number of encounters per beneficiary (all causes, full dataset)
total_encounters_per_bene <- df_input %>%
  group_by(bene_id) %>%
  summarise(total_encounters = n_distinct(claim_id), .groups = "drop")

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
  code_system = "icd10",
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
    avg_cost_per_bene = mean(tot_chg_amt, na.rm = TRUE),
    max_cost_per_bene = max(tot_chg_amt, na.rm = TRUE),
    quant99 = quantile(tot_chg_amt, probs = 0.99, na.rm = TRUE),
    total_cost = sum(tot_chg_amt, na.rm = TRUE),
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
    code_system = "ICD10",
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
  tot_chg_amt ~ acause*hiv_flag + race_cd + sex_id,
  data = filter(df, tot_chg_amt > 0),
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
