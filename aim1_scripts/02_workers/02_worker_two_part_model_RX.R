##----------------------------------------------------------------
##' Title: 02_worker_two_part_model_RX.R
##'
##' Purpose:
##' This script runs a two-part regression model for Medicare pharmaceutical (RX) expenditures 
##' as part of the Aim1 analysis. It estimates the cost of care for people with HIV in a given year, 
##' broken down by disease, race, sex, and other demographic factors.
##' 
##' The script performs the following:
##'  1. Reads and preprocesses pharmaceutical claims data.
##'  2. Merges patient-level attributes (race, sex, HIV status).
##'  3. Runs a two-part regression model:
##'     - Logistic regression to estimate probability of any cost.
##'     - Gamma regression to estimate positive healthcare expenditures.
##'  4. Computes marginal costs per disease, stratified by HIV status and race/sex.
##'  5. Saves results, including regression outputs, marginal cost estimates, and warnings.
##'
##' Inputs:
##'  - Medicare RX claims dataset (ICD-10 coded, year- and state-specific).
##'  - CSV files containing input parameters (`parameters_pharm.csv`, `parameters_pharm_unique.csv`).
##'
##' Outputs:
##'  - CSV file (`two_part_model_results.csv`) with per-beneficiary cost estimates.
##'  - Model summary logs (`logit_model_summary.txt`, `gamma_model_summary.txt`).
##'  - Warning logs (`marginal_analysis_warnings.txt`).
##'  - All outputs are saved inside a date-stamped directory.
##'
##' Author: Bulat Idrisov
##' Date: 2024-03-06
##' Version: 1.0
##'
##----------------------------------------------------------------


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

# 1. Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)
#log_folder <- file.path(output_folder, "Regression_Estimates")
two_part_results_folder <- file.path(output_folder, "03.Rx_Two-Part_Results")

ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

ensure_dir_exists(output_folder)
ensure_dir_exists(two_part_results_folder)


# Function to generate consistent filenames with metadata
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_run", input_run_id, 
         "_toc", input_toc, 
         "_year", input_year_id, 
         "_ICD10",# Hardcode for RX
         "_age", input_age_group_years_start, extension)
}

##----------------------------------------------------------------
## Identify HIV-positive beneficiaries based on presence of any HIV-coded diagnosis
##----------------------------------------------------------------

hiv_beneficiaries <- df_input %>%
  filter(acause == "hiv") %>%
  distinct(bene_id) %>%
  mutate(hiv_flag = 1)

if (nrow(hiv_beneficiaries) == 0) {
  print("hiv_beneficiaries has no data for this run, stopping.")
  stop() # Stop execution if no HIV+ individuals found
}

# Check if HIV sample size is small
hiv_n <- nrow(hiv_beneficiaries)
model_warning <- ifelse(hiv_n < 10, TRUE, FALSE)


##----------------------------------------------------------------
## Keep relevant variables and limit to analytic subpopulation (WHT, BLCK, HISP)
##----------------------------------------------------------------

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



##----------------------------------------------------------------
## Collapse to beneficiary-disease category level
## Aggregate total spending and record demographics and HIV flag
##----------------------------------------------------------------
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

##----------------------------------------------------------------
## Merge total encounter count into analytic dataset
## This reflects overall healthcare utilization and can be used for stratification or adjustment
##----------------------------------------------------------------

df <- df %>%left_join(total_encounters_per_bene, by = "bene_id")

##----------------------------------------------------------------
## Convert key variables to factors for modeling purposes
##----------------------------------------------------------------
df <- df %>%
  mutate(
    acause  = factor(acause),
    race_cd = factor(race_cd),
    sex_id  = factor(sex_id),         # or numeric if you prefer
    hiv_flag= factor(hiv_flag, levels=c(0,1))  # force 2-level factor
  )

##----------------------------------------------------------------
## At this point, the dataset is structured at the beneficiary × disease level
## with one row per comorbidity per person, including demographics, spending,
## disease category, HIV status, and total healthcare utilization (total_encounters).
##----------------------------------------------------------------

##----------------------------------------------------------------
## Build bin distribution from the full data
##----------------------------------------------------------------
df_bins_master <- df %>%
  group_by(acause, race_cd, sex_id) %>%
  summarise(bin_count = n(), .groups="drop") %>%
  group_by(acause, race_cd) %>%
  mutate(prop_bin = bin_count / sum(bin_count, na.rm = TRUE)) %>%
  ungroup()

grid_input_master <- expand.grid(
  acause   = levels(df$acause),
  race_cd  = levels(df$race_cd),
  sex_id   = levels(df$sex_id),
  hiv_flag = levels(df$hiv_flag),
  KEEP.OUT.ATTRS=FALSE
) %>%
  as.data.frame() %>%
  left_join(df_bins_master, by = c("acause","race_cd","sex_id")) %>%
  mutate(
    bin_count = if_else(is.na(bin_count), 0L, bin_count),
    prop_bin  = if_else(is.na(prop_bin),  0,  prop_bin)
  )

##----------------------------------------------------------------
## BOOTSTRAP
##----------------------------------------------------------------
B <- 100
set.seed(123)

all_boot_list <- list()

for (b in seq_len(B)) {
  cat("Bootstrap iteration:", b, "/", B, "\n")
  
  # Resample
  df_boot <- df %>% sample_frac(size=1, replace=TRUE) %>%
    mutate(
      acause  = factor(acause, levels=levels(df$acause)),
      race_cd = factor(race_cd, levels=levels(df$race_cd)),
      sex_id  = factor(sex_id, levels=levels(df$sex_id)),
      hiv_flag= factor(hiv_flag, levels=levels(df$hiv_flag))
    )
  
  # logistic regression
  mod_logit <- glm(
    any_cost ~ acause*hiv_flag + race_cd + sex_id,
    data=df_boot,
    family=binomial(link="logit")
  )
  
  # gamma regression for positive costs
  mod_gamma <- glm(
    tot_chg_amt ~ acause*hiv_flag + race_cd + sex_id,
    data=filter(df_boot, tot_chg_amt>0),
    family=Gamma(link="log")
  )
  
  # predictions
  tmp <- grid_input_master %>%
    mutate(
      prob_any_cost = predict(mod_logit, newdata=., type="response"),
      cost_if_pos   = predict(mod_gamma, newdata=., type="response"),
      exp_cost      = prob_any_cost * cost_if_pos
    )
  
  # summarise bootstrap iteration
  out_b <- tmp %>%
    group_by(acause, race_cd, hiv_flag) %>%
    summarise(
      cost_est  = sum(exp_cost * prop_bin, na.rm=TRUE),
      .groups="drop"
    ) %>%
    pivot_wider(
      id_cols=c(acause,race_cd),
      names_from=hiv_flag,
      values_from=cost_est,
      names_prefix="cost_hiv_"
    ) %>%
    mutate(
      cost_delta     = cost_hiv_1 - cost_hiv_0,
      bootstrap_iter = b
    )
  
  all_boot_list[[b]] <- out_b
}

boot_combined <- bind_rows(all_boot_list)

# bin summary (outside loop)
df_bins_summary <- grid_input_master %>%
  group_by(acause, race_cd) %>%
  summarise(avg_bin_count = sum(bin_count, na.rm=TRUE), .groups="drop")

# Summaries
df_summary <- boot_combined %>%
  group_by(acause, race_cd) %>%
  summarise(
    mean_cost_hiv_0 = mean(cost_hiv_0, na.rm=TRUE),
    lower_ci_cost_hiv_0 = quantile(cost_hiv_0, 0.025),
    upper_ci_cost_hiv_0 = quantile(cost_hiv_0, 0.975),
    
    mean_cost_hiv_1 = mean(cost_hiv_1, na.rm=TRUE),
    lower_ci_cost_hiv_1 = quantile(cost_hiv_1, 0.025),
    upper_ci_cost_hiv_1 = quantile(cost_hiv_1, 0.975),
    
    mean_cost_delta = mean(cost_delta, na.rm=TRUE),
    lower_ci_delta  = quantile(cost_delta, 0.025),
    upper_ci_delta  = quantile(cost_delta, 0.975),
    .groups="drop"
  ) %>%
  left_join(df_bins_summary, by=c("acause","race_cd"))

df_all_race <- df_summary %>%
  group_by(acause) %>%
  summarise(
    race_cd = "all_race",
    
    mean_cost_hiv_0 = sum(mean_cost_hiv_0 * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    lower_ci_cost_hiv_0 = sum(lower_ci_cost_hiv_0 * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    upper_ci_cost_hiv_0 = sum(upper_ci_cost_hiv_0 * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    
    mean_cost_hiv_1 = sum(mean_cost_hiv_1 * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    lower_ci_cost_hiv_1 = sum(lower_ci_cost_hiv_1 * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    upper_ci_cost_hiv_1 = sum(upper_ci_cost_hiv_1 * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    
    mean_cost_delta = sum(mean_cost_delta * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    lower_ci_delta   = sum(lower_ci_delta * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    upper_ci_delta   = sum(upper_ci_delta * avg_bin_count, na.rm=TRUE) /
      sum(avg_bin_count, na.rm=TRUE),
    
    avg_bin_count = sum(avg_bin_count, na.rm=TRUE),  # total bin count
    .groups = "drop"
  )

# Now bind that row with your existing per-race rows:
df_summary <- bind_rows(df_summary, df_all_race)


# final metadata and file output
df_summary2 <- df_summary %>%
  mutate(
    run_id        = input_run_id,
    toc           = input_toc,
    year_id       = input_year_id,
    code_system   = "ICD10",
    age_group     = input_age_group_years_start,
    model_warning = model_warning
  )

file_out <- generate_filename("rx_bootstrap_marginal_results", ".csv")
out_path <- file.path(two_part_results_folder, file_out)
write.csv(df_summary2, out_path, row.names=FALSE)
cat("Wrote final CSV to:", out_path, "\n")
