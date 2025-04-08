
##----------------------------------------------------------------
##' Title: 02_runner_two_part_model_F2T.R
##'
##' Purpose:
##' This script runs a two-part regression model (logistic + gamma) 
##' to estimate the annual cost of care for Medicare beneficiaries 
##' with HIV and comorbidities. Specifically:
##' 
##'  (1) A logistic regression predicts the probability of having 
##'      any healthcare expenditures.
##'  (2) A gamma regression estimates positive expenditures, given 
##'      they are nonzero.
##'
##' The analysis focuses on F2T data (non-pharmaceutical medical costs), 
##' grouped into disease categories. After fitting these models, a 
##' bootstrap procedure is used to quantify uncertainty and compute 
##' confidence intervals for cost estimates (including HIV vs. non-HIV 
##' differences) stratified by disease category, race, and sex.
##'
##' Inputs:
##'   - Claims dataset (filtered by year, disease, and patient attributes)
##'   - Parameter specifications (year, disease categories, etc.)
##'
##' Outputs:
##'   - Bootstrapped two-part model estimates for:
##'       * Disease category
##'       * HIV status
##'       * Race & sex
##'       * 'ALL' race row in the final table
##'   - Summarized differences in cost (HIV vs. non-HIV) 
##'   - Confidence intervals (2.5% and 97.5%) for each cost estimate
##'   - A final CSV with per-beneficiary costs, SEs, and 
##'     CIs for each subgroup
##'
##' Author:      Bulat Idrisov
##' Date:        2025-03-05
##' Version:
##----------------------------------------------------------------


# Clear environment and set library paths
rm(list = ls())
pacman::p_load(arrow, dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,glmnet)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

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

# fp_parameters_input <- "/ihme/limited_use/LU_CMS/DEX/hivsud/aim1/resources_aim1/parameters_aims1.csv"
# array_job_number <- 9

# Determine if script is running interactive or running via job on cluster
if(interactive()) {
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
  
  # use Task id to identify params for this run (NEED TO EDIT THIS SECTION THROUGH fp_input TO PROPERLY RUN NON-INTERACTIVELY)
  # task_id <- 3 #comment out when done
  # df_parameters <- fread(fp_parameters_input)[task_id] # comment out when done
  array_job_number <- Sys.getenv("SLURM_ARRAY_TASK_ID") 
  print(paste0("job number:", array_job_number))
  df_parameters <- as.data.table(fread(fp_parameters_input)) #[job_number] # This line needs to be edited
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

##----------------------------------------------------------------
## 1. create dir foldes 
##----------------------------------------------------------------

# 1. Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)
#log_folder <- file.path(output_folder, "Regression_Estimates")
two_part_results_folder <- file.path(output_folder, "03.Two-Part_Results")

ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

ensure_dir_exists(output_folder)
#ensure_dir_exists(log_folder)
ensure_dir_exists(two_part_results_folder)


# Function to generate consistent filenames with metadata
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_run", input_run_id, 
         "_toc", input_toc, 
         "_year", input_year_id, 
         "_", input_code_system,
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

##----------------------------------------------------------------
## Keep relevant variables and limit to analytic subpopulation (WHT, BLCK, HISP)
##----------------------------------------------------------------
df <- df_input %>%
  select(bene_id, encounter_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt) %>%
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
    grepl("^cvd_", acause) |
      acause %in% c("diabetes_typ2", "ckd", "renal_failure", "cirrhosis", "hepatitis_c", "rf_obesity") ~ "Cardiometabolic",
    grepl("^mental_", acause) | acause == "rf_tobacco" ~ "SUD_mental",
    TRUE ~ "other"
  ))

##----------------------------------------------------------------
## Promote mental health-related rows to "primary" (temporarily set to 3)
##----------------------------------------------------------------
df <- df %>%
  mutate(
    primary_cause = if_else(acause == "SUD_mental", 3L, primary_cause)
  )

##----------------------------------------------------------------
## Retain only one primary row per encounter
## Prioritize SUD_mental (primary_cause = 3), then original primary (primary_cause = 1)
##----------------------------------------------------------------
df <- df %>%
  group_by(encounter_id) %>%
  arrange(desc(primary_cause == 3), desc(primary_cause == 1)) %>% #sort by priority
  slice(1) %>%
  ungroup()

df <- df %>%mutate(primary_cause = if_else(primary_cause == 3L, 1L, primary_cause))

##----------------------------------------------------------------
## Calculate total number of encounters per beneficiary (for later adjustment or stratification)
##----------------------------------------------------------------
total_encounters_per_bene <- df_input %>%
  group_by(bene_id) %>%
  summarise(total_encounters = n_distinct(encounter_id))  # This is overall utilization data

##----------------------------------------------------------------
## Collapse to beneficiary-disease category level
## Aggregate total spending and record demographics and HIV flag
##----------------------------------------------------------------
df <- df %>%
  group_by(bene_id, acause) %>%
  summarise(
    race_cd = first(race_cd),
    sex_id  = first(sex_id),
    hiv_flag = first(hiv_flag),
    tot_pay_amt = sum(tot_pay_amt, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(any_cost = if_else(tot_pay_amt > 0, 1, 0))

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
    tot_pay_amt ~ acause*hiv_flag + race_cd + sex_id,
    data=filter(df_boot, tot_pay_amt>0),
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
    run_id      = input_run_id,
    toc         = input_toc,
    year_id     = input_year_id,
    code_system = input_code_system,
    age_group   = input_age_group_years_start
  )

file_out <- generate_filename("bootstrap_marginal_results", ".csv")
out_path <- file.path(two_part_results_folder, file_out)
write.csv(df_summary2, out_path, row.names=FALSE)
cat("Wrote final CSV to:", out_path, "\n")


