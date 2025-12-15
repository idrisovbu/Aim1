##----------------------------------------------------------------
## Title: B2_worker_cause_model.R
##
## Purpose:
##   Run a cause-specific two-part cost model (logit + gamma) with bootstrap
##   resampling, standardize predictions over (race, sex, age) bins, and write
##   per-cause results to disk. This script is designed to be called either
##   sequentially (looping over causes) or from a SLURM array where each task
##   handles one cause.
##
## What the script does (high level):
##   1) For a given acause_lvl2 (“cause”), filter the data to that cause.
##   2) Build standardization bins over (race_cd, sex_id, age_group_years_start)
##      and compute weights (prop_bin) within each (race, age) to collapse over sex later.
##   3) For B bootstrap iterations:
##        - Resample rows with replacement.
##        - Fit Part 1 (logit): has_cost ~ has_hiv + has_sud + race_cd + sex_id
##        - Fit Part 2 (gamma with log link): tot_pay_amt ~ has_hiv + has_sud + race_cd + sex_id
##          on positive costs (truncated at 99.5% to reduce outlier influence).
##        - Predict expected cost for all combinations of (has_hiv ∈ {0,1}, has_sud ∈ {0,1})
##          across the standardized bin grid.
##        - Standardize by weighting predictions with prop_bin and collapse over sex.
##        - Save each iteration’s results (parquet) under the cause’s boot_chunks folder.
##   4) Combine bootstrap iterations and compute means and 95% quantile CIs for:
##        cost_neither, cost_hiv_only, cost_sud_only, cost_hiv_sud,
##        and their deltas vs. cost_neither.
##   5) Write the per-cause summary CSV under the cause’s results folder.
##
## Inputs (expected in environment or passed in):
##   - df: table with columns acause_lvl2, race_cd, sex_id, has_hiv, has_sud,
##         has_cost, tot_pay_amt, age_group_years_start.
##   - file_type: "F2T" or "RX" (affects B selection and filenames).
##   - year_id: analysis year.
##   - age_group_years_start: starting age of the analysis group.
##   - bootstrap_iterations_F2T / bootstrap_iterations_RX: integers for B.
##
## Outputs (date-stamped run directory):
##   /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/
##     04.Two_Part_Estimates/<YYYYMMDD>/by_cause/<cause_slug>/
##       ├── boot_chunks/          # parquet files per bootstrap iteration
##       └── results/
##           └── bootstrap_marginal_results_<meta>.csv
##
## Notes:
##   - Iterations with no variation in has_hiv or has_sud are skipped.
##   - If all iterations are skipped for a cause, summary is not written.
##   - For cluster reproducibility, consider seeding per cause/array task.
##----------------------------------------------------------------


##----------------------------------------------------------------
# 0 Clear environment and set library paths
##----------------------------------------------------------------
rm(list = ls())
pacman::p_load(arrow, dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,glmnet, broom)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))

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

##---------------------------
## 0.1 Read args / data
##---------------------------
if (interactive()) {
  # Parameters file
  fp_parameters_input <- "/ihme/limited_use//LU_CMS/DEX/hivsud/aim1/resources_aim1//B1_two_part_model_parameters_aim1_BY_CAUSE.csv"
  df_params <- read.csv(fp_parameters_input)
  
  a <- 27
  year_id <- df_params$year_id[a]
  cause_name <- df_params$cause_name[a]
  bootstrap_iterations <- 2
  model_type <- "has_all"
  counterfactual_0 <- F
  
  # List out all F2T & RX filepaths by age group
  list_files <- unlist(as.list(df_params[a, 3:ncol(df_params)]))

} else {
  args <- commandArgs(trailingOnly = TRUE)
  
  array_job_number <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message("SLURM Job ID: ", array_job_number)
  
  fp_parameters_input <- args[1]
  df_params <- fread(fp_parameters_input)
  df_job <- df_params[array_job_number, ]
 
  year_id     <- df_job$year_id
  cause_name  <- df_job$cause_name 
  bootstrap_iterations  <- as.integer(args[2])
  model_type <- args[3]
  counterfactual_0 <- args[4]
  
  # List out all F2T & RX filepaths by age group
  list_files <- unlist(as.list(df_params[array_job_number, 3:ncol(df_params)]))
}

##---------------------------
## 1) Functions / Helpers
##---------------------------
ensure_dir_exists <- function(d) if (!dir.exists(d)) dir.create(d, recursive = TRUE)

generate_filename <- function(prefix, extension, cause = NULL) {
  cpart <- if (is.null(cause)) "" else paste0("_", slugify(cause))
  paste0(prefix, cpart, "_year", year_id, extension)
}

slugify <- function(x) {
  x <- gsub("\\s+", "_", x, perl = TRUE)
  gsub("[^A-Za-z0-9._-]", "_", x, perl = TRUE)
}

make_cause_dirs <- function(cause_name) {
  cause_slug <- slugify(cause_name)
  boot_dir   <- file.path(by_cause_boot_parent, cause_slug, year_id)
  regression_coefficients_dir   <- file.path(by_cause_regression_coefficients, cause_slug)
  res_dir    <- file.path(by_cause_results_parent, cause_slug)
  ensure_dir_exists(boot_dir)
  ensure_dir_exists(res_dir)
  ensure_dir_exists(regression_coefficients_dir)
  list(
    cause_slug  = cause_slug,
    boot_dir    = boot_dir,
    results_dir = res_dir,
    regression_dir = regression_coefficients_dir
  )
}

# Extract coefficients and p-values (used to save regression outputs)
extract_all_coefs <- function(model, suffix) {
  tidy(model) %>%
    filter(term != "(Intercept)") %>%
    rename(
      variable = term,
      !!paste0("estimate_", suffix) := estimate,
      !!paste0("p_", suffix) := p.value
    ) %>%
    select(variable, starts_with("estimate_"), starts_with("p_"))
}

# Coerces "has_" columns to factor, with levels 0 and 1
coerce_has_to_factor <- function(df) {
  # identify columns starting with "has_"
  has_vars <- grep("^has_", names(df), value = TRUE)
  
  for (v in has_vars) {
    # coerce to factor with explicit levels 0 and 1
    df[[v]] <- factor(df[[v]], levels = c(0, 1))
  }
  
  return(df)
}

# Helper: predict on bootstrap sample holding everything (incl. cause_count) as observed,
# but forcing (has_hiv, has_sud) to a given scenario. 
predict_scenario <- function(dat, hiv_level, sud_level, model, nm, set_0) {
  #' @dat Input datatable (df)
  #' @hiv_level Either 0 or 1 (str)
  #' @sud_level Either 0 or 1 (str)
  #' @model Model object
  #' @nm Name for prediction column (str)
  #' @set_0 Whether or not to set has_* variables to all 0, or to preserve as-is (bool)
  tmp <- copy(dat)
  tmp[, has_hiv := factor(hiv_level, levels = levels(dat$has_hiv))]
  tmp[, has_sud := factor(sud_level, levels = levels(dat$has_sud))]
  
  if (set_0) {
    # Set all "has_" level 2 diseases to 0 to isolate just the input disease cost
    has_cols_0 <- c("has_hepc", "has_cvd", "has__otherncd", "has_digest", "has__mental",
                    "has_msk", "has_skin", "has__sense", "has__well", "has_nutrition",
                    "has_diab_ckd", "has__neuro", "has__rf", "has_resp", "has__ri",
                    "has__neo", "has__infect", "has_inj_trans", "has__unintent",
                    "has__intent", "has__enteric_all", "has_std", "has__ntd", "has_mater_neonat")
    for (col in has_cols_0) {
      tmp[[col]] <- factor("0", levels = c("0", "1"))
    }
  }
  
  tmp[, pred_cost := predict(model, newdata = tmp, type = "response")]
  out <- tmp[, .(val = mean(pred_cost, na.rm = TRUE)),
             by = .(race_cd, sex_id, age_group_years_start)]
  setnames(out, "val", nm)
  out
}

##---------------------------
## 1.1) Set Directories
##---------------------------
# outputs
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder     <- format(Sys.time(), "%Y%m%d")
output_folder   <- file.path(base_output_dir, "04.Two_Part_Estimates", date_folder)

# counterfactual_0 string, modifies folder name depending if T or F
counterfactual_string <- "has_1"
if (counterfactual_0) {
  counterfactual_string <- "has_0"
}

# parent branches under by_cause
by_cause_root          <- file.path(output_folder, paste0("by_cause_", counterfactual_string))

# boot_chunks is by cause, year, then age group (e.g. boot_chunks/_enteric_all/2015/65/chunk1.parquet)
by_cause_boot_parent   <- file.path(by_cause_root, "boot_chunks")
by_cause_results_parent<- file.path(by_cause_root, "results")
by_cause_regression_coefficients <- file.path(base_output_dir, "02.Regression_Estimates", date_folder, paste0("estimates_", counterfactual_string))

ensure_dir_exists(output_folder)
ensure_dir_exists(by_cause_root)
ensure_dir_exists(by_cause_boot_parent)
ensure_dir_exists(by_cause_results_parent)
ensure_dir_exists(by_cause_regression_coefficients)

##---------------------------
## 1.2) Read in data
##---------------------------

# Make empty list to store dfs as we read them in
list_df <- list()

# Loop through list_files and read in necessary data, filter by cause, then store in list_df
for (i in 1:length(list_files)) {
  
  fp <- list_files[i]
  
  # read in dataset
  dt <- open_dataset(fp) %>%
    filter(acause_lvl2 == cause_name) %>%
    collect() %>%
    as.data.frame()
  
  # remove "st_resi" column if it exists
  if ("st_resi" %in% names(dt)) {
    dt <- dt %>% select(-c("st_resi"))
  }
  
  # Add to df list
  list_df[[i]] <- dt
  
  # cleanup
  rm(dt)
}

# Combine dataframes into one cause df, all age groups
df <- do.call(rbind, list_df)

##---------------------------
## 1.3) Ensure "has_" column values are correct for each beneficiary
##
## Sometimes F2T and RX will have differences in which causes are present
## resulting in differences in which "has_" cause columns are set to 1 or 0
## This section fixes that by ensuring that beneficiaries have the same set of "has_" column values
## by taking the maximum value between the F2T and RX data
##---------------------------
# make list of "has_" columns
has_cols <- grep(pattern = "has_", x = colnames(df), value = TRUE)

has_cols <- has_cols[-4] # remove "has_cost"

# make df with max value of all "has_" columns, grouped by "bene_id"
df_bene_binary_values <- df %>%
  group_by(bene_id) %>%
  summarise(across(all_of(has_cols), max, .names = "{.col}"), .groups = "drop")

# add "cause_count" column (count excludes hiv, sud, hepc)
has_cols_nohivsudhepc <- has_cols[4:length(has_cols)] # removes hiv, sud, hepc

df_bene_binary_values <- df_bene_binary_values %>%
  mutate(cause_count = rowSums(across(all_of(has_cols_nohivsudhepc))))

# Drop "has_" columns (excl. has_cost & has_hepc), join with df_bene_binary_values for most accurate "has_" columns
df <- df %>% 
  select(-c(all_of(has_cols), cause_count)) %>%
  left_join(y = df_bene_binary_values, by = "bene_id")

# Set to DT
df <- setDT(df)

##---------------------------
## 2) Coerce factors
##---------------------------

# Coerce "has_" columns to factors w/ level 0 and 1
df <- coerce_has_to_factor(df)

# Coerce additional columns to factors
df[, `:=`(
  acause_lvl2 = factor(acause_lvl2),
  race_cd     = factor(race_cd),
  sex_id      = factor(sex_id),
  toc         = factor(toc),
  age_group_years_start = factor(age_group_years_start)
)]

##---------------------------
## 3) Run Bootstrap
##---------------------------

# --- Setup for a single cause run -------------------------------
cat("\n=== Running cause:", cause_name, "===\n")

# --- Set directories
paths <- make_cause_dirs(cause_name) 
bootstrap_chunks_output_folder                 <- paths$boot_dir
results_output_folder                          <- paths$results_dir
regression_coefficients_output_folder          <- paths$regression_dir

# Clean old chunks
old_files <- list.files(bootstrap_chunks_output_folder, pattern = "\\.parquet$", full.names = TRUE)
if (length(old_files) > 0L) unlink(old_files, force = TRUE)

# --- Build weight bins -------------------------------------------------------------
sex_weights <- df %>%
  group_by(race_cd, sex_id, age_group_years_start) %>%
  summarise(row_count = n(), .groups = "drop") %>%
  group_by(race_cd, age_group_years_start) %>%
  mutate(prop_bin = row_count / sum(row_count)) %>%
  ungroup() %>%
  as.data.table()

# Collapsed-over-sex total row count to merge into summary later
df_row_count_metadata <- sex_weights %>%
  group_by(race_cd, age_group_years_start) %>%
  summarise(total_row_count = sum(row_count, na.rm = TRUE), .groups = "drop") %>%
  mutate(acause_lvl2 = as.character(cause_name)) %>%
  select(acause_lvl2, race_cd, age_group_years_start, total_row_count)

# Run Winsorization at %
df[, tot_pay_amt := pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))]

# --- Create DF to store regression outputs ----------------------------------
list_regression <- list()

# --- Bootstrap --------------------------------------------------------------
# Set seed
set.seed(123)

# Loop through bootstrap iterations
kept_iters <- 0L
for (b in seq_len(bootstrap_iterations)) {
  cat("[", cause_name, "] Bootstrap iteration:", b, "/", bootstrap_iterations, "\n", sep = "")
  
  # Perform stratified sampling - this ensures that in the "by_keys" variable below, we are sampling within that
  # particular strata, so we don't end up with samples that have none of the more rarer cases, causing bootstrapping to fail
  
  # Choose stratification keys (may need to change how we decide boot strapped stratification)
  by_keys <- c("sex_id", "has_hiv", "has_sud")
  
  # One stratified resample (size-preserving within each stratum)
  df_boot <- df[, .SD[sample(.N, .N, replace = TRUE)], by = by_keys]
  
  # Factor Check - this checks the count of the number of levels (i.e. are 0 AND 1 present)
  if (nlevels(droplevels(df_boot$has_hiv)) < 2 ||
      nlevels(droplevels(df_boot$has_sud)) < 2) {
    cat("[", cause_name, "] Skip iter ", b, " - insufficient HIV/SUD variation\n", sep = "")
    next
  }
  
  # Model Type
  if (model_type == "has_all") {
    has_vars <- grep("^has_", names(df_boot), value = TRUE)
    has_vars <- has_vars[4:length(has_vars)] # removes has_hiv, has_sud, has_cost
    has_vars <- has_vars[!grepl(paste0("has_*", cause_name), has_vars)] # removes current cause_name
    
    rhs_gamma_formula <- paste(
      c("has_hiv * has_sud", "race_cd", "sex_id", "age_group_years_start", has_vars),
      collapse = " + "
    )

  } else if (model_type == "topk") {
    top_k_cause_list <- c(
      "has__rf","has_cvd","has__otherncd","has_diab_ckd","has_msk","has__neo","has_digest","has_resp",
      "has_skin","has__ri","has__sense","has__well","has__neuro","has__mental","has__unintent"
    )
    filtered_top_k_cause_list <- top_k_cause_list[!grepl(paste0("has_*", cause_name), top_k_cause_list)] # removes current cause_name
    rhs_gamma_formula <- paste(
      c("has_hiv * has_sud", "race_cd", "sex_id + age_group_years_start + cause_count_minus_top_k", filtered_top_k_cause_list),
      collapse = " + "
    )
  } else if (model_type == "cause_count") {
    rhs_gamma_formula <- c("has_hiv * has_sud + race_cd + sex_id + age_group_years_start + cause_count")
  }
  
  gamma_formula <- as.formula(paste(
    "tot_pay_amt ~ ", rhs_gamma_formula
  ))

  # Gamma Model - Gamma on positive costs (truncate 99.5%)
  df_gamma_input <- df_boot[tot_pay_amt > 0]
  
  if (model_type == "topk") { # Creates cause_count_minus_top_k just for this model type
    df_gamma_input[, cause_count_minus_top_k :=
                     cause_count - rowSums(as.data.frame(lapply(.SD, function(x) as.numeric(as.character(x))))),
                   .SDcols = filtered_top_k_cause_list] # This creates the cause_count_minus_top_k amount
  }

  if (nrow(df_gamma_input) < 10) {
    cat("[", cause_name, "] Skip iter ", b, " - too few positive costs\n", sep = ""); next
  }
  
  mod_gamma <- try(glm(
    gamma_formula,
    data = df_gamma_input, family = Gamma(link = "log"),
    control = glm.control(maxit = 100)
  ), silent = TRUE)
  if (inherits(mod_gamma, "try-error")) {
    cat("[", cause_name, "] Skip iter ", b, " - gamma failed\n", sep = ""); next
  }
  
  # Counterfactual testing - Create four scenarios on the bootstrap sample and make model predictions × {HIV(0,1) × SUD(0,1)}
  sc00 <- predict_scenario(df_gamma_input, "0", "0", mod_gamma, "cost_neither", counterfactual_0)
  sc10 <- predict_scenario(df_gamma_input, "1", "0", mod_gamma, "cost_hiv_only", counterfactual_0)
  sc01 <- predict_scenario(df_gamma_input, "0", "1", mod_gamma, "cost_sud_only", counterfactual_0)
  sc11 <- predict_scenario(df_gamma_input, "1", "1", mod_gamma, "cost_hiv_sud", counterfactual_0)
  
  # Merge sex_weights with our counterfactual scenario predictions
  df_sc <- merge(sex_weights, sc00)
  df_sc <- merge(df_sc, sc10)
  df_sc <- merge(df_sc, sc01)
  df_sc <- merge(df_sc, sc11)
  
  # Add cause name
  df_sc$acause_lvl2 <- cause_name
  
  # Multiply the prop_bin (sex weight) by each of the predicted values so we can group by summary to collapse
  for (col in c("cost_neither", "cost_hiv_only", "cost_sud_only", "cost_hiv_sud")) {
    df_sc[[col]] <- df_sc[[col]] * df_sc[["prop_bin"]]
  }
  
  # Group by summary, collapse on sex_id to create final bootstrap predictions
  out_b <- copy(df_sc)
  out_b <- out_b[
    , .(
      cost_neither    = sum(cost_neither,   na.rm = TRUE),
      cost_hiv_only   = sum(cost_hiv_only,  na.rm = TRUE),
      cost_sud_only   = sum(cost_sud_only,  na.rm = TRUE),
      cost_hiv_sud    = sum(cost_hiv_sud,   na.rm = TRUE)
    ),
    by = .(acause_lvl2, race_cd, age_group_years_start)
  ][
    , `:=`(
      delta_hiv_only = cost_hiv_only - cost_neither,
      delta_sud_only = cost_sud_only - cost_neither,
      delta_hiv_sud  = cost_hiv_sud  - cost_neither,
      bootstrap_iter = b
    )
  ]

  # Size checking (Skips bootstrap if detecting tiny or enormous values that don't seem realistic)
  cols_to_check <- c("cost_neither", "cost_sud_only", "cost_hiv_only", "cost_hiv_sud")
  skip_to_next <- FALSE
  
  # Check when columns have tiny values
  for (col in cols_to_check) {
    if (any(out_b[[col]] < 1, na.rm = TRUE)) {
      message("Found values of ", col, " < 1.")
      message("Skipping Bootstrap iteration: ", b)
      skip_to_next <- TRUE
      break # jumps to the next bootstrap iteration
    }
  }
  
  # Check when columns have enormous values
  for (col in cols_to_check) {
    if (any(out_b[[col]] > 1000000, na.rm = TRUE)) {
      message("Found values of ", col, " > 1,000,000.")
      message("Skipping Bootstrap iteration: ", b)
      skip_to_next <- TRUE
      break  # jumps to the next bootstrap iteration
    }
  }
  
  # Skip to next iteration
  if (skip_to_next) {
    next
  }
  
  ##----------------------------------------------------------------
  ## Write parquet file with bootstrapped results
  ##----------------------------------------------------------------
  final_fp <- file.path(bootstrap_chunks_output_folder, sprintf("bootstrap_iter_%03d.parquet", b))
  tmp_fp   <- paste0(final_fp, ".tmp")
  ok <- FALSE
  try({
    arrow::write_parquet(out_b, tmp_fp)
    info <- file.info(tmp_fp)
    if (is.na(info$size) || info$size == 0) stop("temporary parquet size is 0 bytes")
    invisible(arrow::read_parquet(tmp_fp, as_data_frame = TRUE, col_select = 1))
    ok <- file.rename(tmp_fp, final_fp)
  }, silent = TRUE)
  if (!ok) {
    if (file.exists(tmp_fp)) unlink(tmp_fp)
    cat("[", cause_name, "] Skip iter ", b, " - failed to write parquet safely\n", sep = "")
    next
  }
  
  ##----------------------------------------------------------------
  ## Save regression coefficients
  ##----------------------------------------------------------------
  gamma_df <- extract_all_coefs(mod_gamma, "gamma")
  
  # Merge and annotate regression coefficients
  regression_results <- gamma_df %>%
    mutate(
      interaction_dropped = if_else(is.na(estimate_gamma) & str_detect(variable, ":"), TRUE, FALSE),
      year_id = year_id,
    ) %>%
    select(variable, estimate_gamma, p_gamma,
           interaction_dropped, year_id)
  
  # label bootstrap value column
  regression_results$bootstrap_number <- b
  regression_results$cause_name <- cause_name
  
  # Add to regression list
  list_regression[[b]] <- regression_results 
  
  # Add +1 to iterations count
  kept_iters <- kept_iters + 1L
}

# If nothing kept, skip summary gracefully
if (kept_iters == 0L) {
  cat("[", cause_name, "] No valid bootstrap iterations kept. Skipping summary.\n")
} else {
  # Combine only non-empty parquet files
  chunk_files <- list.files(bootstrap_chunks_output_folder, pattern = "\\.parquet$", full.names = TRUE)
  sizes <- file.info(chunk_files)$size
  valid_files <- chunk_files[!is.na(sizes) & sizes > 0]
  if (length(valid_files) == 0L) {
    cat("[", cause_name, "] Parquet chunks are zero bytes. Skipping summary.\n")
  } else {
    boot_combined <- arrow::open_dataset(sources = valid_files) %>% collect()
    
    # Summary + attach total_row_count
    df_summary <- boot_combined %>%
      group_by(acause_lvl2, race_cd, age_group_years_start) %>%
      summarise(
        mean_cost_neither  = mean(cost_neither, na.rm = TRUE),
        mean_cost_hiv_only = mean(cost_hiv_only, na.rm = TRUE),
        mean_cost_sud_only = mean(cost_sud_only, na.rm = TRUE),
        mean_cost_hiv_sud  = mean(cost_hiv_sud,  na.rm = TRUE),
        lower_ci_neither   = quantile(cost_neither, 0.025, na.rm = TRUE),
        upper_ci_neither   = quantile(cost_neither, 0.975, na.rm = TRUE),
        lower_ci_hiv_only  = quantile(cost_hiv_only, 0.025, na.rm = TRUE),
        upper_ci_hiv_only  = quantile(cost_hiv_only, 0.975, na.rm = TRUE),
        lower_ci_sud_only  = quantile(cost_sud_only, 0.025, na.rm = TRUE),
        upper_ci_sud_only  = quantile(cost_sud_only, 0.975, na.rm = TRUE),
        lower_ci_hiv_sud   = quantile(cost_hiv_sud,  0.025, na.rm = TRUE),
        upper_ci_hiv_sud   = quantile(cost_hiv_sud,  0.975, na.rm = TRUE),
        mean_delta_hiv_only = mean(delta_hiv_only, na.rm = TRUE),
        mean_delta_sud_only = mean(delta_sud_only, na.rm = TRUE),
        mean_delta_hiv_sud  = mean(delta_hiv_sud,  na.rm = TRUE),
        lower_ci_delta_hiv_only = quantile(delta_hiv_only, 0.025, na.rm = TRUE),
        upper_ci_delta_hiv_only = quantile(delta_hiv_only, 0.975, na.rm = TRUE),
        lower_ci_delta_sud_only = quantile(delta_sud_only, 0.025, na.rm = TRUE),
        upper_ci_delta_sud_only = quantile(delta_sud_only, 0.975, na.rm = TRUE),
        lower_ci_delta_hiv_sud  = quantile(delta_hiv_sud,  0.025, na.rm = TRUE),
        upper_ci_delta_hiv_sud  = quantile(delta_hiv_sud,  0.975, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      left_join(df_row_count_metadata, by = c("acause_lvl2","race_cd","age_group_years_start")) %>%
      mutate(year_id = year_id) %>%
      rename(
        mean_cost     = mean_cost_neither,
        lower_ci      = lower_ci_neither,
        upper_ci      = upper_ci_neither,
        mean_cost_hiv = mean_cost_hiv_only,
        lower_ci_hiv  = lower_ci_hiv_only,
        upper_ci_hiv  = upper_ci_hiv_only,
        mean_cost_sud = mean_cost_sud_only,
        lower_ci_sud  = lower_ci_sud_only,
        upper_ci_sud  = upper_ci_sud_only
      )
    
    desired_order <- c(
      "acause_lvl2", "race_cd",
      "mean_cost", "lower_ci", "upper_ci",
      "mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv",
      "mean_cost_sud", "lower_ci_sud", "upper_ci_sud",
      "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud",
      "mean_delta_hiv_only", "lower_ci_delta_hiv_only", "upper_ci_delta_hiv_only",
      "mean_delta_sud_only", "lower_ci_delta_sud_only", "upper_ci_delta_sud_only",
      "mean_delta_hiv_sud",  "lower_ci_delta_hiv_sud",  "upper_ci_delta_hiv_sud",
      "total_row_count", "age_group_years_start", "year_id"
    )
    df_summary <- df_summary[, desired_order]
    
    # Write CSV
    final_csv  <- generate_filename("bootstrap_marginal_results", ".csv", cause = cause_name)
    final_path <- file.path(paths$results_dir, final_csv)
    write.csv(df_summary, final_path, row.names = FALSE)
    cat("[", cause_name, "] Wrote final cause summary to:", final_path, "\n", sep = "")
    
    # Combine regression outputs and save as parquet
    df_regression_outputs <- do.call(rbind, list_regression)
    
    file_out_regression <- generate_filename("regression_results", ".parquet", cause = cause_name)
    out_path_regression <- file.path(regression_coefficients_output_folder, file_out_regression)
    write_parquet(df_regression_outputs, out_path_regression)
    cat("✅ Regression coefficients saved to:", out_path_regression, "\n")
  }
}

cat("\nDone. Per-cause results are in:\n", file.path(output_folder, "by_cause"), "\n")
