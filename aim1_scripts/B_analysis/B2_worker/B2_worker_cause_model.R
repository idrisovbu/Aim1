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


# Clear environment and set library paths
rm(list = ls())
pacman::p_load(arrow, dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,glmnet)
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

##----------------------------------------------------------------
# 0. Read in data from SLURM job submission
##----------------------------------------------------------------

if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_F2T_data_2010_age85.parquet"
  # df <- read_parquet(path) %>% sample_n(10000) # This loads the whole dataset in and takes a long time
  #df <- open_dataset(path) %>% head(100000) %>% collect() %>% sample_n(10000) # Only reads first 100,000 rows, then samples 10,000, much faster
  df <- open_dataset(path) %>% collect()
  df <- as.data.table(df)  
  year_id <- 2010
  file_type <- "F2T"
  age_group_years_start <- df$age_group_years_start[1]
  bootstrap_iterations_F2T <- 1
  bootstrap_iterations_RX <- 1
  
  
} else {
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input       <- args[1]
  bootstrap_iterations_F2T  <- as.integer(args[2])
  bootstrap_iterations_RX   <- as.integer(args[3])
  
  array_job_number <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message("SLURM Job ID: ", array_job_number)
  
  df_parameters <- fread(fp_parameters_input)
  df_job <- df_parameters[array_job_number, ]
  
  fp_input <- df_job$directory
  file_type <- df_job$file_type
  year_id   <- df_job$year_id
  cause_name <- df_job$cause_name  # <<< NEW: single cause for this task
  
  df <- read_parquet(fp_input) %>% as.data.table()
  age_group_years_start <- df$age_group_years_start[1]
  age_group_years_start <- df$age_group_years_start[1]
}

##----------------------------------------------------------------
## 0.1. Functions
##----------------------------------------------------------------
# Function to generate output filenames based on file type and year
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_", file_type, "_year", year_id, "_age", age_group_years_start, extension)
}

# Utility function to create directories recursively
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

##----------------------------------------------------------------
## 1. Create directory folders 
##----------------------------------------------------------------


# 0.0 tiny sanitizer for folder names
slugify <- function(x) {
  x <- gsub("\\s+", "_", x, perl = TRUE)
  x <- gsub("[^A-Za-z0-9._-]", "_", x, perl = TRUE)
  x
}

# 0.1 generate filenames with your existing metadata
generate_filename <- function(prefix, extension, cause = NULL) {
  cpart <- if (is.null(cause)) "" else paste0("_", slugify(cause))
  paste0(prefix, cpart, "_", file_type, "_year", year_id,
         "_age", age_group_years_start, extension)
}

# 0.2 ensure a directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) dir.create(dir_path, recursive = TRUE)
}

# 0.3 top-level run directories (date-stamped)
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder     <- format(Sys.time(), "%Y%m%d")
output_folder   <- file.path(base_output_dir, "04.Two_Part_Estimates", date_folder)

# 0.4 fixed parents (by-cause parent, logs)
by_cause_parent_folder <- file.path(output_folder, "by_cause")
log_folder             <- file.path(base_output_dir, "logs")

# create parents
ensure_dir_exists(output_folder)
ensure_dir_exists(by_cause_parent_folder)
ensure_dir_exists(log_folder)

# 0.5 per-cause directory builder; returns a list of paths you can use
make_cause_dirs <- function(cause_name) {
  cause_slug <- slugify(cause_name)
  cause_dir  <- file.path(by_cause_parent_folder, cause_slug)
  boot_dir   <- file.path(cause_dir, "boot_chunks")
  res_dir    <- file.path(cause_dir, "results")
  ensure_dir_exists(cause_dir)
  ensure_dir_exists(boot_dir)
  ensure_dir_exists(res_dir)
  list(
    cause_slug  = cause_slug,
    cause_dir   = cause_dir,
    boot_dir    = boot_dir,
    results_dir = res_dir
  )
}


##----------------------------------------------------------------
## 2. Convert key variables to factors for modeling purposes
##----------------------------------------------------------------
df[, `:=`(
  acause_lvl2   = factor(acause_lvl2),
  race_cd       = factor(race_cd),
  sex_id        = factor(sex_id),
  has_hiv       = factor(has_hiv, levels = c(0, 1)),
  has_sud       = factor(has_sud, levels = c(0, 1)),
  has_hepc      = factor(has_hepc, levels = c(0, 1)),
  has_cost      = factor(has_cost, levels = c(0, 1))
)]

##----------------------------------------------------------------
## 3. Filer to one casue 
##----------------------------------------------------------------

#' Run Two-Part Model Bootstrap for a Single Cause
#'
#' @description
#' Fits a two-part cost model for a specific `acause_lvl2` category using
#' bootstrap resampling. Writes intermediate bootstrap iteration results
#' to parquet files and produces a final per-cause summary CSV with means
#' and 95% CIs for each HIV/SUD cost combination and deltas.
#'
#' @details
#' This function:
#' 1. Creates output folders under `/by_cause/<cause>/boot_chunks` and `/results`.
#' 2. Filters the input dataset to the given cause.
#' 3. Generates standardization bins by race, sex, and age group.
#' 4. Runs `B` bootstrap iterations:
#'    - Resample rows with replacement.
#'    - Fit a logistic model for any cost occurrence.
#'    - Fit a gamma model (log link) for positive costs, truncated at 99.5th percentile.
#'    - Predict expected costs for all HIV/SUD combinations.
#'    - Standardize costs using bin proportions.
#'    - Save each iteration's results as a parquet file.
#' 5. Combines bootstrap outputs, computes means, quantile-based CIs, and deltas.
#' 6. Writes the final summary CSV in the cause's `results` folder.
#'
#' @param df_all Full dataset (data.frame/data.table) containing at least:
#'        `acause_lvl2`, `race_cd`, `sex_id`, `has_hiv`, `has_sud`,
#'        `has_cost`, `tot_pay_amt`, and `age_group_years_start`.
#' @param cause_name Character, the specific `acause_lvl2` value to model.
#' @param B Integer, number of bootstrap iterations.
#' @param file_type String, file type label used in output filenames (e.g., "F2T" or "RX").
#' @param year_id Integer, year of data.
#' @param age_group_years_start Integer, starting age of the group in years.
#'
#' @return Invisibly returns the path to the final summary CSV. Writes results to disk.
#'
#' @note
#' If `has_hiv` or `has_sud` have no variation in a bootstrap iteration, that iteration
#' is skipped. If all iterations are skipped for a cause, no summary is written.
#'
#' @examples
#' run_one_cause(df, "_mental", 100, "F2T", 2010, 65)
#' 


# B selection
B <- if (file_type == "F2T") bootstrap_iterations_F2T else bootstrap_iterations_RX

# Run the single cause for this array task
run_one_cause(
  df_all = df,
  cause_name = cause_name,
  B = B,
  file_type = file_type,
  year_id = year_id,
  age_group_years_start = age_group_years_start
)
# make seeds vary across array tasks
seed_base <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID", unset = 1234))
set.seed(seed_base)


# run_one_cause <- function(df_all, cause_name, B,
#                           file_type, year_id, age_group_years_start) {
#   cat("\n=== Running cause:", cause_name, "===\n")
  
  # Per-cause folders
  cause_paths <- make_cause_dirs(cause_name)
  bootstrap_chunks_output_folder <- cause_paths$boot_dir
  results_output_folder          <- cause_paths$results_dir
  
  # Optional: clean old boot chunks for a fresh run
  old_files <- list.files(bootstrap_chunks_output_folder, full.names = TRUE)
  if (length(old_files) > 0L) file.remove(old_files)
  
  # Filter for cause-specific dataset
  df_cause <- as.data.table(df_all)[acause_lvl2 == cause_name]
  if (nrow(df_cause) == 0L) {
    cat("[", cause_name, "] No rows; skipping.\n", sep = "")
    return(invisible(NULL))
  }
  
  # Bins for standardization
  df_bins_master <- df_cause %>%
    group_by(race_cd, sex_id, age_group_years_start) %>%
    summarise(row_count = n(), .groups = "drop") %>%
    group_by(race_cd, age_group_years_start) %>%
    mutate(prop_bin = row_count / sum(row_count, na.rm = TRUE)) %>%
    ungroup()
  
  # Bootstrap count from file_type (falls back to 1)
  if (file_type == "F2T") {
    B_local <- B
  } else if (file_type == "RX") {
    B_local <- B
  } else {
    B_local <- 1
  }
  set.seed(123)
  
  # --- BOOTSTRAP ---
  kept_iters <- 0L
  for (b in seq_len(B_local)) {
    cat("[", cause_name, "] Bootstrap iteration:", b, "/", B_local, "\n", sep = "")
    
    # Resample
    df_boot <- df_cause[sample(.N, replace = TRUE)]
    
    # Keep factor levels stable
    df_boot[, `:=`(
      race_cd = factor(race_cd, levels = levels(df_all$race_cd)),
      sex_id  = factor(sex_id,  levels = levels(df_all$sex_id)),
      has_hiv = factor(has_hiv, levels = levels(df_all$has_hiv)),
      has_sud = factor(has_sud, levels = levels(df_all$has_sud))
    )]
    
    # Diversity check
    if (nlevels(droplevels(df_boot$has_hiv)) < 2 ||
        nlevels(droplevels(df_boot$has_sud)) < 2) {
      cat("[", cause_name, "] Skipping iteration ", b, " - insufficient HIV/SUD variation\n", sep = "")
      next
    }
    
    # Two-part models
    mod_logit <- glm(
      has_cost ~ has_hiv + has_sud + race_cd + sex_id,
      data   = df_boot,
      family = binomial(link = "logit")
    )
    
    df_gamma_input <- df_boot[tot_pay_amt > 0]
    if (nrow(df_gamma_input) < 10) { # safety
      cat("[", cause_name, "] Skipping iteration ", b, " - too few positive costs\n", sep = "")
      next
    }
    df_gamma_input[, tot_pay_amt := pmin(
      tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE)
    )]
    
    mod_gamma <- glm(
      tot_pay_amt ~ has_hiv + has_sud + race_cd + sex_id,
      data    = df_gamma_input,
      family  = Gamma(link = "log"),
      control = glm.control(maxit = 100)
    )
    
    # Prediction grid
    grid_input_master <- as.data.table(df_bins_master)[
      , .(race_cd, sex_id, age_group_years_start, prop_bin)
    ]
    combo_A <- CJ(
      has_hiv = levels(df_boot$has_hiv),
      has_sud = levels(df_boot$has_sud)
    )
    grid_input_master[, dummy := 1L]
    combo_A[, dummy := 1L]
    grid_input_master <- merge(
      grid_input_master, combo_A, by = "dummy", allow.cartesian = TRUE
    )[, dummy := NULL]
    
    # Predict expected cost
    grid_input_master[, prob_has_cost := predict(mod_logit, newdata = .SD, type = "response")]
    grid_input_master[, cost_if_pos   := predict(mod_gamma, newdata = .SD, type = "response")]
    grid_input_master[, exp_cost      := prob_has_cost * cost_if_pos]
    
    # Standardize + collapse
    out_b <- copy(grid_input_master)
    out_b[, exp_cost_bin := exp_cost * prop_bin]
    out_b[, acause_lvl2 := cause_name]
    out_b <- out_b[
      , .(exp_cost = sum(exp_cost_bin)),
      by = .(acause_lvl2, race_cd, age_group_years_start, has_hiv, has_sud)
    ]
    
    # Wide-format & deltas
    out_b <- dcast(
      out_b,
      acause_lvl2 + race_cd + age_group_years_start ~ has_hiv + has_sud,
      value.var = "exp_cost"
    )
    
    # Ensure all cells exist (fill with NA if missing in rare cases)
    for (nm in c("0_0","1_0","0_1","1_1")) if (!nm %in% names(out_b)) out_b[[nm]] <- NA_real_
    setnames(out_b, c("0_0","1_0","0_1","1_1"),
             c("cost_neither","cost_hiv_only","cost_sud_only","cost_hiv_sud"))
    
    out_b[, `:=`(
      delta_hiv_only = cost_hiv_only - cost_neither,
      delta_sud_only = cost_sud_only - cost_neither,
      delta_hiv_sud  = cost_hiv_sud  - cost_neither,
      bootstrap_iter = b
    )]
    
    # Write this bootstrap chunk
    boot_out_path <- file.path(
      bootstrap_chunks_output_folder,
      sprintf("bootstrap_iter_%03d.parquet", b)
    )
    write_parquet(out_b, boot_out_path)
    kept_iters <- kept_iters + 1L
  }
  
  if (kept_iters == 0L) {
    cat("[", cause_name, "] No valid bootstrap iterations kept. Skipping summary.\n")
    return(invisible(NULL))
  }
  
  # --- SUMMARY ---
  boot_combined <- open_dataset(bootstrap_chunks_output_folder) %>% collect()
  
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
    mutate(year_id = year_id, file_type = file_type)
  
  # Final column order + rename
  df_summary <- df_summary %>%
    rename(
      mean_cost           = mean_cost_neither,
      lower_ci            = lower_ci_neither,
      upper_ci            = upper_ci_neither,
      mean_cost_hiv       = mean_cost_hiv_only,
      lower_ci_hiv        = lower_ci_hiv_only,
      upper_ci_hiv        = upper_ci_hiv_only,
      mean_cost_sud       = mean_cost_sud_only,
      lower_ci_sud        = lower_ci_sud_only,
      upper_ci_sud        = upper_ci_sud_only
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
    "age_group_years_start", "year_id", "file_type"
  )
  df_summary <- df_summary[, desired_order]
  
  # Write per-cause final CSV
  final_csv  <- generate_filename("bootstrap_marginal_results", ".csv", cause = cause_name)
  final_path <- file.path(results_output_folder, final_csv)
  write.csv(df_summary, final_path, row.names = FALSE)
  cat("[", cause_name, "] Wrote final cause summary to:", final_path, "\n", sep = "")
  
  invisible(final_path)
}



# Driver 

##----------------------------------------------------------------
## 3. Run for all causes
##----------------------------------------------------------------

# Convert to factors (you already did this above, keeping here in case of re-ordering)
df[, `:=`(
  acause_lvl2 = factor(acause_lvl2),
  race_cd     = factor(race_cd),
  sex_id      = factor(sex_id),
  has_hiv     = factor(has_hiv, levels = c(0, 1)),
  has_sud     = factor(has_sud, levels = c(0, 1)),
  has_hepc    = factor(has_hepc, levels = c(0, 1)),
  has_cost    = factor(has_cost, levels = c(0, 1))
)]

# Choose causes to run
all_causes <- levels(df$acause_lvl2)
# Exclude HIV and SUD acause if you prefer (optional)
causes_to_run <- setdiff(all_causes, c("hiv", "_subs"))

cat("Will run", length(causes_to_run), "causes:\n")
print(causes_to_run)

# B from your earlier logic
B <- if (file_type == "F2T") bootstrap_iterations_F2T else bootstrap_iterations_RX

# Run sequentially (simple). For parallel / SLURM array, run run_one_cause per task.
outputs <- lapply(
  causes_to_run,
  function(cn) run_one_cause(
    df_all = df,
    cause_name = cn,
    B = B,
    file_type = file_type,
    year_id = year_id,
    age_group_years_start = age_group_years_start
  )
)

cat("\nDone. Per-cause results are in:\n", file.path(output_folder, "by_cause"), "\n")
