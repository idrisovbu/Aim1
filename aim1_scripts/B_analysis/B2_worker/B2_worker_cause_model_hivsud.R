##----------------------------------------------------------------
## Title: B2_worker_cause_model_hivsud.R
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

##---------------------------
## 0) Read args / data
##---------------------------
if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_F2T_data_2010_age65.parquet"
  df <- open_dataset(path) %>% collect() %>% as.data.table()
  year_id <- 2010
  file_type <- "F2T"
  age_group_years_start <- df$age_group_years_start[1]
  bootstrap_iterations_F2T <- 2
  bootstrap_iterations_RX  <- 1
  cause_name <- "_subs"            # <— set a single cause for local testing
  
  fp_input <- path # sets this for message output if needed
} else {
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input       <- args[1]
  bootstrap_iterations_F2T  <- as.integer(args[2])
  bootstrap_iterations_RX   <- as.integer(args[3])
  
  array_job_number <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message("SLURM Job ID: ", array_job_number)
  
  df_parameters <- fread(fp_parameters_input)
  df_job <- df_parameters[array_job_number, ]
  
  fp_input    <- df_job$directory
  file_type   <- df_job$file_type
  year_id     <- df_job$year_id
  cause_name  <- df_job$cause_name              # <— single cause per array task
  
  df <- read_parquet(fp_input) %>% as.data.table()
  age_group_years_start <- df$age_group_years_start[1]
}

##---------------------------
## 1) Helpers
##---------------------------
# slugify <- function(x) {
#   x <- gsub("\\s+", "_", x, perl = TRUE)
#   gsub("[^A-Za-z0-9._-]", "_", x, perl = TRUE)
# }
ensure_dir_exists <- function(d) if (!dir.exists(d)) dir.create(d, recursive = TRUE)
generate_filename <- function(prefix, extension, cause = NULL) {
  cpart <- if (is.null(cause)) "" else paste0("_", slugify(cause))
  paste0(prefix, cpart, "_", file_type, "_year", year_id,
         "_age", age_group_years_start, extension)
}

# outputs
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder     <- format(Sys.time(), "%Y%m%d")
output_folder   <- file.path(base_output_dir, "04.Two_Part_Estimates", date_folder)

# parent branches under by_cause
by_cause_root          <- file.path(output_folder, "by_cause")
by_cause_boot_parent   <- file.path(by_cause_root, "boot_chunks")
by_cause_results_parent<- file.path(by_cause_root, "results")

ensure_dir_exists(output_folder)
ensure_dir_exists(by_cause_root)
ensure_dir_exists(by_cause_boot_parent)
ensure_dir_exists(by_cause_results_parent)

slugify <- function(x) {
  x <- gsub("\\s+", "_", x, perl = TRUE)
  gsub("[^A-Za-z0-9._-]", "_", x, perl = TRUE)
}

make_cause_dirs <- function(cause_name) {
  cause_slug <- slugify(cause_name)
  boot_dir   <- file.path(by_cause_boot_parent,    cause_slug)
  res_dir    <- file.path(by_cause_results_parent, cause_slug)
  ensure_dir_exists(boot_dir)
  ensure_dir_exists(res_dir)
  list(
    cause_slug  = cause_slug,
    boot_dir    = boot_dir,
    results_dir = res_dir
  )
}

##---------------------------
## 2) Coerce factors
##---------------------------
df[, `:=`(
  acause_lvl2 = factor(acause_lvl2),
  race_cd     = factor(race_cd),
  sex_id      = factor(sex_id),
  has_hiv     = factor(has_hiv, levels = c(0, 1)),
  has_sud     = factor(has_sud, levels = c(0, 1)),
  has_hepc    = factor(has_hepc, levels = c(0, 1)),
  has_cost    = factor(has_cost, levels = c(0, 1))
)]

##---------------------------
## 3) Function: run_one_cause
##---------------------------

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

# Set number of iterations
B <- if (file_type == "F2T") bootstrap_iterations_F2T else bootstrap_iterations_RX

# Set directories
cat("\n=== Running cause:", cause_name, "===\n")
paths <- make_cause_dirs(cause_name)
bootstrap_chunks_output_folder <- paths$boot_dir
results_output_folder          <- paths$results_dir

# clean old chunks
old_files <- list.files(bootstrap_chunks_output_folder, full.names = TRUE)
if (length(old_files) > 0L) file.remove(old_files)

# cause subset
df_cause <- as.data.table(df)[acause_lvl2 == cause_name]
if (nrow(df_cause) == 0L) {
  cat("No rows for cause_name: [", cause_name, "]; skipping.\n", sep = ""); 
  cat("Year_id: ", year_id, "\n")
  cat("Age group years start: ", age_group_years_start, "\n")
  cat("Filetype: ", file_type, "\n")
  cat("Filepath: ", fp_input, "\n")
  stop("No available data to process in this dataset. See metadata above for diagnosing.")
}

# bins
df_bins_master <- df_cause %>%
  group_by(race_cd, sex_id, age_group_years_start) %>%
  summarise(row_count = n(), .groups = "drop") %>%
  group_by(race_cd, age_group_years_start) %>%
  mutate(prop_bin = row_count / sum(row_count)) %>%
  ungroup()

# set seed
set.seed(123)

# Run bootstrap model
kept_iters <- 0L
for (b in seq_len(B)) {
  cat("[", cause_name, "] Bootstrap iteration:", b, "/", B, "\n", sep = "")
  df_boot <- df_cause[sample(.N, replace = TRUE)]
  
  # stable levels
  df_boot[, `:=`(
    race_cd = factor(race_cd, levels = levels(df$race_cd)),
    sex_id  = factor(sex_id,  levels = levels(df$sex_id)),
    has_hiv = factor(has_hiv, levels = levels(df$has_hiv)),
    has_sud = factor(has_sud, levels = levels(df$has_sud))
  )]
  
  # HIV
  if (cause_name == "hiv") {
    
    # diversity check
    if (nlevels(droplevels(df_boot$has_sud)) < 2) {
      cat("[", cause_name, "] Skip iter ", b, " - insufficient SUD/\n", sep = "")
      next;
    }
    
    # part 1
    mod_logit <- glm(
      has_cost ~ has_sud + race_cd + sex_id,
      data = df_boot, family = binomial(link = "logit"))
    
    # part 2
    df_gamma_input <- df_boot[tot_pay_amt > 0]
    if (nrow(df_gamma_input) < 10) {  # safety
      cat("[", cause_name, "] Skip iter ", b, " - too few positive costs\n", sep = ""); next
    }
    df_gamma_input[, tot_pay_amt := pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))]
    mod_gamma <- glm(
      tot_pay_amt ~ has_sud + race_cd + sex_id,
      data = df_gamma_input, family = Gamma(link = "log"),
      control = glm.control(maxit = 100)
    )
    
    # prediction grid
    grid_input_master <- as.data.table(df_bins_master)[, .(race_cd, sex_id, age_group_years_start, prop_bin)]
    combo_A <- CJ(
      has_sud = levels(df_boot$has_sud)
    )
    grid_input_master[, dummy := 1L]; combo_A[, dummy := 1L]
    grid_input_master <- merge(grid_input_master, combo_A, by = "dummy", allow.cartesian = TRUE)[, dummy := NULL]
    
    # predict expected costs
    grid_input_master[, prob_has_cost := predict(mod_logit, newdata = .SD, type = "response")]
    grid_input_master[, cost_if_pos   := predict(mod_gamma, newdata = .SD, type = "response")]
    grid_input_master[, exp_cost      := prob_has_cost * cost_if_pos]
    
    # standardize & collapse
    out_b <- copy(grid_input_master)
    out_b[, exp_cost_bin := exp_cost * prop_bin]
    out_b[, acause_lvl2 := cause_name]
    out_b <- out_b[
      , .(exp_cost = sum(exp_cost_bin)),
      by = .(acause_lvl2, race_cd, age_group_years_start, has_sud)
    ]
    
    # wide & deltas
    out_b <- dcast(
      out_b,
      acause_lvl2 + race_cd + age_group_years_start ~ has_sud,
      value.var = "exp_cost"
    )
    
    # set column names
    setnames(out_b, c("0", "1"),
             c("cost_hiv_only","cost_hiv_sud"))
    
    # calculate deltas
    out_b[, `:=`(
      delta_sud_only = cost_hiv_sud - cost_hiv_only,
      bootstrap_iter = b
    )]
    
    # write chunk
    write_parquet(out_b, file.path(bootstrap_chunks_output_folder,
                                   sprintf("bootstrap_iter_%03d.parquet", b)))
    kept_iters <- kept_iters + 1L
  } # END OF HIV SECTION
  
  # SUD
  if (cause_name == "_subs") {
    
    # diversity check
    if (nlevels(droplevels(df_boot$has_hiv)) < 2) {
      cat("[", cause_name, "] Skip iter ", b, " - insufficient HIV/\n", sep = "")
    }
    
    # part 1
    mod_logit <- glm(
      has_cost ~ has_hiv + race_cd + sex_id,
      data = df_boot, family = binomial(link = "logit"))
    
    # part 2
    df_gamma_input <- df_boot[tot_pay_amt > 0]
    if (nrow(df_gamma_input) < 10) {  # safety
      cat("[", cause_name, "] Skip iter ", b, " - too few positive costs\n", sep = ""); next
    }
    df_gamma_input[, tot_pay_amt := pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))]
    mod_gamma <- glm(
      tot_pay_amt ~ has_hiv + race_cd + sex_id,
      data = df_gamma_input, family = Gamma(link = "log"),
      control = glm.control(maxit = 100)
    )
    
    # prediction grid
    grid_input_master <- as.data.table(df_bins_master)[, .(race_cd, sex_id, age_group_years_start, prop_bin)]
    combo_A <- CJ(
      has_hiv = levels(df_boot$has_hiv)
    )
    grid_input_master[, dummy := 1L]; combo_A[, dummy := 1L]
    grid_input_master <- merge(grid_input_master, combo_A, by = "dummy", allow.cartesian = TRUE)[, dummy := NULL]
    
    # predict expected costs
    grid_input_master[, prob_has_cost := predict(mod_logit, newdata = .SD, type = "response")]
    grid_input_master[, cost_if_pos   := predict(mod_gamma, newdata = .SD, type = "response")]
    grid_input_master[, exp_cost      := prob_has_cost * cost_if_pos]
    
    # standardize & collapse
    out_b <- copy(grid_input_master)
    out_b[, exp_cost_bin := exp_cost * prop_bin]
    out_b[, acause_lvl2 := cause_name]
    out_b <- out_b[
      , .(exp_cost = sum(exp_cost_bin)),
      by = .(acause_lvl2, race_cd, age_group_years_start, has_hiv)
    ]
    
    # wide & deltas
    out_b <- dcast(
      out_b,
      acause_lvl2 + race_cd + age_group_years_start ~ has_hiv,
      value.var = "exp_cost"
    )
    
    # set column names
    setnames(out_b, c("0", "1"),
             c("cost_sud_only","cost_hiv_sud"))
    
    # calculate deltas
    out_b[, `:=`(
      delta_hiv_only = cost_hiv_sud - cost_sud_only,
      bootstrap_iter = b
    )]
    
    # write chunk
    write_parquet(out_b, file.path(bootstrap_chunks_output_folder,
                                   sprintf("bootstrap_iter_%03d.parquet", b)))
    kept_iters <- kept_iters + 1L
  } # END OF SUD SECTION
}

# Check to ensure that we actually created bootstrap chunk files  
if (kept_iters == 0L) {
  cat("[", cause_name, "] No valid bootstrap iterations kept. Skipping summary.\n")
  return(invisible(NULL))
}

# Combine bootstrap results into single df
boot_combined <- open_dataset(bootstrap_chunks_output_folder) %>% collect()

# summarise based on HIV / SUD
if (cause_name == "hiv") {
  df_summary <- boot_combined %>%
    group_by(acause_lvl2, race_cd, age_group_years_start) %>%
    summarise(
      mean_cost_hiv_only = mean(cost_hiv_only, na.rm = TRUE),
      mean_cost_hiv_sud  = mean(cost_hiv_sud,  na.rm = TRUE),
      lower_ci_hiv_only  = quantile(cost_hiv_only, 0.025, na.rm = TRUE),
      upper_ci_hiv_only  = quantile(cost_hiv_only, 0.975, na.rm = TRUE),
      lower_ci_hiv_sud   = quantile(cost_hiv_sud,  0.025, na.rm = TRUE),
      upper_ci_hiv_sud   = quantile(cost_hiv_sud,  0.975, na.rm = TRUE),
      mean_delta_sud_only = mean(delta_sud_only, na.rm = TRUE),
      lower_ci_delta_sud_only = quantile(delta_sud_only, 0.025, na.rm = TRUE),
      upper_ci_delta_sud_only = quantile(delta_sud_only, 0.975, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(year_id = year_id, file_type = file_type) %>%
    rename(
      mean_cost_hiv = mean_cost_hiv_only,
      lower_ci_hiv  = lower_ci_hiv_only,
      upper_ci_hiv  = upper_ci_hiv_only
    )
  
  desired_order <- c(
    "acause_lvl2", "race_cd",
    "mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv",
    "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud",
    "mean_delta_sud_only", "lower_ci_delta_sud_only", "upper_ci_delta_sud_only",
    "age_group_years_start", "year_id", "file_type"
  )
  df_summary <- df_summary[, desired_order]
  
} else if (cause_name == "_subs") {
  df_summary <- boot_combined %>%
    group_by(acause_lvl2, race_cd, age_group_years_start) %>%
    summarise(
      mean_cost_sud_only = mean(cost_sud_only, na.rm = TRUE),
      mean_cost_hiv_sud  = mean(cost_hiv_sud,  na.rm = TRUE),
      lower_ci_sud_only  = quantile(cost_sud_only, 0.025, na.rm = TRUE),
      upper_ci_sud_only  = quantile(cost_sud_only, 0.975, na.rm = TRUE),
      lower_ci_hiv_sud   = quantile(cost_hiv_sud,  0.025, na.rm = TRUE),
      upper_ci_hiv_sud   = quantile(cost_hiv_sud,  0.975, na.rm = TRUE),
      mean_delta_hiv_only = mean(delta_hiv_only, na.rm = TRUE),
      lower_ci_delta_hiv_only = quantile(delta_hiv_only, 0.025, na.rm = TRUE),
      upper_ci_delta_hiv_only = quantile(delta_hiv_only, 0.975, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(year_id = year_id, file_type = file_type) %>%
    rename(
      mean_cost_sud = mean_cost_sud_only,
      lower_ci_sud  = lower_ci_sud_only,
      upper_ci_sud  = upper_ci_sud_only
    )
  
  desired_order <- c(
    "acause_lvl2", "race_cd",
    "mean_cost_sud", "lower_ci_sud", "upper_ci_sud",
    "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud",
    "mean_delta_hiv_only", "lower_ci_delta_hiv_only", "upper_ci_delta_hiv_only",
    "age_group_years_start", "year_id", "file_type"
  )
  df_summary <- df_summary[, desired_order]
}

# Save as CSV
final_csv  <- generate_filename("bootstrap_marginal_results", ".csv", cause = cause_name)
final_path <- file.path(paths$results_dir, final_csv)
write.csv(df_summary, final_path, row.names = FALSE)
cat("[", cause_name, "] Wrote final cause summary to:", final_path, "\n", sep = "")

cat("\nDone. Per-cause results are in:\n", file.path(output_folder, "by_cause"), "\n")


