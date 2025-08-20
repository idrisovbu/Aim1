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

##----------------------------------------------------------------
# 0. Read in data from SLURM job submission
##----------------------------------------------------------------

##---------------------------
## 0) Read args / data
##---------------------------
if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_F2T_data_2014_age70.parquet"
  df <- open_dataset(path) %>% collect() %>% as.data.table()
  year_id <- df$year_id[1]
  file_type <- "F2T"
  age_group_years_start <- df$age_group_years_start[1]
  bootstrap_iterations_F2T <- 10
  bootstrap_iterations_RX  <- 1
  cause_name <- "_neo"            # <— set a single cause for local testing
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

# boot_chunks is by cause, year, then age group (e.g. boot_chunks/_enteric_all/2015/65/chunk1.parquet)
by_cause_boot_parent   <- file.path(by_cause_root, "boot_chunks")
by_cause_results_parent<- file.path(by_cause_root, "results")
by_cause_regression_coefficients <- file.path(by_cause_root, "regression_coefficients")

ensure_dir_exists(output_folder)
ensure_dir_exists(by_cause_root)
ensure_dir_exists(by_cause_boot_parent)
ensure_dir_exists(by_cause_results_parent)
ensure_dir_exists(by_cause_regression_coefficients)

slugify <- function(x) {
  x <- gsub("\\s+", "_", x, perl = TRUE)
  gsub("[^A-Za-z0-9._-]", "_", x, perl = TRUE)
}

make_cause_dirs <- function(cause_name) {
  cause_slug <- slugify(cause_name)
  boot_dir   <- file.path(by_cause_boot_parent,    cause_slug, year_id, age_group_years_start)
  regression_coefficients_dir   <- file.path(by_cause_regression_coefficients,    cause_slug, year_id, age_group_years_start)
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

# --- Setup for a single cause run (no function) -------------------------------
cat("\n=== Running cause:", cause_name, "===\n")
paths <- make_cause_dirs(cause_name)
bootstrap_chunks_output_folder <- paths$boot_dir
results_output_folder          <- paths$results_dir
regression_coefficients_output_folder          <- paths$regression_dir

# Clean old chunks (parquet only)
old_files <- list.files(bootstrap_chunks_output_folder, pattern = "\\.parquet$", full.names = TRUE)
if (length(old_files) > 0L) unlink(old_files, force = TRUE)

# Cause subset
df_cause <- as.data.table(df)[acause_lvl2 == cause_name]
if (nrow(df_cause) == 0L) {
  cat("[", cause_name, "] No rows for this cause. Nothing to do.\n", sep = "")
} else {
  # --- Build bins -------------------------------------------------------------
  df_bins_master <- df_cause %>%
    group_by(race_cd, sex_id, age_group_years_start) %>%
    summarise(row_count = n(), .groups = "drop") %>%
    group_by(race_cd, age_group_years_start) %>%
    mutate(prop_bin = row_count / sum(row_count)) %>%
    ungroup()
  
  # Collapsed-over-sex total row count to merge into summary later
  df_bins_summary <- df_bins_master %>%
    group_by(race_cd, age_group_years_start) %>%
    summarise(total_row_count = sum(row_count, na.rm = TRUE), .groups = "drop") %>%
    mutate(acause_lvl2 = as.character(cause_name)) %>%
    select(acause_lvl2, race_cd, age_group_years_start, total_row_count)
  
  # --- Bootstrap --------------------------------------------------------------
  B <- if (file_type == "F2T") as.integer(bootstrap_iterations_F2T) else as.integer(bootstrap_iterations_RX)
  set.seed(123)
  
  kept_iters <- 0L
  for (b in seq_len(B)) {
    cat("[", cause_name, "] Bootstrap iteration:", b, "/", B, "\n", sep = "")
    
    # Perform sampling
    
    # Choose stratification keys (may need to change how we decide boot strapped stratification)
    by_keys <- c("sex_id", "has_hiv", "has_sud")
    
    # One stratified resample (size-preserving within each stratum)
    df_boot <- df_cause[, .SD[sample(.N, .N, replace = TRUE)], by = by_keys]
    
    # Old method of sampling, testing new method above
    #df_boot <- df_cause[sample(.N, replace = TRUE)]
    
    # Stable factor levels
    df_boot[, `:=`(
      race_cd = factor(race_cd, levels = levels(df$race_cd)),
      sex_id  = factor(sex_id,  levels = levels(df$sex_id)),
      has_hiv = factor(has_hiv, levels = levels(df$has_hiv)),
      has_sud = factor(has_sud, levels = levels(df$has_sud))
    )]
    
    # Diversity guard
    if (nlevels(droplevels(df_boot$has_hiv)) < 2 ||
        nlevels(droplevels(df_boot$has_sud)) < 2) {
      cat("[", cause_name, "] Skip iter ", b, " - insufficient HIV/SUD variation\n", sep = "")
      next
    }
    
    # Part 1: logit
    mod_logit <- try(glm(
      has_cost ~ has_hiv + has_sud + race_cd + sex_id,
      data = df_boot, family = binomial(link = "logit")
    ), silent = TRUE)
    if (inherits(mod_logit, "try-error")) {
      cat("[", cause_name, "] Skip iter ", b, " - logit failed\n", sep = ""); next
    }
    
    # Part 2: gamma on positive costs (truncate 99.5%)
    df_gamma_input <- df_boot[tot_pay_amt > 0]
    if (nrow(df_gamma_input) < 10) {
      cat("[", cause_name, "] Skip iter ", b, " - too few positive costs\n", sep = ""); next
    }
    df_gamma_input[, tot_pay_amt := pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))]
    mod_gamma <- try(glm(
      tot_pay_amt ~ has_hiv + has_sud + race_cd + sex_id,
      data = df_gamma_input, family = Gamma(link = "log"),
      control = glm.control(maxit = 100)
    ), silent = TRUE)
    if (inherits(mod_gamma, "try-error")) {
      cat("[", cause_name, "] Skip iter ", b, " - gamma failed\n", sep = ""); next
    }
    
    # Prediction grid over bins × {HIV(0,1) × SUD(0,1)}
    grid_input_master <- as.data.table(df_bins_master)[, .(race_cd, sex_id, age_group_years_start, prop_bin)]
    combo_A <- CJ(has_hiv = levels(df_boot$has_hiv), has_sud = levels(df_boot$has_sud))
    grid_input_master[, dummy := 1L]; combo_A[, dummy := 1L]
    grid_input_master <- merge(grid_input_master, combo_A, by = "dummy", allow.cartesian = TRUE)[, dummy := NULL]
    
    # Predict E[cost] = P(cost>0) × E[cost | cost>0]
    grid_input_master[, prob_has_cost := predict(mod_logit, newdata = .SD, type = "response")]
    grid_input_master[, cost_if_pos   := predict(mod_gamma, newdata = .SD, type = "response")]
    grid_input_master[, exp_cost      := prob_has_cost * cost_if_pos]
    
    # Standardize & collapse to (race, age, has_hiv, has_sud)
    out_b <- copy(grid_input_master)
    out_b[, exp_cost_bin := exp_cost * prop_bin]
    out_b[, acause_lvl2 := cause_name]
    out_b <- out_b[
      , .(exp_cost = sum(exp_cost_bin)),
      by = .(acause_lvl2, race_cd, age_group_years_start, has_hiv, has_sud)
    ]
    
    # Wide & deltas
    out_b <- dcast(
      out_b,
      acause_lvl2 + race_cd + age_group_years_start ~ has_hiv + has_sud,
      value.var = "exp_cost"
    )
    for (nm in c("0_0","1_0","0_1","1_1")) if (!nm %in% names(out_b)) out_b[[nm]] <- NA_real_
    setnames(out_b, c("0_0","1_0","0_1","1_1"),
             c("cost_neither","cost_hiv_only","cost_sud_only","cost_hiv_sud"))
    
    out_b[, `:=`(
      delta_hiv_only = cost_hiv_only - cost_neither,
      delta_sud_only = cost_sud_only - cost_neither,
      delta_hiv_sud  = cost_hiv_sud  - cost_neither,
      bootstrap_iter = b
    )]

    # Size checking
    cols_to_check <- c("cost_neither", "cost_sud_only", "cost_hiv_only", "cost_hiv_sud")
    skip_to_next <- FALSE
    
    # Check when columns have enormous values
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
    logit_df <- extract_all_coefs(mod_logit, "logit")
    gamma_df <- extract_all_coefs(mod_gamma, "gamma")
    
    # Merge and annotate regression coefficients
    regression_results <- full_join(logit_df, gamma_df, by = "variable") %>%
      mutate(
        interaction_dropped = if_else(is.na(estimate_gamma) & str_detect(variable, ":"), TRUE, FALSE),
        year_id = year_id,
        file_type = file_type,
        age_group_years_start = age_group_years_start
      ) %>%
      select(variable, estimate_logit, p_logit, estimate_gamma, p_gamma,
             interaction_dropped, year_id, file_type, age_group_years_start)
    
    # label bootstrap value column
    regression_results$bootstrap_number <- b
    regression_results$cause_name <- cause_name
    
    # Save regression coefficients
    file_out_regression <- generate_filename("regression_results", ".csv")
    file_out_regression <- sub("\\.csv$", paste0("_bootstrap#", b, ".csv"), file_out_regression)
    out_path_regression <- file.path(regression_coefficients_output_folder, file_out_regression)
    write_csv(regression_results, out_path_regression)
    cat("✅ Regression coefficients saved to:", out_path_regression, "\n")
    
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
        left_join(df_bins_summary, by = c("acause_lvl2","race_cd","age_group_years_start")) %>%
        mutate(year_id = year_id, file_type = file_type) %>%
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
        "total_row_count", "age_group_years_start", "year_id", "file_type"
      )
      df_summary <- df_summary[, desired_order]
      
      # Write CSV
      final_csv  <- generate_filename("bootstrap_marginal_results", ".csv", cause = cause_name)
      final_path <- file.path(paths$results_dir, final_csv)
      write.csv(df_summary, final_path, row.names = FALSE)
      cat("[", cause_name, "] Wrote final cause summary to:", final_path, "\n", sep = "")
    }
  }
}

cat("\nDone. Per-cause results are in:\n", file.path(output_folder, "by_cause"), "\n")
