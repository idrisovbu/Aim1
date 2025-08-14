##----------------------------------------------------------------
##' Title: B2_worker_two_part_model.R
##' Notes: # For the future check bootstrap for finite populations
##' Outputs: /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/04.Two_Part_Estimates/<date>/bootstrap_results/
##'          /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/04.Two_Part_Estimates/<date>/boot_chunks/
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
  
  # Optional code to remove low count acause_lvl2 and fix factors only having 0's or 1's
  # # remove low count ones
  # count <- df %>% count(acause_lvl2)
  # keep_cause <- count[order(-n)]$acause_lvl2[1:(length(count$acause_lvl2) - 6)]
  # df <- df[df$acause_lvl2 %in% keep_cause, ]
  # 
  # # fix 0 and 1 factor to be more even
  # df[1:5000, sex_id := 0]
  # df[1:2500, has_hiv := 1]
  # df[2500:7500, has_sud := 0]
  
} else {
  # Read job args from SUBMIT_ARRAY_JOB
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input <- args[1]
  bootstrap_iterations_F2T <- as.integer(args[2])
  bootstrap_iterations_RX <- as.integer(args[3])
  
  # Identify row using SLURM array task ID
  array_job_number <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message("SLURM Job ID: ", array_job_number)
  
  df_parameters <- fread(fp_parameters_input)
  df_job <- df_parameters[array_job_number, ]
  
  fp_input <- df_job$directory
  file_type <- df_job$file_type
  year_id <- df_job$year_id
  
  # Load data (can switch to open_dataset() if needed)
  df <- read_parquet(fp_input) %>% as.data.table()
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

## ---- 0. Dir + filename helpers --------------------------------------------

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

# 0.4 fixed parents (by-cause parent, bin_summary, logs)
by_cause_parent_folder    <- file.path(output_folder, "by_cause")
#bin_summary_output_folder <- file.path(output_folder, "bin_summary")
log_folder                <- file.path(base_output_dir, "logs")

# create parents
ensure_dir_exists(output_folder)
ensure_dir_exists(by_cause_parent_folder)
ensure_dir_exists(bin_summary_output_folder)
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

# 0.6 create per-cause folders and assign bootstrap_chunks_output_folder
cause_name <- "_mental"  # replace dynamically in your loop if needed
cause_paths <- make_cause_dirs(cause_name)
bootstrap_chunks_output_folder <- cause_paths$boot_dir      # <-- use this when writing boot chunks
results_output_folder          <- cause_paths$results_dir   # <-- use this when writing per-cause final CSVs




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


#unique(df$acause_lvl2)


cause_name <- "_mental"

df_cause <- df %>%
  filter(acause_lvl2 == cause_name)



##----------------------------------------------------------------
## 
##----------------------------------------------------------------

# Example cause name
cause_name <- "_mental"

# Filter for cause-specific dataset
df_cause <- as.data.table(df)[acause_lvl2 == cause_name]

#----------------------------------------------------------------
# Bins for standardization (no acause_lvl2 here because it's fixed)
#----------------------------------------------------------------
df_bins_master <- df_cause %>%
  group_by(race_cd, sex_id, age_group_years_start) %>%
  summarise(row_count = n(), .groups = "drop") %>%
  group_by(race_cd, age_group_years_start) %>%
  mutate(prop_bin = row_count / sum(row_count, na.rm = TRUE)) %>%
  ungroup()

#----------------------------------------------------------------
# Bootstrap
#----------------------------------------------------------------



if (file_type == "F2T") {
  B <- bootstrap_iterations_F2T
} else if (file_type == "RX") {
  B <- bootstrap_iterations_RX
} else {
  B <- 1
}
set.seed(123)

for (b in seq_len(B)) {
  cat("Bootstrap iteration:", b, "/", B, "\n")
  
  # Resample with replacement
  df_boot <- df_cause[sample(.N, replace = TRUE)]
  
  # Keep factor levels stable
  df_boot[, `:=`(
    race_cd = factor(race_cd, levels = levels(df$race_cd)),
    sex_id  = factor(sex_id,  levels = levels(df$sex_id)),
    has_hiv = factor(has_hiv, levels = levels(df$has_hiv)),
    has_sud = factor(has_sud, levels = levels(df$has_sud))
  )]
  
  # Check diversity
  if (nlevels(droplevels(df_boot$has_hiv)) < 2 ||
      nlevels(droplevels(df_boot$has_sud)) < 2) {
    cat("Skipping iteration", b, "- insufficient factor diversity\n")
    next
  }
  
  # Part 1: Logistic
  mod_logit <- glm(
    has_cost ~ has_hiv + has_sud + race_cd + sex_id,
    data   = df_boot,
    family = binomial(link = "logit")
  )
  
  # Part 2: Gamma
  df_gamma_input <- df_boot[tot_pay_amt > 0]
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
    grid_input_master, combo_A,
    by = "dummy", allow.cartesian = TRUE
  )[, dummy := NULL]
  
  # Predict expected costs
  grid_input_master[, prob_has_cost := predict(mod_logit, newdata = .SD, type = "response")]
  grid_input_master[, cost_if_pos   := predict(mod_gamma, newdata = .SD, type = "response")]
  grid_input_master[, exp_cost      := prob_has_cost * cost_if_pos]
  
  # Standardize
  out_b <- copy(grid_input_master)
  out_b[, exp_cost_bin := exp_cost * prop_bin]
  out_b[, acause_lvl2 := cause_name]
  
  # Collapse over sex
  out_b <- out_b[
    , .(exp_cost = sum(exp_cost_bin)),
    by = .(acause_lvl2, race_cd, age_group_years_start, has_hiv, has_sud)
  ]
  
  # Wide format
  out_b <- dcast(
    out_b,
    acause_lvl2 + race_cd + age_group_years_start ~ has_hiv + has_sud,
    value.var = "exp_cost"
  )
  
  setnames(out_b, c("0_0", "1_0", "0_1", "1_1"),
           c("cost_neither", "cost_hiv_only", "cost_sud_only", "cost_hiv_sud"))
  
  out_b[, `:=`(
    delta_hiv_only = cost_hiv_only - cost_neither,
    delta_sud_only = cost_sud_only - cost_neither,
    delta_hiv_sud  = cost_hiv_sud  - cost_neither,
    bootstrap_iter = b
  )]
  
  boot_out_path <- file.path(bootstrap_chunks_output_folder,
                             sprintf("bootstrap_iter_%03d.parquet", b))
  write_parquet(out_b, boot_out_path)
  cat("Written:", boot_out_path, "\n")
  
  rm(df_boot, df_gamma_input, mod_logit, mod_gamma, out_b, grid_input_master)
  gc(verbose = FALSE)
}

#----------------------------------------------------------------
# Bootstrap summary
#----------------------------------------------------------------


# Combine bootstrap results for this cause
boot_combined <- open_dataset(bootstrap_chunks_output_folder) %>% collect()

# Per-cause bin summary
df_bins_summary <- df_bins_master %>%
  mutate(acause_lvl2 = cause_name) %>%
  group_by(acause_lvl2, race_cd, age_group_years_start) %>%
  summarise(total_row_count = sum(row_count, na.rm = TRUE), .groups = "drop")

# Per-cause bootstrap summary with CIs
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
  left_join(df_bins_summary, by = c("acause_lvl2", "race_cd", "age_group_years_start")) %>%
  mutate(year_id = year_id, file_type = file_type)

# Rename columns (same as before)
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
    upper_ci_sud        = upper_ci_sud_only,
    mean_cost_hiv_sud   = mean_cost_hiv_sud,
    lower_ci_hiv_sud    = lower_ci_hiv_sud,
    upper_ci_hiv_sud    = upper_ci_hiv_sud,
    mean_delta_hiv      = mean_delta_hiv_only,
    lower_ci_delta_hiv  = lower_ci_delta_hiv_only,
    upper_ci_delta_hiv  = upper_ci_delta_hiv_only,
    mean_delta_sud      = mean_delta_sud_only,
    lower_ci_delta_sud  = lower_ci_delta_sud_only,
    upper_ci_delta_sud  = upper_ci_delta_sud_only,
    mean_delta_hiv_sud  = mean_delta_hiv_sud,
    lower_ci_delta_hiv_sud = lower_ci_delta_hiv_sud,
    upper_ci_delta_hiv_sud = upper_ci_delta_hiv_sud
  )

# Arrange columns in desired order
desired_order <- c(
  "acause_lvl2", "race_cd",
  "mean_cost", "lower_ci", "upper_ci",
  "mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv",
  "mean_cost_sud", "lower_ci_sud", "upper_ci_sud",
  "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud",
  "mean_delta_hiv", "lower_ci_delta_hiv", "upper_ci_delta_hiv",
  "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud",
  "mean_delta_hiv_sud", "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud",
  "total_row_count", "age_group_years_start", "year_id", "file_type"
)
df_summary <- df_summary[, desired_order]

# --- WRITE OUTPUTS ---
final_csv <- generate_filename("bootstrap_marginal_results", ".csv", cause = cause_name)
final_path <- file.path(cause_paths$results_dir, final_csv)
write.csv(df_summary, final_path, row.names = FALSE)
cat("Wrote final cause summary to:", final_path, "\n")

# # Bin summary
# bin_csv <- generate_filename("bin_summary", ".csv", cause = cause_name)
# bin_path <- file.path(cause_paths$results_dir, bin_csv)
# write.csv(df_bins_summary, bin_path, row.names = FALSE)
# cat("Wrote bin summary to:", bin_path, "\n")
# 


