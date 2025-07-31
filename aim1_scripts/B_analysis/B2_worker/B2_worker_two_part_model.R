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
  #path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_RX_data_2010_age65.parquet"
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_F2T_data_2010_age65.parquet"
  # df <- read_parquet(path) %>% sample_n(10000) # This loads the whole dataset in and takes a long time
  df <- open_dataset(path) %>% head(100000) %>% collect() %>% sample_n(10000) # Only reads first 100,000 rows, then samples 10,000, much faster
  df <- as.data.table(df)  
  year_id <- 2010
  file_type <- "F2T"
  age_group_years_start <- df$age_group_years_start[1]
  
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
  age_group_years_start <- df$age_group_years_start[1]  # take the first row value, assuming all rows have the same age
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

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, "04.Two_Part_Estimates", date_folder)

# Define subfolders for output types
bootstrap_results_output_folder <- file.path(output_folder, "bootstrap_results")
bootstrap_chunks_output_folder <- file.path(output_folder, "boot_chunks", generate_filename("boot", ""))

# Create logs subfolder inside summary stats
log_folder <- file.path(base_output_dir, "logs")

# Create all necessary directories
ensure_dir_exists(bootstrap_results_output_folder)
ensure_dir_exists(bootstrap_chunks_output_folder)
ensure_dir_exists(log_folder)

##----------------------------------------------------------------
## 2. Convert key variables to factors for modeling purposes
##----------------------------------------------------------------
# --- Assign explicit factor variable for toc ---
toc_levels <- c("AM", "ED", "HH", "IP", "NF", "RX")

df[, `:=`(
  acause_lvl2   = factor(acause_lvl2),
  race_cd       = factor(race_cd),
  sex_id        = factor(sex_id),
  toc_fact      = factor(as.character(toc), levels = toc_levels),         
  has_hiv       = factor(has_hiv, levels = c(0, 1)),
  has_sud       = factor(has_sud, levels = c(0, 1)),
  has_hepc      = factor(has_hepc, levels = c(0, 1)),
  has_cost      = factor(has_cost, levels = c(0, 1))
)]


##----------------------------------------------------------------
## 3. Build bin distribution from the full data, used in bootstrapping
##----------------------------------------------------------------

df_bins_master <- df %>%
  group_by(acause_lvl2, race_cd, sex_id, age_group_years_start, toc_fact, has_hiv, has_sud) %>%
  summarise(row_count = n(), .groups = "drop") %>%
  group_by(acause_lvl2, race_cd, sex_id, age_group_years_start, toc_fact) %>%
  mutate(prop_bin = row_count / sum(row_count, na.rm = TRUE)) %>%
  ungroup()

##----------------------------------------------------------------
## 4. Bootstrap
##----------------------------------------------------------------

B <- 50 # Number of bootstrap iterations
 
# Set seed
set.seed(123)

# Bootstrap
for (b in seq_len(B)) {
  cat("Bootstrap iteration:", b, "/", B, "\n")
  
  # Sample with replacement — efficient in data.table
  df_boot <- df[sample(.N, replace = TRUE)]
  
  # Ensure factor levels are preserved
  df_boot[, `:=`(
    acause_lvl2 = factor(acause_lvl2, levels = levels(df$acause_lvl2)),
    race_cd     = factor(race_cd, levels = levels(df$race_cd)),
    sex_id      = factor(sex_id, levels = levels(df$sex_id)),
    toc_fact    = factor(toc_fact, levels = levels(df$toc_fact)),
    has_hiv     = factor(has_hiv, levels = levels(df$has_hiv)),
    has_sud     = factor(has_sud, levels = levels(df$has_sud))
  )]
  
  # Skip iterations with low factor level diversity
  if (nlevels(droplevels(df_boot$has_hiv)) < 2 ||
      nlevels(droplevels(df_boot$has_sud)) < 2 ||
      nlevels(droplevels(df_boot$acause_lvl2)) < 2) {
    cat("Skipping iteration", b, "- insufficient factor diversity\n")
    next
  }
  
  # Logistic model (toc_fact removed for RX)
  mod_logit <- if (file_type == "RX") {
    glm(has_cost ~ acause_lvl2 * has_hiv + acause_lvl2 * has_sud + race_cd + sex_id,
        data = df_boot, family = binomial(link = "logit"))
  } else {
    glm(has_cost ~ acause_lvl2 * has_hiv + acause_lvl2 * has_sud + race_cd + sex_id + toc_fact,
        data = df_boot, family = binomial(link = "logit"))
  }
  
  # Gamma input
  df_gamma_input <- df_boot[tot_pay_amt > 0]
  df_gamma_input[, tot_pay_amt := pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))]
  
  # Gamma model (toc_fact removed for RX)
  mod_gamma <- if (file_type == "RX") {
    glm(tot_pay_amt ~ acause_lvl2 * has_hiv + acause_lvl2 * has_sud + race_cd + sex_id,
        data = df_gamma_input, family = Gamma(link = "log"), control = glm.control(maxit = 100))
  } else {
    glm(tot_pay_amt ~ acause_lvl2 * has_hiv + acause_lvl2 * has_sud + race_cd + sex_id + toc_fact,
        data = df_gamma_input, family = Gamma(link = "log"), control = glm.control(maxit = 100))
  }
  
  # Predict in chunks to save memory
  grid_input_master <- as.data.table(df_bins_master)
  grid_input_master[, prob_has_cost := predict(mod_logit, newdata = .SD, type = "response")]
  grid_input_master[, cost_if_pos := predict(mod_gamma, newdata = .SD, type = "response")]
  grid_input_master[, exp_cost := prob_has_cost * cost_if_pos]
  
  # Group summary
  out_b <- grid_input_master[, .(exp_cost = sum(exp_cost * prop_bin, na.rm = TRUE)),
                             by = .(acause_lvl2, race_cd, has_hiv, has_sud, age_group_years_start, toc_fact)]
  
  out_b <- dcast(
    out_b,
    acause_lvl2 + race_cd + age_group_years_start + toc_fact ~ has_hiv + has_sud,
    value.var = "exp_cost"
  )
  
  # Rename and compute deltas
  setnames(out_b, c("0_0", "1_0", "0_1", "1_1"),
           c("cost_neither", "cost_hiv_only", "cost_sud_only", "cost_hiv_sud"))
  
  out_b[, `:=`(
    delta_hiv_only = cost_hiv_only - cost_neither,
    delta_sud_only = cost_sud_only - cost_neither,
    delta_hiv_sud  = cost_hiv_sud - cost_neither,
    bootstrap_iter = b
  )]
  
  # Write output for this iteration
  boot_out_path <- file.path(bootstrap_chunks_output_folder, sprintf("bootstrap_iter_%03d.parquet", b))
  write_parquet(out_b, boot_out_path)
  cat("Written:", boot_out_path, "\n")
  
  # Cleanup
  rm(df_boot, df_gamma_input, mod_logit, mod_gamma, out_b)
  gc(verbose = FALSE)
}

##----------------------------------------------------------------
## 5. Create Bootstrap Summary Output
##----------------------------------------------------------------

# Combine all bootstrap iterations
boot_combined <- open_dataset(bootstrap_chunks_output_folder) %>% collect()

# Create bootstrap bin summary
df_bins_summary <- df_bins_master %>%
  group_by(acause_lvl2, race_cd, age_group_years_start, toc_fact) %>%
  summarise(total_row_count = sum(row_count, na.rm=TRUE), .groups = "drop")

# Create main bootstrap output summary, means, quantiles, deltas 
df_summary <- boot_combined %>%
  group_by(acause_lvl2, race_cd, age_group_years_start, toc_fact) %>%
  summarise(
    mean_cost_neither    = mean(cost_neither, na.rm = TRUE),
    mean_cost_hiv_only   = mean(cost_hiv_only, na.rm = TRUE),
    mean_cost_sud_only   = mean(cost_sud_only, na.rm = TRUE),
    mean_cost_hiv_sud    = mean(cost_hiv_sud, na.rm = TRUE),
    
    lower_ci_neither     = quantile(cost_neither, 0.025),
    upper_ci_neither     = quantile(cost_neither, 0.975),
    lower_ci_hiv_only    = quantile(cost_hiv_only, 0.025),
    upper_ci_hiv_only    = quantile(cost_hiv_only, 0.975),
    lower_ci_sud_only    = quantile(cost_sud_only, 0.025),
    upper_ci_sud_only    = quantile(cost_sud_only, 0.975),
    lower_ci_hiv_sud     = quantile(cost_hiv_sud, 0.025),
    upper_ci_hiv_sud     = quantile(cost_hiv_sud, 0.975),
    
    mean_delta_hiv_only   = mean(delta_hiv_only, na.rm=TRUE),
    mean_delta_sud_only   = mean(delta_sud_only, na.rm=TRUE),
    mean_delta_hiv_sud    = mean(delta_hiv_sud, na.rm=TRUE),
    
    lower_ci_delta_hiv_only   = quantile(delta_hiv_only, 0.025),
    upper_ci_delta_hiv_only   = quantile(delta_hiv_only, 0.975),
    lower_ci_delta_sud_only   = quantile(delta_sud_only, 0.025),
    upper_ci_delta_sud_only   = quantile(delta_sud_only, 0.975),
    lower_ci_delta_hiv_sud    = quantile(delta_hiv_sud, 0.025),
    upper_ci_delta_hiv_sud    = quantile(delta_hiv_sud, 0.975),
    .groups = "drop"
  ) %>%
  left_join(df_bins_summary, by = c("acause_lvl2", "race_cd", "age_group_years_start", "toc_fact"))

# Add in year_id, file_type
df_summary <- df_summary %>%
  mutate(
    year_id = year_id,
    file_type = file_type)

# Rename columns
df_summary <- df_summary %>%
  rename(
    toc                 = toc_fact,
    
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

# Reorder columns
desired_order <- c(
  "acause_lvl2", "race_cd", "toc",
  "mean_cost", "lower_ci", "upper_ci",
  "mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv",
  "mean_cost_sud", "lower_ci_sud", "upper_ci_sud",
  "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud",
  "mean_delta_hiv", "lower_ci_delta_hiv", "upper_ci_delta_hiv",
  "mean_delta_sud",  "lower_ci_delta_sud", "upper_ci_delta_sud",
  "mean_delta_hiv_sud", "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud",
  "total_row_count", "age_group_years_start", "year_id", "file_type"
)
df_summary <- df_summary[, desired_order]

##----------------------------------------------------------------
## 6. Save to CSV
##----------------------------------------------------------------

# Write to CSV
file_out <- generate_filename("bootstrap_marginal_results", ".csv")
out_path <- file.path(bootstrap_results_output_folder, file_out)
write.csv(df_summary, out_path, row.names = FALSE)
cat("Wrote final CSV to:", out_path, "\n")
