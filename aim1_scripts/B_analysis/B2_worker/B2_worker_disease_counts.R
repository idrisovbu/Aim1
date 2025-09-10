##----------------------------------------------------------------
##' Title: B2_worker_disease_counts.R
##' Description: Creates summary statistics and summary model outputs
##' Author:      Bulat Idrisov
##' Date:        2025-05-04
##' Outputs: /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/01.Summary_Statistics/<date>
##'          /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/02.Regression_Estimates/<date>
##'          /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/03.Meta_Statistics/<date>
##' ----------------------------------------------------------------

rm(list = ls())
pacman::p_load(
  dplyr, tibble, broom, readr, stringr, purrr,
  openxlsx, RMySQL, data.table, ini, DBI
)

library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if ("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))

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

##----------------------------------------------------------------
# 0. Read in data from SLURM job submission
##----------------------------------------------------------------

if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/bested/aggregated_by_year/compiled_F2T_data_2019_age80.parquet"
  df_input <- open_dataset(path) %>% collect() %>% as.data.table() 
  year_id <- df_input$year_id[1]
  file_type <- if (str_detect(path, "F2T")) "F2T" else "RX"
  age_group_years_start <- df_input$age_group_years_start[1]
  
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
  df_input <- read_parquet(fp_input) %>% as.data.table()
  age_group_years_start <- df_input$age_group_years_start[1]
}

##----------------------------------------------------------------
## 0.1. Functions
##----------------------------------------------------------------

# Utility function to create directories recursively if not already present
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Function to generate output filenames based on file type and year
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_", file_type, "_year", year_id, "_age", age_group_years_start, extension)
}

##----------------------------------------------------------------
## 1. Create output folders
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder <- format(Sys.time(), "%Y%m%d")

# Define subfolders for output types
summary_stats_folder <- file.path(base_output_dir, "01.Summary_Statistics/", date_folder)
regression_estimates_folder <- file.path(base_output_dir, "02.Regression_Estimates/", date_folder)
meta_stats_folder <- file.path(base_output_dir, "03.Meta_Statistics/", date_folder)

# Define log folder
log_folder <- file.path(base_output_dir, "logs")

# Create all necessary directories
ensure_dir_exists(summary_stats_folder)
ensure_dir_exists(regression_estimates_folder)
ensure_dir_exists(meta_stats_folder)
ensure_dir_exists(log_folder)

# Construct log file path
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
if (is.na(task_id)) task_id <- "interactive"
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file <- file.path(log_folder, paste0("error_log_", task_id, "_", timestamp, ".txt"))

##----------------------------------------------------------------
## 1. Metadata summary
##----------------------------------------------------------------

meta_stats_2019 <- df_input[, .(
  year_id = unique(year_id),
  file_type = unique(file_type),
  total_unique_bene = uniqueN(bene_id),
  total_bene_acause_combo = uniqueN(.SD[, .(bene_id, acause_lvl2)]),
  mean_any_cost = mean(has_cost, na.rm = TRUE),
  hiv_unique_bene = uniqueN(bene_id[has_hiv == 1]),
  sud_unique_bene = uniqueN(bene_id[has_sud == 1]),
  hepc_unique_bene = uniqueN(bene_id[has_hepc == 1]),
  hiv_and_sud_unique_bene = uniqueN(bene_id[has_hiv == 1 & has_sud == 1]),      
  hiv_and_hepc_unique_bene = uniqueN(bene_id[has_hiv == 1 & has_hepc == 1]),    
  sud_and_hepc_unique_bene = uniqueN(bene_id[has_sud == 1 & has_hepc == 1]),    
  hiv_sud_hepc_unique_bene = uniqueN(bene_id[has_hiv == 1 & has_sud == 1 & has_hepc == 1]) 
), by = .(toc, age_group_years_start)]



# Save metadata
file_out_meta <- generate_filename("summary_meta", ".csv")
out_path_meta <- file.path(meta_stats_folder, file_out_meta)
fwrite(meta_stats_2019, out_path_meta)

cat("Meta statistics saved to:", out_path_meta, "\n")

##----------------------------------------------------------------
## 2. Summary statistics by disease group
##----------------------------------------------------------------

# Cross-tab: count of beneficiaries per group
cross_tab_dt <- df_input[, .(n_benes_per_group = .N),
                         by = .(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start)
]

# Utilization summary
acause_utilization_dt <- df_input[, .(
  avg_encounters_per_bene = mean(unique_encounters),
  sum_encounters_per_group = sum(unique_encounters)
), by = .(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start)]

# Cost summary
acause_cost_dt <- df_input[, .(
  avg_cost_per_bene = mean(tot_pay_amt, na.rm = TRUE),
  avg_cost_per_encounter = sum(tot_pay_amt) / sum(unique_encounters),
  # Winsorized mean (cutting at 99.5%)
  avg_cost_per_bene_winsorized = {
    cutoff <- quantile(tot_pay_amt, probs = 0.995, na.rm = TRUE)
    mean(pmin(tot_pay_amt, cutoff), na.rm = TRUE)
  },
  max_cost_per_bene = max(tot_pay_amt, na.rm = TRUE),
  quantile_99_cost_per_bene = quantile(tot_pay_amt, probs = 0.99, na.rm = TRUE),
  sum_cost_per_group = sum(tot_pay_amt, na.rm = TRUE)
), by = .(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start)]

# Merge summaries
summary_dt <- merge(acause_cost_dt, cross_tab_dt,
                    by = c("acause_lvl2", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start"),
                    all = TRUE)

summary_dt <- merge(summary_dt, acause_utilization_dt,
                    by = c("acause_lvl2", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start"),
                    all = TRUE)

# Add metadata
summary_dt[, `:=`(year_id = year_id, file_type = file_type)]

# Set column order
setcolorder(summary_dt, c(
  "acause_lvl2", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start",
  "avg_cost_per_bene", "avg_cost_per_encounter","avg_cost_per_bene_winsorized","max_cost_per_bene", "quantile_99_cost_per_bene", "sum_cost_per_group",
  "n_benes_per_group", "avg_encounters_per_bene", "sum_encounters_per_group",
  "year_id", "file_type"
))

# Save
file_out_summary <- generate_filename("summary_stats", ".csv")
out_path_summary <- file.path(summary_stats_folder, file_out_summary)
fwrite(summary_dt, out_path_summary)

cat("Summary statistics saved to:", out_path_summary, "\n")

# ##----------------------------------------------------------------
# ## 3. Run logistic and gamma regressions
# ##----------------------------------------------------------------
# 
# # Ensure factor levels are established
# #toc_levels <- c("AM", "ED", "HH", "IP", "NF", "RX")
# 
# df_input[, `:=`(
#   acause_lvl2   = factor(acause_lvl2),
#   race_cd       = factor(race_cd),
#   sex_id        = factor(sex_id),
#   has_hiv       = factor(has_hiv, levels = c(0, 1)),
#   has_sud       = factor(has_sud, levels = c(0, 1)),
#   has_hepc      = factor(has_hepc, levels = c(0, 1)),
#   has_cost      = factor(has_cost, levels = c(0, 1))
# )]
# ##----------------------------------------------------------------
# ## Check diversity of toc levels (write to log only)
# ##----------------------------------------------------------------
# valid_toc_levels <- df_input[, unique(na.omit(as.character(toc)))]
# n_unique_toc <- length(valid_toc_levels)
# 
# # if (n_unique_toc < 2 & file_type != "RX") {
# #   msg <- paste0(
# #     "❌ REGRESSION HALTED\n",
# #     "Reason: Less than 2 levels of toc present\n",
# #     "Year: ", year_id, " | Age group: ", age_group_years_start, "\n",
# #     "Available toc levels: ", paste(valid_toc_levels, collapse = ", "), "\n",
# #     "Timestamp: ", Sys.time(), "\n"
# #   )
# #   writeLines(msg, con = log_file)
# #   stop("Insufficient toc variation for regression (see log).")
# # }
# 
# # Skip iterations with low factor level diversity
# if (nlevels(droplevels(df_input$has_hiv)) < 2 ||
#     nlevels(droplevels(df_input$has_sud)) < 2 ||
#     nlevels(droplevels(df_input$acause_lvl2)) < 2) {
#   cat("Error - ", b, "- insufficient factor diversity\n")
#   stop("Insufficient factor levels in dataset! Issue caused by has_hiv, has_sud, or acause_lvl2 having less than 2 factor levels")
# }
# 
# #### Set regression formulas
# # Logistic model (toc_fact removed for RX)
# mod_logit <- glm(
#   has_cost ~ acause_lvl2 * has_hiv +
#     acause_lvl2 * has_sud +
#     race_cd + sex_id,
#   data   = df_input,
#   family = binomial(link = "logit"))
# 
# # Gamma input
# df_gamma_input <- df_input[tot_pay_amt > 0]
# df_gamma_input[, tot_pay_amt := pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))]
# 
# # Gamma model 
# mod_gamma <- glm(
#   tot_pay_amt ~ acause_lvl2 * has_hiv +
#     acause_lvl2 * has_sud +
#     race_cd + sex_id,
#   data    = df_gamma_input,
#   family  = Gamma(link = "log"),
#   control = glm.control(maxit = 100)
# )
# 
# 
# ##----------------------------------------------------------------
# ## 4. Save regression coefficients
# ##----------------------------------------------------------------
# 
# # Extract coefficients and p-values
# extract_all_coefs <- function(model, suffix) {
#   tidy(model) %>%
#     filter(term != "(Intercept)") %>%
#     rename(
#       variable = term,
#       !!paste0("estimate_", suffix) := estimate,
#       !!paste0("p_", suffix) := p.value
#     ) %>%
#     select(variable, starts_with("estimate_"), starts_with("p_"))
# }
# 
# logit_df <- extract_all_coefs(mod_logit, "logit")
# gamma_df <- extract_all_coefs(mod_gamma, "gamma")
# 
# # Merge and annotate regression coefficients
# regression_results <- full_join(logit_df, gamma_df, by = "variable") %>%
#   mutate(
#     interaction_dropped = if_else(is.na(estimate_gamma) & str_detect(variable, ":"), TRUE, FALSE),
#     year_id = year_id,
#     file_type = file_type,
#     age_group_years_start = age_group_years_start
#   ) %>%
#   select(variable, estimate_logit, p_logit, estimate_gamma, p_gamma,
#          interaction_dropped, year_id, file_type, age_group_years_start)
# 
# # Save regression coefficients
# file_out_regression <- generate_filename("regression_results", ".csv")
# out_path_regression <- file.path(regression_estimates_folder, file_out_regression)
# write_csv(regression_results, out_path_regression)
# cat("✅ Regression coefficients saved to:", out_path_regression, "\n")
# 
# ##----------------------------------------------------------------
# ## 4. Save Full Model Summaries
# ##----------------------------------------------------------------
# 
# # Save full model summaries
# save_model_summary <- function(model, model_name, folder) {
#   fname <- generate_filename(paste0("full_model_summary_", model_name), ".txt")
#   fpath <- file.path(folder, fname)
#   writeLines(capture.output(summary(model)), fpath)
#   cat("📄 Full model summary saved to:", fpath, "\n")
# }
# 
# save_model_summary(mod_logit, "logit", regression_estimates_folder)
# save_model_summary(mod_gamma, "gamma", regression_estimates_folder)
# 
# message("Job ended at: ", Sys.time())
# 
