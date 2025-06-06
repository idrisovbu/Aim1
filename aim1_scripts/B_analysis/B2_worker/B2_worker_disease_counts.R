##----------------------------------------------------------------
##' Title: B2_worker_disease_counts.R
##'
##' Author:      Bulat Idrisov
##' Date:        2025-05-04
##' ----------------------------------------------------------------



rm(list = ls())
pacman::p_load(
  dplyr, tibble, broom, readr, stringr, purrr,
  openxlsx, RMySQL, data.table, ini, DBI
)

#dplyr,tidyr
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if ("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

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


if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250524/aggregated_by_year/compiled_F2T_data_2010.parquet"
  df <- read_parquet(path)
  df_input <- as.data.table(df)  
  year_id <- 2010
  file_type <- "FT"
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
}



##----------------------------------------------------------------
## 0. Create output folders
##----------------------------------------------------------------
colnames(df)
colnames(df_FT)

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Define subfolders for output types
summary_stats_folder <- file.path(output_folder, "01.Summary_Statistics")
regression_estimates_folder <- file.path(output_folder, "02.Regression_Estimates")
meta_stats_folder <- file.path(output_folder, "03.Meta_Statistics")

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
ensure_dir_exists(meta_stats_folder)

# Create logs subfolder inside summary stats
log_folder <- file.path(summary_stats_folder, "logs")
ensure_dir_exists(log_folder)

# Construct log file path (optional)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
if (is.na(task_id)) task_id <- "interactive"
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
log_file <- file.path(log_folder, paste0("error_log_", task_id, "_", timestamp, ".txt"))

# Function to generate output filenames based on file type and year
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_", file_type, "_year", year_id, extension)
}

##----------------------------------------------------------------
## 1. Metadata summary
##----------------------------------------------------------------

meta_stats <- df_input[, .(
  year_id = unique(year_id),
  file_type = unique(file_type),
  total_unique_bene = uniqueN(bene_id),
  total_bene_acause_combo = uniqueN(.SD[, .(bene_id, acause)]),
  mean_any_cost = mean(has_cost, na.rm = TRUE),
  hiv_unique_bene = uniqueN(bene_id[has_hiv == 1]),
  sud_unique_bene = uniqueN(bene_id[has_sud == 1]),
  hepc_unique_bene = uniqueN(bene_id[has_hepc == 1])
), by = .(toc, age_group_years_start, code_system)]


# Save metadata
file_out_meta <- generate_filename("summary_meta", ".csv")
out_path_meta <- file.path(meta_stats_folder, file_out_meta)
fwrite(meta_stats, out_path_meta)

cat("Meta statistics saved to:", out_path_meta, "\n")


##----------------------------------------------------------------
## 2. Summary statistics by disease group
##----------------------------------------------------------------
# 

# Cross-tab: count of beneficiaries per group
cross_tab_dt <- df_input[, .(n_benes_per_group = .N),
                         by = .(acause, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start, code_system)
]

# Utilization summary
acause_utilization_dt <- df_input[, .(
  avg_encounters_per_bene = mean(unique_encounters),
  sum_encounters_per_group = sum(unique_encounters)
), by = .(acause, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start, code_system)]

# Cost summary
acause_cost_dt <- df_input[, .(
  avg_cost_per_bene = mean(tot_pay_amt, na.rm = TRUE),
  max_cost_per_bene = max(tot_pay_amt, na.rm = TRUE),
  quantile_99_cost_per_bene = quantile(tot_pay_amt, probs = 0.99, na.rm = TRUE),
  sum_cost_per_group = sum(tot_pay_amt, na.rm = TRUE)
), by = .(acause, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start, code_system)]

# Merge summaries
summary_dt <- merge(acause_cost_dt, cross_tab_dt,
                    by = c("acause", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start", "code_system"),
                    all = TRUE)

summary_dt <- merge(summary_dt, acause_utilization_dt,
                    by = c("acause", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start", "code_system"),
                    all = TRUE)

# Add metadata
summary_dt[, `:=`(year_id = year_id, file_type = file_type)]

# Set column order
setcolorder(summary_dt, c(
  "acause", "has_hiv", "has_sud", "has_hepc", "race_cd", "toc", "age_group_years_start", "code_system",
  "avg_cost_per_bene", "max_cost_per_bene", "quantile_99_cost_per_bene", "sum_cost_per_group",
  "n_benes_per_group", "avg_encounters_per_bene", "sum_encounters_per_group",
  "year_id", "file_type"
))

# Save
file_out_summary <- generate_filename("summary_stats", ".csv")
out_path_summary <- file.path(summary_stats_folder, file_out_summary)
fwrite(summary_dt, out_path_summary)

cat("Summary statistics saved to:", out_path_summary, "\n")

##----------------------------------------------------------------
## Run logistic and gamma regressions
##----------------------------------------------------------------
# unique(df_input$toc)
# 
# # Ensure df_input is a tibble
# df_input <- as_tibble(df_input)
# 
# # Set interaction formula explicitly
# logit_formula <- has_cost ~ acause * has_hiv + race_cd + sex_id
# gamma_formula <- tot_pay_amt ~ acause * has_hiv + race_cd + sex_id
# 
# # Fit logit model
# mod_logit <- glm(
#   formula = logit_formula,
#   data = df_input,
#   family = binomial(link = "logit")
# )
# 
# # Prepare positive-cost subset with outlier capping
# df_positive_cost <- df_input %>%
#   filter(tot_pay_amt > 0) %>%
#   mutate(
#     tot_pay_amt = pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))
#   ) %>%
#   droplevels()
# 
# # Fit gamma model
# mod_gamma <- glm(
#   formula = gamma_formula,
#   data = df_positive_cost,
#   family = Gamma(link = "log"),
#   control = glm.control(maxit = 100)
# )
# 
# # Extract coefficients and p-values using broom
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
# # Merge and annotate
# regression_results <- full_join(logit_df, gamma_df, by = "variable") %>%
#   mutate(
#     interaction_dropped = if_else(
#       is.na(estimate_gamma) & str_detect(variable, ":"), TRUE, FALSE
#     ),
#     year_id = year_id,
#     file_type = file_type
#   ) %>%
#   select(variable, estimate_logit, p_logit, estimate_gamma, p_gamma,
#          interaction_dropped, year_id, file_type)
# 
# # Save regression results
# file_out_regression <- generate_filename("regression_results", ".csv")
# out_path_regression <- file.path(regression_estimates_folder, file_out_regression)
# write_csv(regression_results, out_path_regression)
# cat("✅ Regression results saved to:", out_path_regression, "\n")
# 
# # Save full model summaries
# save_model_summary <- function(model, model_name, folder) {
#   fname <- generate_filename(paste0("full_model_summary_", model_name), ".txt")
#   fpath <- file.path(folder, fname)
#   writeLines(capture.output(summary(model)), fpath)
#   cat("Saved full model summary to:", fpath, "\n")
# }
# 
# save_model_summary(mod_logit, "logit", regression_estimates_folder)
# save_model_summary(mod_gamma, "gamma", regression_estimates_folder)
# 
# message("Job ended at: ", Sys.time())


### Us this below 

# Ensure df_input is a tibble
df_input <- as_tibble(df_input)

# Set regression formulas
logit_formula <- has_cost ~ acause * has_hiv + race_cd + sex_id
gamma_formula <- tot_pay_amt ~ acause * has_hiv + race_cd + sex_id

# Fit logistic model
mod_logit <- glm(
  formula = logit_formula,
  data = df_input,
  family = binomial(link = "logit")
)

# Cap outliers and fit gamma model
df_positive_cost <- df_input %>%
  filter(tot_pay_amt > 0) %>%
  mutate(tot_pay_amt = pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))) %>%
  droplevels()

mod_gamma <- glm(
  formula = gamma_formula,
  data = df_positive_cost,
  family = Gamma(link = "log"),
  control = glm.control(maxit = 100)
)

# Create prediction grid with metadata
prediction_grid <- df_input %>%
  distinct(acause, race_cd, sex_id, age_group_years_start, toc, code_system) %>%
  crossing(has_hiv = factor(c(0, 1), levels = levels(df_input$has_hiv)))

# Predict expected cost
prediction_grid <- prediction_grid %>%
  mutate(
    prob_has_cost = predict(mod_logit, newdata = ., type = "response"),
    cost_if_pos   = predict(mod_gamma, newdata = ., type = "response"),
    exp_cost      = prob_has_cost * cost_if_pos
  )

# Summarize predictions
regression_summary <- prediction_grid %>%
  group_by(acause, race_cd, age_group_years_start, toc, code_system, has_hiv) %>%
  summarise(predicted_cost = mean(exp_cost), .groups = "drop") %>%
  pivot_wider(
    names_from = has_hiv,
    values_from = predicted_cost,
    names_prefix = "cost_hiv_"
  ) %>%
  mutate(
    cost_delta = cost_hiv_1 - cost_hiv_0,
    year_id = year_id,
    file_type = file_type
  )

# Save regression summary table
file_out_summary <- generate_filename("regression_summary_estimates", ".csv")
out_path_summary <- file.path(regression_estimates_folder, file_out_summary)
write_csv(regression_summary, out_path_summary)
cat("✅ Summary predictions saved to:", out_path_summary, "\n")

# Extract coefficients and p-values
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

logit_df <- extract_all_coefs(mod_logit, "logit")
gamma_df <- extract_all_coefs(mod_gamma, "gamma")

# Merge and annotate regression coefficients
regression_results <- full_join(logit_df, gamma_df, by = "variable") %>%
  mutate(
    interaction_dropped = if_else(is.na(estimate_gamma) & str_detect(variable, ":"), TRUE, FALSE),
    year_id = year_id,
    file_type = file_type
  ) %>%
  select(variable, estimate_logit, p_logit, estimate_gamma, p_gamma,
         interaction_dropped, year_id, file_type)

# Save regression coefficients
file_out_regression <- generate_filename("regression_results", ".csv")
out_path_regression <- file.path(regression_estimates_folder, file_out_regression)
write_csv(regression_results, out_path_regression)
cat("✅ Regression coefficients saved to:", out_path_regression, "\n")

# Save full model summaries
save_model_summary <- function(model, model_name, folder) {
  fname <- generate_filename(paste0("full_model_summary_", model_name), ".txt")
  fpath <- file.path(folder, fname)
  writeLines(capture.output(summary(model)), fpath)
  cat("📄 Full model summary saved to:", fpath, "\n")
}

save_model_summary(mod_logit, "logit", regression_estimates_folder)
save_model_summary(mod_gamma, "gamma", regression_estimates_folder)

message("Job ended at: ", Sys.time())

