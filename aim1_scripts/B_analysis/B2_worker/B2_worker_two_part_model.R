
##----------------------------------------------------------------
##' Title: 02_runner_two_part_model_F2T.R
##'
##----------------------------------------------------------------

# library(arrow)
# 
# # This works *inside the container* where /Volumes/ is remounted under /mnt/share
# f <- read_parquet("/mnt/share/Volumes/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1/20250413/compiled_F2T_data_2008.parquet")
# 

###
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

if (interactive()) {
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250524/aggregated_by_year/compiled_F2T_data_2010.parquet"
  df <- read_parquet(path) %>% head(100000)
  df <- as.data.table(df)  
  year_id <- 2010
  file_type <- "F2T"
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
}


##----------------------------------------------------------------
## 1. create dir foldes 
##----------------------------------------------------------------
# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Define subfolders for output types
two_part_stats_folder <- file.path(output_folder, "04.Two_Part_Estimates")

# Utility function to create directories recursively if not already present
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Create all necessary directories
ensure_dir_exists(output_folder)
ensure_dir_exists(two_part_stats_folder)


# Create logs subfolder inside summary stats
log_folder <- file.path(two_part_stats_folder, "logs")
ensure_dir_exists(log_folder)


# Function to generate output filenames based on file type and year
generate_filename <- function(prefix, extension) {
  paste0(prefix, "_", file_type, "_year", year_id, extension)
}



##----------------------------------------------------------------
## Convert key variables to factors for modeling purposes
##----------------------------------------------------------------

# Removing rows where acause is "hiv" to avoid tautological interaction with has_hiv flag.
# This allows meaningful interpretation of has_hiv × acause interactions (e.g., HIV's impact on CVD).
# We retain STD-related causes under the same acause_lvl2 group ("HIV/AIDS and sexually transmitted infections"),
# since those still provide interpretable contrasts with the has_hiv flag.
# In the future, consider adjusting the data processing pipeline to separate HIV from other STDs during cause mapping.
df <- df[acause_lvl3 != "hiv"]


# Retaining all SUD and mental health rows for now to preserve modeling consistency across all acause categories.
# While interactions like has_sud × acause (when acause is itself a SUD or mental disorder) are tautological,
# we exclude these from interpretation rather than filtering them out.
# In the future, consider flagging or filtering these rows during preprocessing
# to cleanly separate comorbidity effects from primary SUD/mental diagnoses.



df[, `:=`(
  acause    = factor(acause),
  race_cd   = factor(race_cd),
  sex_id    = factor(sex_id),
  has_hiv   = factor(has_hiv, levels = c(0, 1)),
  has_sud   = factor(has_sud, levels = c(0, 1)),
  has_hepc  = factor(has_hepc, levels = c(0, 1)),
  has_cost  = factor(has_cost, levels = c(0, 1))
)]


##----------------------------------------------------------------
## Build bin distribution from the full data
##----------------------------------------------------------------

# Convert to tibble for dplyr-based modeling and bootstrapping
df <- as_tibble(df)



df_bins_master <- df %>%
  group_by(acause, race_cd, sex_id, age_group_years_start, toc, code_system) %>%
  summarise(bin_count = n(), .groups="drop") %>%
  group_by(acause, race_cd, age_group_years_start, toc, code_system) %>%
  mutate(prop_bin = bin_count / sum(bin_count, na.rm = TRUE)) %>%
  ungroup()


grid_input_master <- df_bins_master %>%
  distinct(acause, race_cd, sex_id, age_group_years_start, toc, code_system) %>%
  crossing(
    has_hiv = factor(c(0, 1), levels = levels(df$has_hiv)),
    has_sud = factor(c(0, 1), levels = levels(df$has_sud))
  ) %>%
  left_join(df_bins_master, by = c("acause", "race_cd", "sex_id", "age_group_years_start", "toc", "code_system"))

  # crossing(has_hiv = factor(c(0, 1), levels = levels(df$has_hiv))) %>%
  # left_join(df_bins_master, by = c("acause", "race_cd", "sex_id", "age_group_years_start", "toc", "code_system"))

##----------------------------------------------------------------
## BOOTSTRAP
##----------------------------------------------------------------
boot_chunks_folder <- file.path(two_part_stats_folder, "boot_chunks", generate_filename("boot", ""))
dir.create(boot_chunks_folder, showWarnings = FALSE, recursive = TRUE)


B <- 2 #F2T prcessed on 40
set.seed(123)

# To remove from storing in memory
#all_boot_list <- list()

for (b in seq_len(B)) {
  cat("Bootstrap iteration:", b, "/", B, "\n")
  
  # Resample
  df_boot <- df %>% sample_frac(size = 1, replace = TRUE) %>%
    mutate(
      acause  = factor(acause, levels = levels(df$acause), exclude = NULL),
      race_cd = factor(race_cd, levels = levels(df$race_cd), exclude = NULL),
      sex_id  = factor(sex_id, levels = levels(df$sex_id), exclude = NULL),
      has_hiv = factor(has_hiv, levels = levels(df$has_hiv), exclude = NULL),
      has_sud = factor(has_sud, levels = levels(df$has_sud), exclude = NULL)
      #has_hiv = factor(has_hiv, levels = levels(df$has_hiv), exclude = NULL)
    )
  
  #check on levls
  n_levels <- nlevels(droplevels(df_boot$has_hiv)) #has_hiv
  if (n_levels < 2) {
    cat(" Skipping bootstrap iteration", b, "- only", n_levels, "level(s) in has_hiv\n")
    next
  }
  
  # logistic regression
  mod_logit <- glm(
    has_cost ~ acause * has_hiv + acause * has_sud + race_cd + sex_id,
    #has_cost ~ acause*has_hiv + race_cd + sex_id,
    data=df_boot,
    family=binomial(link="logit")
  )
  
  # Gamma regression on positive cost only, with 99.5% cap
  df_gamma_input <- df_boot %>%
    filter(tot_pay_amt > 0) %>%
    mutate(
      tot_pay_amt = pmin(tot_pay_amt, quantile(tot_pay_amt, 0.995, na.rm = TRUE))
    ) %>%
    droplevels()
  
  mod_gamma <- glm(
    tot_pay_amt ~ acause * has_hiv + acause * has_sud + race_cd + sex_id,
    #tot_pay_amt ~ acause*has_hiv + race_cd + sex_id,
    data = df_gamma_input,
    family = Gamma(link = "log"),
    control = glm.control(maxit = 100)
  )
  
  
  
  # predictions
  tmp <- grid_input_master %>% 
    mutate(
      prob_has_cost = predict(mod_logit, newdata=., type="response"),
      cost_if_pos   = predict(mod_gamma, newdata=., type="response"),
      exp_cost      = prob_has_cost * cost_if_pos
    )
  
  # summarise bootstrap iteration
  # Ensure has_hiv has both levels so pivot_wider works correctly


  # Ensure has_hiv has both levels so pivot_wider works correctly
  #tmp <- tmp %>%mutate(has_hiv = factor(has_hiv, levels = c(0, 1)))
  tmp <- tmp %>%
    mutate(
      has_hiv = factor(has_hiv, levels = c(0, 1)),
      has_sud = factor(has_sud, levels = c(0, 1))
    )
  
  # Summarise bootstrap iteration
  # out_b <- tmp %>%
  #   group_by(acause, race_cd, has_hiv, age_group_years_start, toc, code_system) %>%
  #   summarise(
  #     cost_est = sum(exp_cost * prop_bin, na.rm = TRUE),
  #     .groups = "drop"
  #   ) %>%
  #   pivot_wider(
  #     id_cols = c(acause, race_cd, age_group_years_start, toc, code_system),
  #     names_from = has_hiv,
  #     values_from = cost_est,
  #     names_prefix = "cost_hiv_",
  #     values_fn = sum  # ensures numeric output
  #   ) %>%
  #   mutate(
  #     cost_delta     = cost_hiv_1 - cost_hiv_0,
  #     bootstrap_iter = b
  #   )
  # out_b <- tmp %>%
  #   group_by(acause, race_cd, has_hiv, has_sud, age_group_years_start, toc, code_system) %>%
  #   summarise(
  #     cost_est = sum(exp_cost * prop_bin, na.rm = TRUE),
  #     .groups = "drop"
  #   ) %>%
  #   pivot_wider(
  #     id_cols = c(acause, race_cd, age_group_years_start, toc, code_system),
  #     names_from = c(has_hiv, has_sud),
  #     values_from = cost_est,
  #     names_prefix = "cost_hiv",
  #     names_sep = "_"
  #   ) %>%
  #   mutate(
  #     delta_hiv     = cost_hiv1_0 - cost_hiv0_0,
  #     delta_sud     = cost_hiv0_1 - cost_hiv0_0,
  #     delta_double  = cost_hiv1_1 - cost_hiv0_0,
  #     bootstrap_iter = b
  #   )
  out_b <- tmp %>%
    group_by(acause, race_cd, has_hiv, has_sud, age_group_years_start, toc, code_system) %>%
    summarise(cost_est = sum(exp_cost * prop_bin, na.rm = TRUE), .groups = "drop") %>%
    pivot_wider(
      id_cols = c(acause, race_cd, age_group_years_start, toc, code_system),
      names_from = c(has_hiv, has_sud),
      values_from = cost_est,
      names_prefix = "cost_hiv",
      names_sep = "_"
    ) %>%
    rename(
      cost_neither   = cost_hiv0_0,
      cost_hiv_only  = cost_hiv1_0,
      cost_sud_only  = cost_hiv0_1,
      cost_hiv_sud   = cost_hiv1_1
    ) %>%
    mutate(
      delta_hiv_only   = cost_hiv_only - cost_neither,
      delta_sud_only   = cost_sud_only - cost_neither,
      delta_hiv_sud    = cost_hiv_sud - cost_neither,
      bootstrap_iter   = b
    )
  
  
  #again removing from saving the list
  #all_boot_list[[b]] <- out_b
  boot_out_path <- file.path(boot_chunks_folder, sprintf("bootstrap_iter_%03d.parquet", b))
  write_parquet(out_b, boot_out_path)
  cat("Written:", boot_out_path, "\n")
  #memory clean-up
  rm(df_boot, df_gamma_input, mod_logit, mod_gamma, tmp, out_b)
  gc()
  
}

boot_combined <- open_dataset(boot_chunks_folder) %>% collect()
#boot_combined <- bind_rows(all_boot_list)


# bin summary (outside loop) — FIXED
df_bins_summary <- df_bins_master %>%
  group_by(acause, race_cd, age_group_years_start, toc, code_system) %>%
  summarise(avg_bin_count = sum(bin_count, na.rm=TRUE), .groups = "drop")



df_summary <- boot_combined %>%
  group_by(acause, race_cd, age_group_years_start, toc, code_system) %>%
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
  left_join(df_bins_summary, by = c("acause", "race_cd", "age_group_years_start", "toc", "code_system"))


df_all_race <- df_summary %>%
  group_by(acause) %>%
  summarise(
    race_cd = "all_race",
    
    mean_cost_neither    = sum(mean_cost_neither * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    mean_cost_hiv_only   = sum(mean_cost_hiv_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    mean_cost_sud_only   = sum(mean_cost_sud_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    mean_cost_hiv_sud    = sum(mean_cost_hiv_sud * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    
    lower_ci_neither     = sum(lower_ci_neither * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_neither     = sum(upper_ci_neither * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    lower_ci_hiv_only    = sum(lower_ci_hiv_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_hiv_only    = sum(upper_ci_hiv_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    lower_ci_sud_only    = sum(lower_ci_sud_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_sud_only    = sum(upper_ci_sud_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    lower_ci_hiv_sud     = sum(lower_ci_hiv_sud * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_hiv_sud     = sum(upper_ci_hiv_sud * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    
    mean_delta_hiv_only   = sum(mean_delta_hiv_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    mean_delta_sud_only   = sum(mean_delta_sud_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    mean_delta_hiv_sud    = sum(mean_delta_hiv_sud * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    
    lower_ci_delta_hiv_only   = sum(lower_ci_delta_hiv_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_delta_hiv_only   = sum(upper_ci_delta_hiv_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    lower_ci_delta_sud_only   = sum(lower_ci_delta_sud_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_delta_sud_only   = sum(upper_ci_delta_sud_only * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    lower_ci_delta_hiv_sud    = sum(lower_ci_delta_hiv_sud * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    upper_ci_delta_hiv_sud    = sum(upper_ci_delta_hiv_sud * avg_bin_count, na.rm=TRUE) / sum(avg_bin_count, na.rm=TRUE),
    
    avg_bin_count = sum(avg_bin_count, na.rm=TRUE),
    .groups = "drop"
  )




# Now bind that row with your existing per-race rows:
df_summary <- bind_rows(df_summary, df_all_race)


# final metadata and file output
df_summary2 <- df_summary %>%
  mutate(
    year_id = ifelse(race_cd == "all_race", year_id, year_id),  # could also just assign once
    file_type = ifelse(race_cd == "all_race", file_type, file_type),
    age_group_years_start = ifelse(race_cd == "all_race", "all_age", age_group_years_start),
    toc = ifelse(race_cd == "all_race", "all_toc", toc),
    code_system = ifelse(race_cd == "all_race", "ICD", code_system)  # or make this dynamic
  )


file_out <- generate_filename("bootstrap_marginal_results", ".csv")
out_path <- file.path(two_part_stats_folder, file_out)
write.csv(df_summary2, out_path, row.names=FALSE)
cat("Wrote final CSV to:", out_path, "\n")
