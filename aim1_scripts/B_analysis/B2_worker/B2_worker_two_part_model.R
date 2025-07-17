
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
  path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250620/aggregated_by_year/compiled_RX_data_2010_age65.parquet"
  #path <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250620/aggregated_by_year/compiled_F2T_data_2010_age65.parquet"
  df <- read_parquet(path) %>% sample_n(10000)
  df <- as.data.table(df)  
  year_id <- 2010
  file_type <- "F2T"
  age_group_years_start <- df$age_group_years_start[1]
  
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
## 1. create dir foldes 
##----------------------------------------------------------------
# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Define subfolders for output types
two_part_stats_folder <- file.path(output_folder, "04.Two_Part_Estimates")

# Utility function to create directories recursively
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
# generate_filename <- function(prefix, extension) {
#   paste0(prefix, "_", file_type, "_year", year_id, extension)
# }

generate_filename <- function(prefix, extension) {
  paste0(prefix, "_", file_type, "_year", year_id, "_age", age_group_years_start, extension)
}



##----------------------------------------------------------------
## Convert key variables to factors for modeling purposes
##----------------------------------------------------------------
# colnames(df)
# str(df)
# --- Assign explicit factor variable for toc ---
toc_levels <- c("AM", "ED", "HH", "IP", "NF", "RX")  # add RX if it's a possible toc

df[, toc_fact := factor(as.character(toc), levels = toc_levels)]   # always convert to character first!

df[, `:=`(
  acause_lvl2   = factor(acause_lvl2),
  race_cd       = factor(race_cd),
  sex_id        = factor(sex_id),
  #toc_fact      = factor(toc_fact),                
  has_hiv       = factor(has_hiv, levels = c(0, 1)),
  has_sud       = factor(has_sud, levels = c(0, 1)),
  has_hepc      = factor(has_hepc, levels = c(0, 1)),
  has_cost      = factor(has_cost, levels = c(0, 1))
)]




##----------------------------------------------------------------
## Build bin distribution from the full data
##----------------------------------------------------------------
# colnames(df)
# unique(df$age_group_years_start)

# Convert to tibble for dplyr-based modeling and bootstrapping
df <- as_tibble(df)


df_bins_master <- df %>%
  group_by(acause_lvl2, race_cd, sex_id, age_group_years_start, toc_fact) %>%
  summarise(row_count = n(), .groups="drop") %>%
  group_by(acause_lvl2, race_cd, age_group_years_start, toc_fact) %>%
  mutate(prop_bin = row_count / sum(row_count, na.rm = TRUE)) %>%
  ungroup()


grid_input_master <- df_bins_master %>%
  distinct(acause_lvl2, race_cd, sex_id, age_group_years_start, toc_fact) %>%
  crossing(
    has_hiv = factor(c(0, 1), levels = levels(df$has_hiv)),
    has_sud = factor(c(0, 1), levels = levels(df$has_sud))
  ) %>%
  left_join(df_bins_master, by = c("acause_lvl2", "race_cd", "sex_id", "age_group_years_start", "toc_fact"))

#  
# df_bins_master %>%
#   group_by(acause_lvl2, race_cd, age_group_years_start, toc) %>%
#   summarise(total_prop = sum(prop_bin))


##----------------------------------------------------------------
## BOOTSTRAP
##----------------------------------------------------------------
boot_chunks_folder <- file.path(two_part_stats_folder, "boot_chunks", generate_filename("boot", ""))
dir.create(boot_chunks_folder, showWarnings = FALSE, recursive = TRUE)


B <- 40 #F2T prcessed on 40
set.seed(123)

# To remove from storing in memory
#all_boot_list <- list()

for (b in seq_len(B)) {
  cat("Bootstrap iteration:", b, "/", B, "\n")
  
  # Resample
  df_boot <- df %>% sample_frac(size = 1, replace = TRUE) %>%
    mutate(
      acause_lvl2  = factor(acause_lvl2, levels = levels(df$acause_lvl2), exclude = NULL),
      race_cd      = factor(race_cd, levels = levels(df$race_cd), exclude = NULL),
      sex_id       = factor(sex_id, levels = levels(df$sex_id), exclude = NULL),
      toc_fact          = factor(toc_fact, levels = levels(df$toc_fact), exclude = NULL),   # <-- ADD THIS
      has_hiv      = factor(has_hiv, levels = levels(df$has_hiv), exclude = NULL),
      has_sud      = factor(has_sud, levels = levels(df$has_sud), exclude = NULL)
    )
  
  
  
  #check on levls
  n_levels <- nlevels(droplevels(df_boot$has_hiv)) #has_hiv
  if (n_levels < 2) {
    cat(" Skipping bootstrap iteration", b, "- only", n_levels, "level(s) in has_hiv\n")
    next
  }
  
  # logistic regression
  mod_logit <- glm(
    has_cost ~ acause_lvl2 * has_hiv + acause_lvl2 * has_sud + race_cd + sex_id + toc_fact,
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
  
  #consider doing toc_fact as an interaction 
  
  mod_gamma <- glm(
    tot_pay_amt ~ acause_lvl2 * has_hiv + acause_lvl2 * has_sud + race_cd + sex_id + toc_fact, 
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
  

  out_b <- tmp %>%
    group_by(acause_lvl2, race_cd, has_hiv, has_sud, age_group_years_start, toc_fact) %>%
    summarise(cost_est = sum(exp_cost * prop_bin, na.rm = TRUE), .groups = "drop") %>%
    pivot_wider(
      id_cols = c(acause_lvl2, race_cd, age_group_years_start, toc_fact),
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
  group_by(acause_lvl2, race_cd, age_group_years_start, toc_fact) %>%
  summarise(avg_row_count = sum(row_count, na.rm=TRUE), .groups = "drop")

###for the future check bootstrap for finite populations

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


# df_all_race <- df_summary %>%
#   group_by(acause_lvl2, age_group_years_start, toc_fact) %>%
#   summarise(
#     race_cd = "all_race",
#     
#     mean_cost_neither    = sum(mean_cost_neither * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     mean_cost_hiv_only   = sum(mean_cost_hiv_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     mean_cost_sud_only   = sum(mean_cost_sud_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     mean_cost_hiv_sud    = sum(mean_cost_hiv_sud * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     
#     lower_ci_neither     = sum(lower_ci_neither * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_neither     = sum(upper_ci_neither * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     lower_ci_hiv_only    = sum(lower_ci_hiv_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_hiv_only    = sum(upper_ci_hiv_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     lower_ci_sud_only    = sum(lower_ci_sud_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_sud_only    = sum(upper_ci_sud_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     lower_ci_hiv_sud     = sum(lower_ci_hiv_sud * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_hiv_sud     = sum(upper_ci_hiv_sud * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     
#     mean_delta_hiv_only   = sum(mean_delta_hiv_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     mean_delta_sud_only   = sum(mean_delta_sud_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     mean_delta_hiv_sud    = sum(mean_delta_hiv_sud * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     
#     lower_ci_delta_hiv_only   = sum(lower_ci_delta_hiv_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_delta_hiv_only   = sum(upper_ci_delta_hiv_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     lower_ci_delta_sud_only   = sum(lower_ci_delta_sud_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_delta_sud_only   = sum(upper_ci_delta_sud_only * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     lower_ci_delta_hiv_sud    = sum(lower_ci_delta_hiv_sud * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     upper_ci_delta_hiv_sud    = sum(upper_ci_delta_hiv_sud * avg_row_count, na.rm=TRUE) / sum(avg_row_count, na.rm=TRUE),
#     
#     avg_row_count = sum(avg_row_count, na.rm=TRUE),
#     .groups = "drop"
#   )




# Now bind that row with your existing per-race rows:
#df_summary <- bind_rows(df_summary, df_all_race)




# df_summary2 <- df_summary %>%
#   mutate(
#     year_id = year_id,  # or drop this line entirely if it's already present
#     file_type = file_type,  # same as above
#     age_group_years_start = ifelse(race_cd == "all_race", "all_age", age_group_years_start),
#     toc = ifelse(race_cd == "all_race", "all_toc", toc)
#   )



# df_summary2 <- df_summary %>%
#   mutate(
#     year_id = year_id,
#     file_type = file_type,
#     # Aggregate toc label
#     toc = ifelse(race_cd == "all_race", paste0("all_toc_", age_group_years_start), as.character(toc_fact)),
#     # Aggregate race label
#     race_cd = ifelse(race_cd == "all_race", paste0("all_race_", age_group_years_start), race_cd),
#     # Do NOT touch age_group_years_start -- always keep it as-is!
#     age_group_years_start = age_group_years_start
#   )


df_summary2 <- df_summary2 %>% select(-toc_fact)


df_summary2 <- df_summary2 %>%
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


desired_order <- c(
  "acause_lvl2", "race_cd", "toc",
  "mean_cost", "lower_ci", "upper_ci",
  "mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv",
  "mean_cost_sud", "lower_ci_sud", "upper_ci_sud",
  "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud",
  "mean_delta_hiv", "lower_ci_delta_hiv", "upper_ci_delta_hiv",
  "mean_delta_sud",  "lower_ci_delta_sud", "upper_ci_delta_sud",
  "mean_delta_hiv_sud", "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud",
  "avg_row_count", "age_group_years_start", "year_id", "file_type"
)
df_summary2 <- df_summary2[, desired_order]


# Write to CSV
file_out <- generate_filename("bootstrap_marginal_results", ".csv")
out_path <- file.path(two_part_stats_folder, file_out)
write.csv(df_summary2, out_path, row.names = FALSE)
cat("Wrote final CSV to:", out_path, "\n")

