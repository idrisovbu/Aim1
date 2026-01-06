##----------------------------------------------------------------
##' Title: 01_launcher_disease_counts.R
##'
##' Purpose:
##' This script launches jobs to count and summarize disease occurrences among other things
#
##'
##' Author: Bulat Idrisov
##' Date: 7/31/25
##----------------------------------------------------------------

##----------------------------------------------------------------
## 0. Clear environment and set library paths
##----------------------------------------------------------------
rm(list = ls())

pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx)
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
## 1. Create parameters CSV file
##----------------------------------------------------------------
# Input location (adjust date as needed based on the run date)
run_date <- "bested" # bested was 20260106

fp_input_data <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation", run_date, "aggregated_by_year")

# List all .parquet files
input_files <- list.files(fp_input_data, pattern = "\\.parquet$", full.names = TRUE)

# Build parameter table
df_params <- data.frame(directory = input_files) %>%
  mutate(
    file_type = ifelse(grepl("F2T", basename(directory)), "F2T", "RX"),
    year_id = as.numeric(sub(".*_(\\d{4})_age\\d+\\.parquet$", "\\1", basename(directory)))
    ) %>%   
  filter(!is.na(year_id)) %>%
  filter(year_id %in% c(2010, 2014, 2015, 2016, 2019)) #%>% # 2010, 2014, 2015, 2016, 2019 -> These years have the full data for all types of care (excl. RX)
#filter(file_type == "RX")  ### remove after run

# Manual df_params override for re-running particular jobs
# df_params <- df_params[c(66, 71), ]

# Write CSV for full job param list
param_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/resources_aim1/")
dir.create(param_dir, recursive = TRUE, showWarnings = FALSE)
fp_parameters <- file.path(param_dir, "B1_disease_counts_parameters_aim1.csv")
fwrite(df_params, fp_parameters)

##----------------------------------------------------------------
## 2. Define variables for job / create output folders
##----------------------------------------------------------------
# Define script and output paths
user <- Sys.info()[["user"]]
script_path <- file.path(h, "repo/Aim1/aim1_scripts/B_analysis/B2_worker/B2_worker_disease_counts.R")
log_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis/logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

##----------------------------------------------------------------
## 3. Submit jobs
##----------------------------------------------------------------
# Submit jobs
jid <- SUBMIT_ARRAY_JOB(
  name = "B1_dx_count",
  script = script_path,
  args = c(fp_parameters),
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "all.q", 
  n_jobs = nrow(df_params),
  memory = "100G", 
  threads = 1,
  time = "0:10:00", # Finishes in ~2 min at most
  ## long.q is 384H (2 weeks) if its under 72 then do all.q #https://docs.cluster.ihme.washington.edu/#hpc-execution-host-hardware-specifications
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F  # T for Testing, F (false) for Full run
)

cat("Submitted", nrow(df_params), "array tasks.\n")

#######################################################
# 
# # Check output for unfinished jobs
# date_output <- "20250724"
# #folder_output <- "01.Summary_Statistics" # Change based on folder you want to check
# folder_output <- "02.Regression_Estimates"
# #folder_output <- "03.Meta_Statistics"
# fp_output <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis/", folder_output, date_output, "/")
# 
# output_files <- list.files(fp_output, pattern = "\\.csv$", full.names = FALSE)
# output_files_df <- as.data.table(output_files)
# 
# output_files_df$year_id <- as.integer(stringr::str_extract(output_files_df$output_files, "(?<=year)\\d{4}"))
# output_files_df$age_group_years_start <- as.integer(stringr::str_extract(output_files_df$output_files, "(?<=age)\\d+"))
# output_files_df$file_type <- str_extract(output_files_df$output_files, "(?<=_)[^_]+(?=_year)")
# 
# # Join to df_params to check which are missing
# df_params_age <- df_params
# df_params_age$age_group_years_start <- as.integer(stringr::str_extract(df_params_age$directory, "(?<=age)\\d+"))
# df_output_check <- left_join(x = df_params_age, y = output_files_df, by = c("year_id", "file_type", "age_group_years_start"))
# 
# # Print missing
# df_missing <- df_output_check %>% filter(is.na(output_files))
# 
# for (i in 1:nrow(df_missing)) {
#   if (nrow(df_missing) == 0) {
#     print("Nothing missing!")
#     break
#   } else {
#     print(paste0("Missing data for: "))
#     print(df_missing[i, "directory"])
#     cat("\n")
#   }
# }
# 
# # 66 (RX 2015 age 65), 71 (RX 2016 age 65) for 01.Summary
# # 71 (RX 2016 age 65) for 03.Meta
