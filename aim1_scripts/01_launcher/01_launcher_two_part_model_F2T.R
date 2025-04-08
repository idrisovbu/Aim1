##----------------------------------------------------------------
##' Title: 01_launcher_two_part_model_F2T.R
##'
##' Purpose: 
##' This script launches the runner/worker script (02_worker_two_part_model_F2T.R) 
##' to execute a two-part regression model estimating healthcare costs 
##' for Medicare beneficiaries with HIV and comorbidities.
##'
##' The script:
##'  - Identifies available input data based on file structure
##'  - Generates a CSV with parameter permutations for the runner script
##'  - Submits batch jobs to the computing cluster for processing
##'
##' Inputs:
##'  - Medicare claims data directory (F2T dataset)
##'  - Parameters including year, age, disease category
##'
##' Outputs:
##'  - CSV file (`parameters_aims1.csv`) with filtered data parameters
##'  - Cluster job submissions for regression model execution
##'
##' Author: Bulat Idrisov
##' Date: 2025-03-06
##' Version: 1.0
##' 
##----------------------------------------------------------------


# # Clear environment and set library paths
rm(list = ls())

pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx)
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


# Dynamically read in directories where data is stored to see what data they have in the first place
# Create a table based off their directory hierarchy (use code to scan folder directories to determine available data)
run_id <- 77
fp_input_data <- paste0("/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_",run_id,"/F2T/")
list_input_data <- list.dirs(fp_input_data, recursive = TRUE)

# filter down to folders we are interested in that contain data
list_input_data_subset <- list()

for (i in 1:length(list_input_data)) {
  if (str_count(list_input_data[i], pattern = "age_group_years_start") == 1) {
    list_input_data_subset[length(list_input_data_subset) + 1] <- list_input_data[i]
  }
}

# unlist to flatten list
list_input_data_subset <- unlist(list_input_data_subset)

# create data frame
df_list_input_data_subset <- data.frame(list_input_data_subset)
colnames(df_list_input_data_subset)[colnames(df_list_input_data_subset) == "list_input_data_subset"] <- "directory"

# add additional identifier columns
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(runid = str_extract(directory, "(?<=run_)\\d+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(year_id = str_extract(directory, "(?<=year_id=)\\d+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(age_group_years_start = str_extract(directory, "(?<=age_group_years_start=)\\d+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(code_system = str_extract(directory, "(?<=code_system=)[^/]+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(toc = str_extract(directory, "(?<=toc=)[^/]+"))

# Convert age_group_years_start to numeric if a character string
df_list_input_data_subset$age_group_years_start <- as.numeric(df_list_input_data_subset$age_group_years_start)

# Filter out ages below 65
#df_list_input_data_subset <- df_list_input_data_subset %>%filter(age_group_years_start >= 65)
df_list_input_data_subset$year_id <- as.numeric(df_list_input_data_subset$year_id)

df_list_input_data_subset <- df_list_input_data_subset %>% 
  filter(age_group_years_start >= 65 & age_group_years_start <= 85 & year_id != 2000  & toc != "NF")


# Save the filtered parameters to CSV save list as .csv for runner script to read in based off job
fp_parameters <- paste0(l, "/LU_CMS/DEX/hivsud/aim1/resources_aim1/parameters_aims1.csv")
write.csv(df_list_input_data_subset, file = fp_parameters, row.names = FALSE)

##########################################
# define output directories
##########################################
user <- Sys.info()[["user"]]
# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"
date_folder <- format(Sys.time(), "%Y%m%d")  # Generates folder based on today's date
output_folder <- file.path(base_output_dir, date_folder)

# Define logs directory inside the dated output folder
log_dir <- file.path(output_folder, "logs")

# Function to ensure the logs directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Create only the logs directory
ensure_dir_exists(log_dir)

script_path <- paste0(h, "/repo/dex_us_county/misc/hivsud/aim1_scripts/02_workers/02_worker_two_part_model_F2T.R")

# Define key variables
# user <- Sys.info()[["user"]]
# script_path <- paste0(h, "/repo/dex_us_county/misc/hivsud/02_regressions/02_model_aim1_runner.R")
# output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1/"
# log_dir <- paste0("/mnt/share/limited_use/LU_CMS/DEX/logs/", user)
# 
# # Create output and log directories
# if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
# if (!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)

# Submit jobs to the cluster - Launcher Script for Aim 1 study - All Years, Ages, TOC from fp_parameter
jid <- SUBMIT_ARRAY_JOB(
  name = "aim1_regression",
  script = script_path,
  args = c(fp_parameters), #I'm thinking this is the file path to our arguments (fp_arguments) .csv file we created with the parameter permutations
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "long.q",
  n_jobs = nrow(df_list_input_data_subset),
  #n_jobs = 5, #just for testing purposes
  memory = "100G", #determine based off running a few individual runner scripts and checking the Rstudio environment to see how much memory it uses
  threads = 1, # can usually keep at 1 unless the code itself is doing something complicated
  time = "9:00:00", # determine based off running a few regression / runner scripts and timing the entire script to get an estimate, then add +15 min
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F # 
)

# Print job summary
# cat(sprintf("Launcher submitted %d tasks with job ID: %s\n", nrow(df_list_input_data_subset), jid))
