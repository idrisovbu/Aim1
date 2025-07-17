##----------------------------------------------------------------
##' Title: A1_launcher_rx_data_compiler.R
##'
##' Purpose:
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



# Define run ID and input data path
run_id <- 77
fp_input_data <- paste0("/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_", run_id, "/CAUSEMAP/data/carrier=false/toc=RX")

list_input_data <- list.dirs(fp_input_data, recursive = TRUE)


# Extract relevant variables from folder structure

df_rx_list_input_data_subset <- data.frame(directory = list_input_data) %>%
  mutate(
    runid = run_id,
    year_id = as.numeric(str_extract(directory, "(?<=year_id=)\\d+")),
    age_group_years_start = as.numeric(str_extract(directory, "(?<=age_group_years_start=)\\d+")),
    sex_id = as.numeric(str_extract(directory, "(?<=sex_id=)\\d+"))
  ) %>%
  filter(!is.na(year_id) & !is.na(age_group_years_start) & !is.na(sex_id)) %>%
  filter(age_group_years_start >= 65 & age_group_years_start <= 85 & year_id >= 2010)  # Adj

# Save the filtered parameters to CSV for the runner script
# Define file path
fp_parameters <- paste0(l, "/LU_CMS/DEX/hivsud/aim1/resources_pharm/")

# Ensure the directory exists before writing the file
if (!dir.exists(fp_parameters)) dir.create(dirname(fp_parameters), recursive = TRUE, showWarnings = FALSE)

# Write full directory paths to CSV
fp_parameters_full_directories <- paste0(fp_parameters, "parameters_pharm.csv")
write.csv(df_rx_list_input_data_subset, file = fp_parameters_full_directories, row.names = FALSE)

# Create table with unique year + age group combinations for runner script to read in 
# all files in subdirectories for both sexes + all states 
# Create table with unique year combinations for RX runner script
df_list_input_unique_combinations <- df_rx_list_input_data_subset %>%
  select(year_id) %>%
  distinct() %>%
  arrange(year_id)


# Reset row count
rownames(df_list_input_unique_combinations) <- 1:nrow(df_list_input_unique_combinations)

# Write to CSV
fp_parameters_unique <- paste0(fp_parameters, "parameters_pharm_unique.csv")
write.csv(df_list_input_unique_combinations, file = fp_parameters_unique, row.names = FALSE)


##########################################
# Define output directories (Using existing date-stamped folder)
##########################################
user <- Sys.info()[["user"]]
# Define base output directory (shared with F2T)
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation"
date_folder <- format(Sys.time(), "%Y%m%d")  # Generates folder based on today's date
output_folder <- file.path(base_output_dir, date_folder)  # Uses the existing date folder

# Define logs directory inside the shared date-stamped output folder (specific for RX)
#log_dir <- file.path(output_folder, "dx_count_logs_rx")
# Define output and log directory paths
date_folder <- format(Sys.Date(), "%Y%m%d")
log_dir <- file.path("/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/logs", date_folder)


# Function to ensure the logs directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Create only the logs directory inside the shared output folder
ensure_dir_exists(log_dir)

# Update script path to new organized structure
script_path <- paste0(h, "/repo/dex_us_county/misc/hivsud/aim1_scripts/A_data_preparation/A2_worker/A2_worker_rx_data_compiler.R")


# Submit jobs to the cluster - Launcher Script for Aim 1 study - All Years, Ages, TOC from fp_parameter
jid <- SUBMIT_ARRAY_JOB(
  name = "aim1_rx_df_compiler",
  script = script_path,
  args = c(fp_parameters_full_directories, fp_parameters_unique), #I'm thinking this is the file path to our arguments (fp_arguments) .csv file we created with the parameter permutations, parameter_pharm.csv and parameter_pharm_unique.csv
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "all.q",
  n_jobs = nrow(df_list_input_unique_combinations),
  #n_jobs = 5, #just for testing purposes
  memory = "350G", #determined by running on May 18th
  threads = 1, # can usually keep at 1 unless the code itself is doing something complicated
  time = "24:00:00", # determine based off running a runner scripts and timing the entire script to get an estimate, then add +15 min
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F # In R, T = TRUE
)
