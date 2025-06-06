##----------------------------------------------------------------
##' Title: A1_launcher_rx_chunk_combiner.R
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


# Dynamically read in directories where data is stored to see what data they have in the first place
# Create a table based off their directory hierarchy (use code to scan folder directories to determine available data)

date_of_rx_chunk_input <- "20250524"

rx_input_data <- paste0("/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/", date_of_rx_chunk_input, "/rx_chunks")

rx_input_data <- list.files(rx_input_data, pattern = "\\.parquet$", full.names = TRUE)


df_rx_list_input_data_subset <- data.frame(directory = rx_input_data) %>%
  mutate(
    year_id = str_extract(directory, "(?<=rx_)\\d{4}") %>% as.numeric(),
    age_group_years_start = str_extract(directory, "(?<=_age)\\d{2}") %>% as.numeric(),
    sex_id = str_extract(directory, "(?<=_sex)\\d") %>% as.numeric(),
    state = str_extract(directory, "(?<=_state)[A-Z]{2}")
  )


param_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/resources_pharm/"


# Ensure the directory exists
dir.create(param_output_dir, recursive = TRUE, showWarnings = FALSE)

# 1. Save full list of RX chunk paths with metadata
fp_parameters_full <- file.path(param_output_dir, "parameters_rx_chunks.csv")
write.csv(df_rx_list_input_data_subset, file = fp_parameters_full, row.names = FALSE)


# 2. Create and save unique year-based job list for launcher
df_rx_unique_years <- df_rx_list_input_data_subset %>%
  select(year_id) %>%
  distinct() %>%
  arrange(year_id)


fp_parameters_unique <- file.path(param_output_dir, "chunk_parameters_rx_unique.csv")
write.csv(df_rx_unique_years, file = fp_parameters_unique, row.names = FALSE)





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
script_path <- paste0(h, "/repo/dex_us_county/misc/hivsud/aim1_scripts/A_data_preparation/A2_worker/A2_worker_rx_chunks_combiner.R")


# Submit jobs to the cluster - Launcher Script for Aim 1 study - All Years, Ages, TOC from fp_parameter
jid <- SUBMIT_ARRAY_JOB(
  name = "chunkRX",
  script = script_path,
  args = c(fp_parameters_full, fp_parameters_unique), #I'm thinking this is the file path to our arguments (fp_arguments) .csv file we created with the parameter permutations, parameter_pharm.csv and parameter_pharm_unique.csv
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "long.q",
  n_jobs = nrow(df_rx_unique_years),
  #n_jobs = 5, #just for testing purposes
  memory = "450G", #determine based off running a few individual runner scripts and checking the Rstudio environment to see how much memory it uses
  threads = 1, # can usually keep at 1 unless the code itself is doing something complicated
  time = "17:00:00", # determine based off running a runner scripts and timing the entire script to get an estimate, then add +15 min
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F # In R, T = TRUE
)
