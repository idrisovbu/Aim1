##----------------------------------------------------------------
##' Title: A1_launcher_rx_chunk_combiner.R
##' 
##' Purpose: A1_launcher_rx_data_compiler.R is ran first before this script to generate year/state/age level RX data chunks, after
##'          which this script combines the chunks into the final year/age level data. 
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
## 1. Generate DF with all processed chunks
##----------------------------------------------------------------

# List out processed RX Chunks
date_of_rx_chunk_input <- "20250924" #format(Sys.time(), "%Y%m%d")

rx_input_data_dir <- file.path("/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/", date_of_rx_chunk_input, "rx_chunks")

rx_input_data <- list.files(
  path = rx_input_data_dir,
  pattern = "\\.parquet$",
  full.names = TRUE,
  recursive = TRUE
)

# Create DF listing all processed RX chunks
df_rx_list_input_data_subset <- data.frame(directory = rx_input_data) %>%
  mutate(
    year_id = str_extract(directory, "(?<=rx_)\\d{4}") %>% as.numeric(),
    age_group_years_start = str_extract(directory, "(?<=_age)\\d{2}") %>% as.numeric(),
    sex_id = str_extract(directory, "(?<=_sex)\\d") %>% as.numeric(),
    state = str_extract(directory, "(?<=_state)[A-Z]{2}")
  )

# Check against params file to ensure that all chunks have output files 
param_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/resources_pharm/"
df_A1_rx_parameters_pharm <- fread(file.path(param_output_dir, "/A1_rx_parameters_pharm.csv"))

if (nrow(df_A1_rx_parameters_pharm) == nrow(df_rx_list_input_data_subset)) {
  print("All RX chunks created by A1_launcher_rx_data_compiler.R")
} else {
  print("MISSING CHUNKS! COMPARE INPUT_DATA_SUBSET AGAINST A1_RX_PARAMETERS_PHARM.CSV")
}


##----------------------------------------------------------------
## 2. Save CSVs with list of RX chunk filepaths & unique combinations for processing
##----------------------------------------------------------------

# Ensure the directory exists
dir.create(param_output_dir, recursive = TRUE, showWarnings = FALSE)

# 1. Save full list of RX chunk paths with metadata
fp_parameters_full <- file.path(param_output_dir, "A1_rx_parameters_rx_chunks.csv")
write.csv(df_rx_list_input_data_subset, file = fp_parameters_full, row.names = FALSE)

# 2. Create and save unique year-based job list for launcher
df_rx_unique_combos <- df_rx_list_input_data_subset %>%
  select(year_id, age_group_years_start) %>%
  distinct() %>%
  arrange(year_id, age_group_years_start)

fp_parameters_unique <- file.path(param_output_dir, "A1_rx_chunk_parameters_rx_unique.csv")
write.csv(df_rx_unique_combos, file = fp_parameters_unique, row.names = FALSE)

##----------------------------------------------------------------
## 3. Define output directories (Using existing date-stamped folder)
##----------------------------------------------------------------
user <- Sys.info()[["user"]]

# Define output and log directory paths
date_folder <- format(Sys.Date(), "%Y%m%d")
log_dir <- file.path("/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/logs/")

# Function to ensure the logs directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Create only the logs directory inside the shared output folder
ensure_dir_exists(log_dir)

# Update script path to new organized structure
script_path <- paste0(h, "/repo/Aim1/aim1_scripts/A_data_preparation/A2_worker/A2_worker_rx_chunks_combiner.R")

##----------------------------------------------------------------
## 4. Submit Jobs
##----------------------------------------------------------------

# Submit jobs to the cluster - Launcher Script for Aim 1 study - All Years, Ages, TOC from fp_parameter
jid <- SUBMIT_ARRAY_JOB(
  name = "chunkRX",
  script = script_path,
  args = c(fp_parameters_full, fp_parameters_unique), #I'm thinking this is the file path to our arguments (fp_arguments) .csv file we created with the parameter permutations, parameter_pharm.csv and parameter_pharm_unique.csv
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "all.q",
  n_jobs = nrow(df_rx_unique_combos),
  memory = "20G",
  threads = 1, 
  time = "00:30:00", #takes about ~2 min
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F # F = Full Run, T = Test Run (only run the first job in a batch)
)

cat("Submitted", nrow(df_rx_unique_combos), "array tasks.\n")
