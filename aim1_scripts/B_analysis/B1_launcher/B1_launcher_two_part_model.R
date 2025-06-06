##----------------------------------------------------------------
##' Title: 01_launcher_two_part_model.R
##' Author: Bulat Idrisov
##' Date: 2025-05-04
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


# Input location (adjust date as needed based on the run date)
run_date <- "20250524"
fp_input_data <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation", run_date, "aggregated_by_year")

# List all .parquet files
input_files <- list.files(fp_input_data, pattern = "\\.parquet$", full.names = TRUE)

# Build parameter table
df_params <- data.frame(directory = input_files) %>%
  mutate(
    file_type = ifelse(grepl("F2T", basename(directory)), "F2T", "RX"),
    year_id = as.numeric(sub(".*_(\\d{4})\\.parquet$", "\\1", basename(directory)))
  ) %>%
  filter(!is.na(year_id))%>%
  #filter(year_id == 2010)
  filter(!(year_id %in% c(2002, 2004, 2006))) %>%
  filter(file_type == "RX")

# Write CSV for full job param list
param_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/resources_aim1/")
dir.create(param_dir, recursive = TRUE, showWarnings = FALSE)
fp_parameters <- file.path(param_dir, "by_year_parameters_aim1.csv")
fwrite(df_params, fp_parameters)

# Define script and output paths
user <- Sys.info()[["user"]]
script_path <- file.path(h, "repo/dex_us_county/misc/hivsud/aim1_scripts/B_analysis/B2_worker/B2_worker_two_part_model.R")
log_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis/logs", run_date)
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

# Submit jobs
jid <- SUBMIT_ARRAY_JOB(
  name = "aim1_two_part",
  script = script_path,
  args = c(fp_parameters),
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "long.q", # 256 is longer wait time due to lower nodes but max can be 1024G; max on long is 2048.
  n_jobs = nrow(df_params),
  memory = "2000G", # 750as of May 26th run 750G on allq was ok, but needed more than 16hours time, need to put on long with 3 days at least
  threads = 1,
  time = "120:00:00", #120:00:00 72 hours max with this all.q; "24:00:00""
  ## long.q is 384H (2 weeks) if its under 72 then do all.q #https://docs.cluster.ihme.washington.edu/#hpc-execution-host-hardware-specifications
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F  # 
)