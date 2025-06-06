
##----------------------------------------------------------------
## A2_worker_rx_chunks_combiner.R
## Purpose: Read pre-collapsed RX chunk files and merge them into one Parquet per year
##----------------------------------------------------------------

rm(list = ls())
# Create a personal user library path

pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, readr, purrr,arrow)
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

#####
# Get parameters
# ----------------------------------------------------------------
# Get parameters: define year and rx_files list (interactive or SLURM mode)
# ----------------------------------------------------------------

if (interactive()) {
  # Manual testing (set year of interest)
  data_year <- 2010
  rx_folder <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250524/rx_chunks"
  rx_files <- list.files(rx_folder, pattern = "\\.parquet$", full.names = TRUE)
  rx_files <- rx_files[grepl(paste0("rx_", data_year), rx_files)]
  
} else {
  # From SUBMIT_ARRAY_JOB: args[1] = full CSV, args[2] = unique year CSV
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_full <- args[1]
  fp_parameters_unique <- args[2]
  
  # Get this job's row and year
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message("Running SLURM task: ", task_id)
  
  df_unique <- fread(fp_parameters_unique)
  data_year <- df_unique[task_id, year_id]
  
  # Load the full list of chunk paths
  df_all <- fread(fp_parameters_full)
  rx_files <- df_all[year_id == data_year, directory]
}

####

##----------------------------------------------------------------
## Output Directory Setup
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation")

# Date-stamped folder (e.g., A_data_preparation/20250421/)
#date_folder <- format(Sys.time(), "%Y%m%d")
date_folder <- "20250524"
output_folder <- file.path(base_output_dir, date_folder)

# Subfolder for final yearly RX datasets
compiled_dir <- file.path(output_folder, "aggregated_by_year")

# Create directories if they don't exist
dir.create(compiled_dir, recursive = TRUE, showWarnings = FALSE)

##----------------------------------------------------------------
## Combine RX Chunks by Year
##----------------------------------------------------------------

# Extract the year assigned by SLURM array task
yr <- data_year
message("Processing RX year: ", yr)

# Filter chunk files for this year using stringr
year_files <- rx_files[str_detect(rx_files, paste0("rx_", yr))]
year_list <- list()

for (i in seq_along(year_files)) {
  f <- year_files[i]
  message(sprintf("  [%d/%d] Reading: %s", i, length(year_files), basename(f)))
  start <- Sys.time()
  
  dt <- tryCatch({
    read_parquet(f) %>% as.data.table()
  }, error = function(e) {
    message("  Failed to read: ", f)
    return(NULL)
  })
  
  if (!is.null(dt)) {
    year_list[[length(year_list) + 1]] <- dt
    message("  Done in: ", round(Sys.time() - start, 2), " seconds")
  }
}

  
##----------------------------------------------------------------
## Final Write Step for Combined Yearly File
##----------------------------------------------------------------

# Combine all files for the year
rx_year_df <- rbindlist(year_list, use.names = TRUE, fill = TRUE)

# Define output path and save
out_path <- file.path(compiled_dir, sprintf("compiled_RX_data_%s.parquet", yr))
write_parquet(rx_year_df, out_path, compression = "snappy")

message("Final compiled file saved: ", out_path)

# Cleanup
rm(rx_year_df, year_list); gc()


#####
