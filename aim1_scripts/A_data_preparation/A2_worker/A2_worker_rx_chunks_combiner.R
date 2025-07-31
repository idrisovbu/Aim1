
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
  base_output_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation")
  date_folder <- "20250627"
  # Manual testing (set year of interest)
  data_year <- 2010
  rx_folder <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250627/rx_chunks"
  #rx_files <- list.files(rx_folder, pattern = "\\.parquet$", full.names = TRUE)
  rx_files <- list.files(rx_folder, pattern = paste0("rx_", data_year, "_age.*\\.parquet$"), recursive = TRUE, full.names = TRUE)
  #rx_files <- rx_files[grepl(paste0("rx_", data_year), rx_files)]
  data_age  <- 65
  
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
  data_age  <- df_unique[task_id, age_group_years_start]
  
  
  # Load the full list of chunk paths
  df_all <- fread(fp_parameters_full)

  # select only those files matching year and age
  rx_files <- df_all[year_id == data_year & age_group_years_start == data_age, directory]
}

####

##----------------------------------------------------------------
## Output Directory Setup
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation")

# Date-stamped folder (e.g., A_data_preparation/20250421/)
date_folder <- format(Sys.time(), "%Y%m%d")
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
# Combine all files for the year
rx_year_df <- rbindlist(year_list, use.names = TRUE, fill = TRUE)

# Collapse to unique beneficiary × disease × other dimensions
collapsed <- rx_year_df[, .(
  has_hiv            = max(has_hiv, na.rm=TRUE),
  has_sud            = max(has_sud, na.rm=TRUE),
  has_hepc           = max(has_hepc, na.rm=TRUE),
  unique_encounters  = sum(unique_encounters, na.rm=TRUE), # sum across chunks
  tot_pay_amt        = sum(tot_pay_amt, na.rm=TRUE),
  has_cost           = as.integer(sum(tot_pay_amt, na.rm=TRUE) > 0)
), by = .(bene_id, acause_lvl1, acause_lvl2, cause_name_lvl1, cause_name_lvl2,
          year_id, age_group_years_start, toc, race_cd, sex_id)]

# Consistent column order
desired_order <- c("bene_id", "acause_lvl2", "acause_lvl1", "cause_name_lvl1", "cause_name_lvl2",
                   "year_id", "age_group_years_start", "race_cd", "sex_id", "toc",
                   "has_hiv", "has_sud", "has_hepc", "has_cost", "unique_encounters", "tot_pay_amt")

setcolorder(collapsed, intersect(desired_order, names(collapsed)))

# Define output path and save
# out_path <- file.path(compiled_dir, sprintf("compiled_RX_data_%s.parquet", yr))
# write_parquet(collapsed, out_path, compression = "snappy")
# message("Final compiled file saved: ", out_path)

# Save one file per age group
# unique_ages <- sort(unique(collapsed$age_group_years_start))
# for (ag in unique_ages) {
#   df_age <- collapsed[age_group_years_start == ag]
#   out_path <- file.path(compiled_dir, sprintf("compiled_RX_data_%s_age%s.parquet", yr, ag))
#   write_parquet(df_age, out_path, compression = "snappy")
#   message("Saved: ", out_path)
# }

out_path <- file.path(compiled_dir, sprintf("compiled_RX_data_%s_age%s.parquet", data_year, data_age))
write_parquet(collapsed, out_path, compression = "snappy")
message("Saved: ", out_path)


# Cleanup
rm(rx_year_df, year_list); gc()


#####
