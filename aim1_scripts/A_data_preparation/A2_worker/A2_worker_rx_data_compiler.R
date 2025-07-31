##----------------------------------------------------------------
##' Title: A2_worker_rx_data_compiler.R
##' Purpose:
##'
##' ----------------------------------------------------------------
rm(list = ls())
# Create a personal user library path
user_lib <- file.path(Sys.getenv("HOME"), "R", "library", paste0(R.version$major, ".", R.version$minor))
dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
# 
# # Prioritize personal library in libPaths
.libPaths(c(user_lib, .libPaths()))

pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx, readr, purrr,arrow)
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

##----------------------------------------------------------------
## 0. Read in data
##----------------------------------------------------------------

if (interactive()) {
  # Set filepath for parameters_aims1.csv, this has all filepaths for parquet files
  fp_parameters <- paste0(l, "LU_CMS/DEX/hivsud/aim1/resources_pharm/A1_rx_parameters_pharm.csv")
  df_params <- read.csv(fp_parameters)
  
  # Get unique years to read in
  unique_years <- unique(df_params$year_id)
  
  # select year to read in data from
  data_year <- unique_years[1] # can set this to 2010, 2011, etc. for any particular year of interest

} else { 
  # Read in args from SUBMIT_ARRAY_JOBS(), read in .csv containing permutations, subset to rows based on year
  args <- commandArgs(trailingOnly = TRUE)
  fp_parameters_input <- args[1] # fp_parameters_input <- fp_parameters
  
  # use Task id to determine the data_year, used for reading in all data for that particular year
  array_job_number <- Sys.getenv("SLURM_ARRAY_TASK_ID") 
  print(paste0("job number:", array_job_number))
  df_params <- as.data.table(fread(fp_parameters_input))
  
  # get unique years to read in 
  unique_years <- unique(df_params$year_id)
  
  # select year to read in data from
  data_year <- unique_years[as.numeric(array_job_number)] 
}



# ##----------------------------------------------------------------
# ##  #define dirs
# ##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Define subdirectory for RX intermediate files
#rx_chunk_folder <- file.path(output_folder, "rx_chunks")

# Create the folders if they don't exist
dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)
#dir.create(rx_chunk_folder, showWarnings = FALSE, recursive = TRUE)


# -- List of directories based on filtered year -- #
data_dirs <- df_params %>% filter(year_id == data_year)

# --- Loop over each folder ---
for (i in 1:nrow(data_dirs)) {
  
  row <- data_dirs[i, ]
  dir <- row$directory
  rx_chunk_folder <- file.path(output_folder, "rx_chunks",
                               paste0("year", row$year_id),
                               paste0("age", row$age_group_years_start))
  dir.create(rx_chunk_folder, showWarnings = FALSE, recursive = TRUE)
  
  message("Reading: ", dir)
  start <- Sys.time()
  
  # Read in data
  dt <- open_dataset(dir) %>%
    select(claim_id, acause, primary_cause, race_cd, tot_chg_amt) %>%
    collect() %>%
    as.data.table()
  
  # Rename columns
  setnames(dt, "claim_id", "bene_id")
  setnames(dt, "tot_chg_amt", "tot_pay_amt")
  
  # Add metadata from folder structure
  dt[, `:=`(
    sex_id = row$sex_id,
    year_id = row$year_id,
    age_group_years_start = row$age_group_years_start,
    toc = "RX",
    code_system = "RX"
  )]
  
  # Step 1: Save flags BEFORE filtering
  hiv_benes <- unique(dt[acause == "hiv", .(bene_id)][, has_hiv := 1L])
  sud_benes <- unique(dt[grepl("^mental_", acause), .(bene_id)][, has_sud := 1L])
  hepc_benes <- unique(dt[acause == "hepatitis_c", .(bene_id)][, has_hepc := 1L])
  
  # Step 2: Filter to primary cause only
  dt <- dt[primary_cause == 1]
  
  # Step 3: Reattach flags
  dt <- merge(dt, hiv_benes, by = "bene_id", all.x = TRUE)
  dt <- merge(dt, sud_benes, by = "bene_id", all.x = TRUE)
  dt <- merge(dt, hepc_benes, by = "bene_id", all.x = TRUE)
  
  dt[is.na(has_hiv), has_hiv := 0L]
  dt[is.na(has_sud), has_sud := 0L]
  dt[is.na(has_hepc), has_hepc := 0L]
  
  # Step 4: Restrict to BLCK, WHT, HISP
  dt <- dt[race_cd %in% c("BLCK", "WHT", "HISP")]
  
  # Define mapping columns (so we can drop them safely before join)
  mapping_cols <- c("acause_lvl2", "cause_name_lvl2", "acause_lvl1", "cause_name_lvl1")
  
  # Convert your dt to tibble and clean 'acause'
  dt <- as_tibble(dt) %>%
    mutate(acause = trimws(tolower(acause))) %>%
    select(-any_of(mapping_cols))  # Drop mapping cols if already present
  
  # Load and clean the mapping table
  df_map <- read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
    select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
    mutate(
      acause = trimws(tolower(acause)),
      acause_lvl2      = if_else(acause == "hiv", "hiv", acause_lvl2),
      cause_name_lvl2  = if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
      acause_lvl2      = if_else(acause == "std", "std", acause_lvl2),
      cause_name_lvl2  = if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
    )
  
  # Join (mapping cols can only exist once now)
  dt <- dt %>%
    left_join(df_map, by = "acause")
  
  # Defensive check for accidental duplicate columns (should never hit)
  if(any(duplicated(names(dt)))) {
    dupes <- names(dt)[duplicated(names(dt))]
    stop(paste("ERROR: Duplicated columns after join:", paste(dupes, collapse = ", ")))
  }
  
  # Quick check for mapping success
  message("Mapping completed. Number of records: ", nrow(dt))
  print(table(is.na(dt$acause_lvl2)))
  
  # Optional: preview a sample
  #print(head(dt, 5))
  
  # Convert back to data.table
  setDT(dt)
  
  collapsed <- dt[, .(
    has_hiv            = max(has_hiv, na.rm=TRUE),
    has_sud            = max(has_sud, na.rm=TRUE),
    has_hepc           = max(has_hepc, na.rm=TRUE),
    unique_encounters  = .N,
    tot_pay_amt        = sum(tot_pay_amt, na.rm=TRUE),
    has_cost           = as.integer(sum(tot_pay_amt, na.rm=TRUE) > 0)
  ), by = .(bene_id, acause_lvl1, acause_lvl2, cause_name_lvl1, cause_name_lvl2,
            year_id, age_group_years_start, toc, race_cd, sex_id)]
  
  
  desired_order <- c("bene_id", "acause_lvl2", "acause_lvl1", "cause_name_lvl1", "cause_name_lvl2",
                     "year_id", "age_group_years_start", "race_cd", "sex_id", "toc",
                     "has_hiv", "has_sud", "has_hepc", "has_cost", "unique_encounters", "tot_pay_amt")
  setcolorder(collapsed, intersect(desired_order, names(collapsed)))

  
  #####
  
  # Extract state abbreviation from directory path
  state <- stringr::str_extract(dir, "(?<=st_resi=)[A-Z]{2}")
  
  # Create informative file name
  chunk_filename <- sprintf("rx_%s_age%s_sex%s_state%s.parquet",
                            row$year_id,
                            row$age_group_years_start,
                            row$sex_id,
                            state)
  
  # Full output path
  chunk_path <- file.path(rx_chunk_folder, chunk_filename)
  
  # Write output
  write_parquet(collapsed, chunk_path, compression = "snappy", use_dictionary = TRUE)
  
  message(sprintf("Completed %d of %d folders", i, nrow(data_dirs)))
  message("Done in: ", Sys.time() - start)
  rm(dt, collapsed); gc()
}
  
  #######
