##---------------------------------------------------------------
## A0_profile_inputs.R
## Purpose:
##   For each input dataset (dir), read WITH ONLY the minimal filters:
##     ENHANCED_FIVE_PERCENT_FLAG == "Y", pri_payer == "1", mc_ind == 0L
##   Then:
##     1) Write a raw snapshot parquet (filtered, pre-processing)
##     2) Write per-input profiles: main summary, missingness, schema, small freqs
##   After looping all inputs:
##     3) Write year-level summaries across all inputs combined:
##          - ICD overlap by bene per year
##          - Bene activity (encounters per bene)
##          - Extreme-value counts (based on tot_pay_amt)
##          - Counts by TOC
##          - sub_dataset variation per bene across all years (and per year)
## Notes:
##   - Keeps memory reasonable by selecting only needed columns.
##   - If an input is massive, consider sampling the raw snapshot.
##---------------------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, readr, purrr, arrow)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if ("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))

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
  fp_parameters <- paste0(l, "/LU_CMS/DEX/hivsud/aim1/resources_aim1/A1_f2t_parameters_aims1.csv")
  df_params <- read.csv(fp_parameters)
  
  # Get unique years to read in
  unique_years <- unique(df_params$year_id)
  
  # select year to read in data from
  data_year <- unique_years[3] # can set this to 2010, 2011, etc. for any particular year of interest
  
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


# --- Initialize output list ---
df_list <- list()

# --- List of directories to all data from --- #
data_dirs <- df_params %>% filter(year_id == data_year)

# --- Loop over each folder ---
for (i in 1:nrow(data_dirs)) {
  
  # get directory row that contains our data
  #dir <- "/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_77/F2T//data/toc=IP/year_id=2016/code_system=icd10/age_group_years_start=80"
  row <- data_dirs[i, ]
  dir <- row$directory
  
  # message to show what we're reading in
  print(paste0("Data: ", dir))
  start <- Sys.time()
  
  # read in dataset
  # dt <- open_dataset(dir) %>%
  #   filter(ENHANCED_FIVE_PERCENT_FLAG == "Y") %>% # Filters on 5% random sample column
  #   filter(pri_payer == "1") %>%
  #   filter(mc_ind == "0L") %>%
  #   select(bene_id, encounter_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt, st_resi) %>%
  #   collect() %>%
  #   as.data.frame()
  
  # read in dataset
  
  cols <- c(
    "bene_id","encounter_id","acause","primary_cause","race_cd","sex_id",
    "tot_pay_amt","st_resi","ENHANCED_FIVE_PERCENT_FLAG","pri_payer","mc_ind"
  )
  
  dt <- open_dataset(dir) %>%
    select(all_of(cols)) %>%
    filter(mc_ind == 0L) %>%           # numeric filter pushed down
    collect() %>%                      # pull to R
    filter(ENHANCED_FIVE_PERCENT_FLAG == "Y") %>%  # do string filter in-memory
    mutate(pri_payer = suppressWarnings(as.integer(pri_payer))) %>%  # coerce if needed
    filter(pri_payer == 1L) %>%
    select(bene_id, encounter_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt, st_resi)
  
  
  # Add metadata based on path
  dt$toc <- row$toc
  dt$year_id <- row$year_id 
  dt$age_group_years_start <- row$age_group_years_start
  dt$code_system <- row$code_system
  
  # Add dataframe to list
  df_list[[length(df_list) + 1]] <- dt
  
  # Print time
  message("Done in: ", Sys.time() - start)
}

# --- Combine all dfs from df_list into one df ---
df <- rbindlist(df_list, use.names = TRUE, fill = TRUE)

#########

colnames(dt)
unique(dt$code_system)