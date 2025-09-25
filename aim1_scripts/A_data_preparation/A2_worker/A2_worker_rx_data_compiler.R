##----------------------------------------------------------------
##' Title: A2_worker_rx_data_compiler.R
##' Purpose:
##' ----------------------------------------------------------------

##----------------------------------------------------------------
## 0. Setup Environment
##----------------------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx, readr, purrr,arrow)
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
## 1. Read in array job parameters
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

##----------------------------------------------------------------
## 2. Define directories
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Create the folders if they don't exist
dir.create(output_folder, showWarnings = FALSE, recursive = TRUE)

##----------------------------------------------------------------
## 3. Read data
##----------------------------------------------------------------

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
  df <- open_dataset(dir) %>%
    filter(ENHANCED_FIVE_PERCENT_FLAG == "Y") %>% # Filters on 5% random sample column
    filter(mc_ind == 0L) %>%
    filter(pri_payer == 1) %>%
    select(bene_id, claim_id, acause, primary_cause, race_cd, tot_chg_amt) %>%
    collect() %>%
    as.data.table()
  
  # rename once (no duplicates)
  setnames(df, old = c("claim_id", "tot_chg_amt"),
           new = c("encounter_id", "tot_pay_amt"))
  
  # Add metadata from folder structure
  df[, `:=`(
    sex_id = row$sex_id,
    year_id = row$year_id,
    age_group_years_start = row$age_group_years_start,
    toc = "RX",
    code_system = "RX"
  )]
  
  ##----------------------------------------------------------------
  ## 1. Assign condition flags *before* filtering
  ##----------------------------------------------------------------
  # Save beneficiary-level flags
  hiv_benes <- unique(df[acause == "hiv", .(bene_id)][, has_hiv := 1L])
  sud_benes <- unique(
    df[acause %in% c("mental_alcohol", "mental_drug_agg", "mental_drug_opioids"), .(bene_id)][, has_sud := 1L])
  hepc_benes <- unique(df[acause == "hepatitis_c", .(bene_id)][, has_hepc := 1L])
  
  # Join flags
  df <- merge(df, hiv_benes, by = "bene_id", all.x = TRUE)
  df <- merge(df, sud_benes, by = "bene_id", all.x = TRUE)
  df <- merge(df, hepc_benes, by = "bene_id", all.x = TRUE)
  
  # Replace NAs with 0
  df[is.na(has_hiv), has_hiv := 0L]
  df[is.na(has_sud), has_sud := 0L]
  df[is.na(has_hepc), has_hepc := 0L]
  
  # Keep only Black, White, and Hispanic
  df <- df[race_cd %in% c("BLCK", "WHT", "HISP")]
  
  ##----------------------------------------------------------------
  ## 2. Acause maping per DEX categories
  ##----------------------------------------------------------------
  
  # Define mapping columns (so we can drop them safely before join)
  mapping_cols <- c("acause_lvl2", "cause_name_lvl2", "acause_lvl1", "cause_name_lvl1")
  
  # Convert your df to tibble and clean 'acause'
  df <- as_tibble(df) %>%
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
  df <- df %>%
    left_join(df_map, by = "acause")
  
  # Defensive check for accidental duplicate columns (should never hit)
  if(any(duplicated(names(df)))) {
    dupes <- names(df)[duplicated(names(df))]
    stop(paste("ERROR: Duplicated columns after join:", paste(dupes, collapse = ", ")))
  }
  
  # Quick check for mapping success
  message("Mapping completed. Number of records: ", nrow(df))
  print(table(is.na(df$acause_lvl2)))
  
  ##----------------------------------------------------------------
  ## 3. Add binary disease flag columns and unique disease count column per grouped bene
  ##----------------------------------------------------------------
  cause_list <- c("_enteric_all","_infect","_intent","_mental","_neo","_neuro",
                  "_ntd","_otherncd","_rf","_ri","_sense","_subs","_unintent","_well","cvd","diab_ckd",
                  "digest","hiv","inj_trans","mater_neonat","msk","nutrition","resp", "skin", "std")
  
  # Create matrix of binary values for unique bene x has_<cause>
  df_cause_flags <- df %>%
    mutate(cause = acause_lvl2) %>%
    distinct(bene_id, cause) %>%              # one cause per patient
    mutate(flag = 1L, cause = paste0("has_", cause)) %>%
    pivot_wider(names_from = cause, values_from = flag, values_fill = 0)
  
  # Force missing columns to exist if they don't
  expected <- paste0("has_", cause_list)
  missing  <- setdiff(expected, names(df_cause_flags))
  df_cause_flags[missing] <- 0L
  
  # Remove "has_hiv" & "has_sud"
  df_cause_flags <- df_cause_flags %>%
    select(-c("has_hiv", "has__subs"))
  
  # Create row count (disease count #)
  df_cause_flags <- df_cause_flags %>%
    mutate(cause_count = rowSums(across(colnames(df_cause_flags)[-1])))
  
  ##----------------------------------------------------------------
  ## 4. Filter to primary cause only
  ##----------------------------------------------------------------
  df <- df %>%
    filter(primary_cause == 1)
  
  ##----------------------------------------------------------------
  ## 5. Collapse to bene × disease category and attach metadata
  ##----------------------------------------------------------------
  # Convert back to data.table if needed
  setDT(df)
  
  df <- df[, .(
    has_hiv           = max(has_hiv, na.rm=TRUE),
    has_sud           = max(has_sud, na.rm=TRUE),
    has_hepc          = max(has_hepc, na.rm=TRUE),
    unique_encounters = uniqueN(encounter_id),
    tot_pay_amt       = sum(tot_pay_amt, na.rm=TRUE),
    has_cost          = as.integer(sum(tot_pay_amt, na.rm=TRUE) > 0)
  ), by = .(bene_id, acause_lvl1, acause_lvl2, cause_name_lvl1, cause_name_lvl2,
            year_id, age_group_years_start, toc, race_cd, sex_id)]
  
  ##----------------------------------------------------------------
  ## 6. Join with binary disease flag df for "has_" column information for each bene
  ##----------------------------------------------------------------
  df <- df %>% left_join(df_cause_flags, by = "bene_id")
  
  # Set column order - doesn't have "st_resi" for RX data
  desired_order <- c("bene_id", "acause_lvl2", "acause_lvl1", "cause_name_lvl1", "cause_name_lvl2",
                     "year_id", "age_group_years_start", "race_cd", "sex_id", "toc",
                     "has_hiv", "has_sud", "has_hepc", "has_cost", "unique_encounters", "tot_pay_amt", "has_cvd", "has__otherncd", "has_digest", 
                     "has__mental", "has_msk", "has_skin", "has__sense", "has__well", 
                     "has_nutrition", "has_diab_ckd", "has__neuro", "has__rf", "has_resp", 
                     "has__ri", "has__neo", "has__infect", "has_inj_trans", "has__unintent", 
                     "has__intent", "has__enteric_all", "has_std", "has__ntd", "has_mater_neonat", 
                     "cause_count")
  
  setcolorder(df, intersect(desired_order, names(df)))

  ##----------------------------------------------------------------
  ## 7. Save as chunk
  ##----------------------------------------------------------------
  
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
  write_parquet(df, chunk_path, compression = "snappy", use_dictionary = TRUE)
  
  message(sprintf("Completed %d of %d folders", i, nrow(data_dirs)))
  message("Done in: ", Sys.time() - start)
  rm(df); gc()
}

  
