##----------------------------------------------------------------
##' Title: A2_worker_f2t_data_complier.R
##' Purpose:
##'
##' ----------------------------------------------------------------
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
  dt <- open_dataset(dir) %>%
    filter(ENHANCED_FIVE_PERCENT_FLAG == "Y") %>% # Filters on 5% random sample column
    filter(pri_payer == 1) %>% # These are set to strings in later pipeline runs, 1 here means "mdcr"
    filter(mc_ind == 0) %>% # mc_ind == 0 means a lot of costs have 0 of spending, driving costs down
    select(bene_id, encounter_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt, st_resi) %>%
    collect() %>%
    as.data.frame()
    
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

# Define the mapping columns to avoid duplicates after the join
mapping_cols <- c("acause_lvl2", "cause_name_lvl2", "acause_lvl1", "cause_name_lvl1")

df <- as_tibble(df)

### note for the future to redo the mapping table (remove HIV/STD group in the map so that its is clear for agreagation too)

# Load and clean the mapping file
df_map <- read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
  select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
  mutate(
    acause = trimws(tolower(acause)),
    acause_lvl2      = if_else(acause == "hiv", "hiv", acause_lvl2),
    cause_name_lvl2  = if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
    acause_lvl2      = if_else(acause == "std", "std", acause_lvl2),
    cause_name_lvl2  = if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
  )

# Clean the acause in your data and remove any pre-existing mapping columns
df <- df %>%
  mutate(acause = trimws(tolower(acause))) %>%
  select(-any_of(mapping_cols)) %>%
  left_join(df_map, by = "acause")

# Quick join check
message("Mapping completed. Number of records: ", nrow(df))
print(table(is.na(df$acause_lvl2)))   # Should be mostly FALSE (mapped), if many TRUE, join mismatch

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
), by = .(bene_id, st_resi, acause_lvl1, acause_lvl2, cause_name_lvl1, cause_name_lvl2,
          year_id, age_group_years_start, toc, race_cd, sex_id)]

##----------------------------------------------------------------
## 6. Join with binary disease flag df for "has_" column information for each bene
##----------------------------------------------------------------
df <- df %>% left_join(df_cause_flags, by = "bene_id")

# Set column order
desired_order <- c("bene_id", "st_resi", "acause_lvl2", "acause_lvl1", "cause_name_lvl1", "cause_name_lvl2",
                   "year_id", "age_group_years_start", "race_cd", "sex_id", "toc",
                   "has_hiv", "has_sud", "has_hepc", "has_cost", "unique_encounters", "tot_pay_amt", "has_cvd", "has__otherncd", "has_digest", 
                      "has__mental", "has_msk", "has_skin", "has__sense", "has__well", 
                      "has_nutrition", "has_diab_ckd", "has__neuro", "has__rf", "has_resp", 
                      "has__ri", "has__neo", "has__infect", "has_inj_trans", "has__unintent", 
                      "has__intent", "has__enteric_all", "has_std", "has__ntd", "has_mater_neonat", 
                      "cause_count")

setcolorder(df, intersect(desired_order, names(df)))

##----------------------------------------------------------------
## 7. Write to parquet file
##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Subfolder for final yearly f2t datasets
compiled_dir <- file.path(output_folder, "aggregated_by_year")

# Create directories if they don't exist
dir.create(compiled_dir, recursive = TRUE, showWarnings = FALSE)

# Get unique age groups in the data
unique_age_groups <- sort(unique(df$age_group_years_start))

# Loop over age groups and save each as a separate Parquet file
for (ag in unique_age_groups) {
  df_age <- df[age_group_years_start == ag]
  fname <- sprintf("compiled_F2T_data_%s_age%s.parquet", data_year, ag)
  fpath <- file.path(compiled_dir, fname)
  write_parquet(df_age, fpath)
  message("Saved: ", fpath)
}

