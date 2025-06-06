##----------------------------------------------------------------
##' Title: A2_worker_f2t_data_complier.R
##'
##' Purpose:
##'
##' ----------------------------------------------------------------
rm(list = ls())

# Create a personal user library path
# user_lib <- file.path(Sys.getenv("HOME"), "R", "library", paste0(R.version$major, ".", R.version$minor))
# dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
# 
# # Prioritize personal library in libPaths
# .libPaths(c(user_lib, .libPaths()))
# 
# # Now load/install packages
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, readr, purrr, arrow)

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
  fp_parameters <- paste0(l, "/LU_CMS/DEX/hivsud/aim1/resources_aim1/parameters_aims1.csv")
  df_params <- read.csv(fp_parameters)
  
  # Get unique years to read in
  unique_years <- unique(df_params$year_id)
  
  # select year to read in data from
  data_year <- unique_years[4] # can set this to 2010, 2011, etc. for any particular year of interest

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
  row <- data_dirs[i, ]
  dir <- row$directory
  
  # message to show what we're reading in
  print(paste0("Data: ", dir))
  start <- Sys.time()
  
  # read in dataset
  dt <- open_dataset(dir) %>%
    select(bene_id, encounter_id, acause, primary_cause, race_cd, sex_id, tot_pay_amt) %>%
    collect() %>%
    as.data.frame()
    
  # Add metadata based on path
  dt$toc <- row$toc
  dt$year_id <- row$year_id #can be removed becasue we dont have 
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
## 1. Assign condition flags *before* filtering, then reattach later
##----------------------------------------------------------------

# Save beneficiary-level flags
hiv_benes <- unique(df[acause == "hiv", .(bene_id)][, has_hiv := 1L])
sud_benes <- unique(df[grepl("^mental_", acause), .(bene_id)][, has_sud := 1L])
hepc_benes <- unique(df[acause == "hepatitis_c", .(bene_id)][, has_hepc := 1L])

##----------------------------------------------------------------
## 2. Filter to primary cause only
##----------------------------------------------------------------
df <- df[primary_cause == 1]
##----------------------------------------------------------------
## 3a. Join flags back in
##----------------------------------------------------------------
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
## 3b. Acause maping per DEX categories
##----------------------------------------------------------------


# Convert df to tibble for dplyr-style manipulation
df <- as_tibble(df)

#  Load and clean mapping file
df_map <- read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
  distinct(acause, acause_lvl2, acause_lvl1)


# Join acause levels from mapping
df <- df %>%
  mutate(acause_lvl3 = acause) %>%  # keep original
  left_join(df_map, by = "acause") %>%
  mutate(acause = acause_lvl2)  # promote lvl2 to primary

df <- df %>%
  filter(!acause %in% c("Maternal and neonatal disorders"))


##----------------------------------------------------------------
## 4. Collapse to bene × disease category and attach metadata
##----------------------------------------------------------------
# Convert back to data.table if needed
setDT(df)

# Collapse to bene × disease (using new variable names)
df <- df[, .(
  # acause_lvl3 = first(acause_lvl3), #remove
  # acause_lvl1 = first(acause_lvl1),#remove
  race_cd = first(race_cd),
  sex_id  = first(sex_id),
  has_hiv = max(has_hiv),
  has_sud = max(has_sud),
  has_hepc = max(has_hepc),
  unique_encounters = uniqueN(encounter_id),
  tot_pay_amt = sum(tot_pay_amt, na.rm = TRUE),
  # year_id = first(year_id), # remove year_id
  # age_group_years_start = first(age_group_years_start),
  # toc = first(toc), #all
  code_system = first(code_system)
), by = .(bene_id, acause)] # group by age_group_years_start, toc
# add re-flag for hiv, sud and hep c. 

df[, has_cost := as.integer(tot_pay_amt > 0)]
setcolorder(df, c(
  "bene_id", "acause", "acause_lvl3", "acause_lvl1",
  "race_cd", "sex_id", "has_hiv", "has_sud", "has_hepc", "has_cost",
  "unique_encounters", "tot_pay_amt",
  "year_id", "age_group_years_start", "toc", "code_system"
))



# ##----------------------------------------------------------------
# ## 4. Write to parquet file
# ##----------------------------------------------------------------

# Define base output directory
base_output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation"
date_folder <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_output_dir, date_folder)

# Subfolder for final yearly f2t datasets
compiled_dir <- file.path(output_folder, "aggregated_by_year")
# Create directories if they don't exist
dir.create(compiled_dir, recursive = TRUE, showWarnings = FALSE)

# Save to Parquet file
write_parquet(df, paste0(compiled_dir, "/compiled_F2T_data_", data_year, ".parquet"))


