# Exploration

##----------------------------------------------------------------
## 0. Clear environment and set library paths
##----------------------------------------------------------------
# rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,readr,purrr)
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
## 0. Create directory folders 
##----------------------------------------------------------------
# Ensure the output directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Define input directory 
date_of_input <- "bested" # bested from 20250731
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"
input_summary_stats <- file.path(base_dir, "01.Summary_Statistics", date_of_input)
input_regression_estimates <- file.path(base_dir, "02.Regression_Estimates", date_of_input)
input_meta_stats <- file.path(base_dir, "03.Meta_Statistics", date_of_input) 
input_two_part <- file.path(base_dir, "04.Two_Part_Estimates", date_of_input, "bootstrap_results")

# Define output directory
date_of_output <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output)

# Ensure directory exists
ensure_dir_exists(output_folder)


##----------------------------------------------------------------
## 1. Read in data
##----------------------------------------------------------------
input_two_part_bested <- file.path(base_dir, "04.Two_Part_Estimates", "bested", "bootstrap_results")
input_two_part_old <- file.path(base_dir, "04.Two_Part_Estimates", "20250624", "bootstrap_results")

df_new <- fread(input = file.path(input_two_part_bested, "bootstrap_marginal_results_F2T_year2010_age85.csv"))

df_old <- fread(input = file.path(input_two_part_old, "bootstrap_marginal_results_F2T_year2010_age85.csv"))

# WTF is going on -- df is striaght from the A output section

# _infect, BLCK, 85, IP, cost_neither (abt 30K), cost_hiv_only (64 dollars)
colnames(df)
df_filter <- df %>%
  filter(acause_lvl2 == "_infect") %>%
  filter(race_cd == "BLCK") %>%
  filter(toc == "IP") %>%
  filter(has_hiv == 1)


# let's filter on boot_combined, are all bootstrap results wonky
boot_combined_filter <- boot_combined %>%
  filter(acause_lvl2 == "_infect") %>%
  filter(race_cd == "BLCK") %>%
  filter(toc_fact == "IP") 



##check RX data on DEX team 

fp <- "/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_77/CAUSEMAP/data/carrier=false/toc=RX/year_id=2019/st_resi=AK/age_group_years_start=70/sex_id=1/rx_part0.parquet"

# Read the parquet file into a data frame
df <- read_parquet(fp_chunk)

df <- open_dataset(fp) %>% head(100000)

df_summary <- df %>%
  group_by(bene_id) %>%
  summarise(n = n_distinct(acause), .groups = "drop") %>%
  collect()


fp_chunk <-"/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/20250730/rx_chunks/year2019/age75/rx_2019_age75_sex1_stateCT.parquet"


# Read the parquet file into a data frame
df <- read_parquet(fp_chunk)

df <- open_dataset(fp) %>% head(100000)

df_summary <- df %>%
  group_by(bene_id) %>%
  summarise(n = n_distinct(acause_lvl2), .groups = "drop") %>%
  collect()


