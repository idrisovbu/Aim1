##----------------------------------------------------------------
##' Title: B3_aggregation_summary.R
##'
##' Purpose: Aggregates the outputs from the B2 scripts (output folders 01, 02, 03, 04)
##' 
##' Outputs: 
##
##----------------------------------------------------------------

##----------------------------------------------------------------
## 0. Clear environment and set library paths
##----------------------------------------------------------------
rm(list = ls())
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
## 1. Create directory folders 
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

# Define output directory
date_of_output <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output)

# Ensure directory exists
ensure_dir_exists(output_folder)

##----------------------------------------------------------------
## 2. Aggregate & Summarize - 01.Summary_Statistics
##----------------------------------------------------------------

##----------------------------------------------------------------
## 2.1 Read in Data
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list_ss <- list.files(input_summary_stats, pattern = "\\.csv$", full.names = TRUE) 

# Read files
df_input_ss <- map_dfr(files_list_ss, ~read_csv(.x, show_col_types = FALSE))

##----------------------------------------------------------------
## 2.2 Inflation adjustment
##----------------------------------------------------------------

# Inflation adjustment setup
inflation_raw <- data.frame(
  year_id = c(2000, 2008:2024),
  inflation_rate = c(
    0.0334,   # 2000: approximated 3.34%
    0.001,    # 2008
    -0.004,   # 2009
    0.016,    # 2010
    0.017,    # 2011
    0.015,    # 2012
    0.008,    # 2013
    0.007,    # 2014
    0.021,    # 2015
    0.021,    # 2016
    0.019,    # 2017
    0.023,    # 2018
    0.014,    # 2019
    0.070,    # 2020 (if you originally had 7%)
    0.065,    # 2021
    0.034,    # 2022 (or your previous)
    0.041,    # 2023
    0.029     # 2024
  )
) # Source https://www.bls.gov/cpi/tables/supplemental-files/historical-cpi-u-202404.pdf

# Create deflators based on years in data
years_present <- sort(unique(df_input_ss$year_id))
inflation_data <- inflation_raw %>%
  filter(year_id %in% years_present) %>%
  arrange(year_id) %>%
  mutate(
    inflation_factor = cumprod(1 + inflation_rate),
    deflator = inflation_factor / inflation_factor[year_id == 2019]
  ) %>%
  select(year_id, deflator)

# Adjust cost variables
cost_columns <- c("avg_cost_per_bene", "quantile_99_cost_per_bene", "max_cost_per_bene", "sum_cost_per_group")

df_adj_ss <- df_input_ss %>%
  left_join(inflation_data, by = "year_id") %>%
  mutate(across(all_of(cost_columns), ~ .x / deflator))

# Create weighted summary table
summary_table <- df_adj_ss %>%
  group_by(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start, year_id, file_type) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, n_benes_per_group, na.rm = TRUE),
    max_cost_per_bene = max(max_cost_per_bene, na.rm = TRUE),
    quantile_99_cost_per_bene = weighted.mean(quantile_99_cost_per_bene, n_benes_per_group, na.rm = TRUE),
    sum_cost_per_group = sum(sum_cost_per_group, na.rm = TRUE),
    avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, n_benes_per_group, na.rm = TRUE),
    sum_encounters_per_group = sum(sum_encounters_per_group, na.rm = TRUE),
    total_unique_bene = sum(n_benes_per_group, na.rm = TRUE),
    .groups = "drop"
  )

# Save
write_csv(summary_table, file.path(output_folder, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"))
cat("✅ Inflation-adjusted summary table saved to:", file.path(output_folder, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"), "\n")


##----------------------------------------------------------------
## 3. Aggregate & Summarize - 03.Meta_Statistics
##----------------------------------------------------------------

##----------------------------------------------------------------
## 3.1 Read in data
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list_ms <- list.files(input_meta_stats, pattern = "\\.csv$", full.names = TRUE) 

# Read files
df_input_ms <- map_dfr(files_list_ms, ~read_csv(.x, show_col_types = FALSE))

##----------------------------------------------------------------
## 3.2 Save aggregated Metastats inputs to CSV
##----------------------------------------------------------------
output_file_ms <- file.path(output_folder, "03.Meta_Statistics_aggregated.csv")
write_csv(df_input_ms, output_file_ms)
cat("table saved", output_file_ms, "\n")

##----------------------------------------------------------------
## 3.3 Create year * total unique bene & year * toc w/ total unique bene count tables
##----------------------------------------------------------------

# The code below summarizes the analytic sample by year and type of care.
# It lists the unique types of care available each year, the number of unique beneficiaries per year,
# and produces a table showing beneficiary counts for each type of care and year.
#such as this table https://docs.google.com/document/d/1o6e8yvv1kW4mf7Be5F667b9mMOmA-O4G9TF5XKGFH_k/edit?usp=sharing

# Summarize total unique beneficiaries per year
total_bene_by_year <- df_input_ms %>%
  group_by(year_id) %>%
  summarise(
    total_unique_bene = sum(total_unique_bene, na.rm = TRUE),
    toc_list = paste(sort(unique(toc)), collapse = ", "),
    toc_unique_count = n_distinct(toc)
  ) %>%
  arrange(year_id)

# Summarize total unique beneficiaries per year and toc
total_bene_by_year_toc <- df_input_ms %>%
  group_by(year_id, toc) %>%
  summarise(total_unique_bene = sum(total_unique_bene, na.rm = TRUE)) %>%
  arrange(year_id, toc)

##----------------------------------------------------------------
## 3.4 Output beneficiary stats tables to CSV
##----------------------------------------------------------------

# Save total unique beneficiaries by year
output_file_by_year <- file.path(output_folder, "03.Meta_Statistics_total_bene_by_year.csv")
write_csv(total_bene_by_year, output_file_by_year)

# Save total unique beneficiaries by year and toc
output_file_by_year_toc <- file.path(output_folder, "03.Meta_Statistics_total_bene_by_year_by_toc.csv")
write_csv(total_bene_by_year_toc, output_file_by_year_toc)



