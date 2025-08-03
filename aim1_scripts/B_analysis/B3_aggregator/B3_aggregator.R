##----------------------------------------------------------------
##' Title: B3_aggregator.R
##'
##' Purpose: Aggregates the outputs from the B2 scripts (output folders 01, 02 (unused ATM), 03, 04) and creates summary files
##' 
##' Outputs: /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/01.Summary_Statistics_inflation_adjusted_aggregated.csv
##           /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/03.Meta_Statistics_aggregated.csv
##           /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/03.Meta_Statistics_total_bene_by_year.csv
##           /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/03.Meta_Statistics_total_bene_by_year_by_toc.csv
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
## 1. Aggregate & Summarize - 01.Summary_Statistics
##----------------------------------------------------------------

##----------------------------------------------------------------
## 1.1 Read in Data
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list_ss <- list.files(input_summary_stats, pattern = "\\.csv$", full.names = TRUE) 

# Read files
df_input_ss <- map_dfr(files_list_ss, ~read_csv(.x, show_col_types = FALSE))

##----------------------------------------------------------------
## 1.2 Inflation adjustment
##----------------------------------------------------------------
## note DEX helper function is described in z.utilities folder: /Aim1/aim1_scripts/Z_utilities/deflate.R
#The function converts monetary values from any year to a common reference year (e.g., 2019 USD)
##' using the Consumer Price Index (CPI-U) for inflation adjustment.
# Source of  CPI https://www.bls.gov/cpi/tables/supplemental-files/historical-cpi-u-202404.pdf

# Adjust cost variables
cost_columns <- c("avg_cost_per_bene", "quantile_99_cost_per_bene", "max_cost_per_bene", "sum_cost_per_group")

df_adj_ss <- deflate(
  data = df_input_ss,
  val_columns = cost_columns,
  old_year = "year_id",
  new_year = 2019
)


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
cat("inflation-adjusted summary table saved to:", file.path(output_folder, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"), "\n")

##----------------------------------------------------------------
## 2. Aggregate & Summarize - 02.Regression_Estimates
##----------------------------------------------------------------


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

##----------------------------------------------------------------
## 4. Aggregate & Summarize - 04.Two_Part_Estimates
##----------------------------------------------------------------

##----------------------------------------------------------------
## 4.1 Read in data
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list_tpe <- list.files(input_two_part, pattern = "\\.csv$", full.names = TRUE) 

# Read files
df_input_tpe <- map_dfr(files_list_tpe, ~read_csv(.x, show_col_types = FALSE))

##----------------------------------------------------------------
## 4.2 Inflation adjustment and mapping 
##----------------------------------------------------------------

colnames(df_input_tpe)
# cost column to adjust for inflation

cost_columns <- c(
  "mean_cost",        "lower_ci",        "upper_ci",
  "mean_cost_hiv",    "lower_ci_hiv",    "upper_ci_hiv",
  "mean_cost_sud",    "lower_ci_sud",    "upper_ci_sud",
  "mean_cost_hiv_sud","lower_ci_hiv_sud","upper_ci_hiv_sud",
  "mean_delta_hiv",   "lower_ci_delta_hiv",   "upper_ci_delta_hiv",
  "mean_delta_sud",   "lower_ci_delta_sud",   "upper_ci_delta_sud",
  "mean_delta_hiv_sud", "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud"
)


df_input_tpe <- deflate(
  data = df_input_tpe,
  val_columns = cost_columns,
  old_year = "year_id",
  new_year = 2019
)


# Load and clean the mapping file
df_map <- read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
  select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
  mutate(
    acause_lvl2      = if_else(acause == "hiv", "hiv", acause_lvl2),
    cause_name_lvl2  = if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
    acause_lvl2      = if_else(acause == "std", "std", acause_lvl2),
    cause_name_lvl2  = if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
  ) %>% select(-acause) %>% unique()

# Join with df_map (cause map table)
df_input_tpe <- df_input_tpe %>%
  left_join(df_map, by = "acause_lvl2") %>%
  relocate(acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1, .before = year_id)

##----------------------------------------------------------------
## 4.3 Save adjusted table Two Part Estimates to CSV
##----------------------------------------------------------------

# Save the aggregated results
output_file_tpe <- file.path(output_folder, "04.Two_Part_Estimates_inflation_adjusted_aggregated.csv")
write_csv(df_input_tpe, output_file_tpe)
cat("table saved", output_file_tpe, "\n")

##----------------------------------------------------------------
## 4.4 Create sub-tables from main aggregated TPE table
##----------------------------------------------------------------

# Helper function to calculate weighted means
weighted_mean_all <- function(df, group_cols, value_cols, weight_col) {
  df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(across(all_of(value_cols), 
                     ~ weighted.mean(.x, get(weight_col), na.rm = TRUE),
                     .names = "{.col}"),
    total_bin_count = sum(.data[[weight_col]], na.rm = TRUE),
    .groups = "drop")
}

# Columns to *always* include in the grouping
cause_cols <- c("acause_lvl1", "cause_name_lvl1", "acause_lvl2", "cause_name_lvl2")

# Value columns
value_cols <- c(
  "mean_cost","lower_ci","upper_ci",
  "mean_cost_hiv","lower_ci_hiv","upper_ci_hiv",
  "mean_cost_sud","lower_ci_sud","upper_ci_sud",
  "mean_cost_hiv_sud","lower_ci_hiv_sud","upper_ci_hiv_sud",
  "mean_delta_hiv","lower_ci_delta_hiv","upper_ci_delta_hiv",
  "mean_delta_sud","lower_ci_delta_sud","upper_ci_delta_sud",
  "mean_delta_hiv_sud","lower_ci_delta_hiv_sud","upper_ci_delta_hiv_sud"
)

# By cause (Level 2 + Level 1)
by_cause <- weighted_mean_all(master_table_tpe, cause_cols, value_cols, "total_row_count")
write_csv(by_cause, file.path(output_folder, "04.Two_Part_Estimates_subtable_by_cause.csv"))

# By year (preserving both cause levels)
by_year <- weighted_mean_all(master_table_tpe, c(cause_cols, "year_id"), value_cols, "total_row_count")
write_csv(by_year, file.path(output_folder, "04.Two_Part_Estimates_subtable_by_year.csv"))

# By type of care (preserving both cause levels)
by_toc <- weighted_mean_all(master_table_tpe, c(cause_cols, "toc"), value_cols, "total_row_count")
write_csv(by_toc, file.path(output_folder, "04.Two_Part_Estimates_subtable_by_toc.csv"))

# By race (preserving both cause levels)
by_race <- weighted_mean_all(master_table_tpe, c(cause_cols, "race_cd"), value_cols, "total_row_count")
write_csv(by_race, file.path(output_folder, "04.Two_Part_Estimates_subtable_by_race.csv"))

# By age group (preserving both cause levels)
by_age <- weighted_mean_all(master_table_tpe, c(cause_cols, "age_group_years_start"), value_cols, "total_row_count")
write_csv(by_age, file.path(output_folder, "04.Two_Part_Estimates_subtable_by_age.csv"))

# Example: By cause and year (joint stratification, cause levels always present)
by_cause_year <- weighted_mean_all(master_table_tpe, c(cause_cols, "year_id"), value_cols, "total_row_count")
write_csv(by_cause_year, file.path(output_folder, "04.Two_Part_Estimates_subtable_by_cause_year.csv"))

cat("All subtables (with cause levels preserved) have been saved to CSV in ", output_folder, "\n")




