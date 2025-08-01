##----------------------------------------------------------------
##' Title: B3_aggregator_meta_stats.R
##'
##' Purpose:
##
##----------------------------------------------------------------
# Environment setup
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

# Defined manually
date_of_input <- "bested" # bested from 20250631
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"

# Define input directory 
input_summary_stats <- file.path(base_dir, "01.Summary_Statistics", date_of_input) 

# Define output directory
date_of_output <- format(Sys.time(), "%Y%m%d")

output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output, "aggregation_summary_stats_results")

ensure_dir_exists(output_folder)



##----------------------------------------------------------------
## 1. Read in data
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list <- list.files(input_summary_stats, pattern = "\\.csv$", full.names = TRUE) 

# Read files
df_input <- map_dfr(files_list, ~read_csv(.x, show_col_types = FALSE))



##----------------------------------------------------------------
## 2. Inflation adjustment
##----------------------------------------------------------------

df_sum <- df_input
# Inflation adjustment setup
inflation_raw <- data.frame(
  year_id = 2008:2024,
  inflation_rate = c(0.001, 0.027, 0.015, 0.03, 0.017, 0.015, 0.008, 0.007, 0.021, 0.021,
                     0.019, 0.023, 0.014, 0.07, 0.065, 0.034, 0.029)
)

# Create deflators based on years in data
years_present <- sort(unique(df_sum$year_id))
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

df_adj <- df_sum %>%
  left_join(inflation_data, by = "year_id") %>%
  mutate(across(all_of(cost_columns), ~ .x / deflator))

# Create weighted summary table
summary_table <- df_adj %>%
  group_by(acause, has_hiv, has_sud, has_hepc, race_cd, toc, age_group_years_start, code_system, year_id, file_type) %>%
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
write_csv(summary_table, file.path(output_folder, "weighted_summary_table.csv"))
cat("✅ Inflation-adjusted summary table saved to:", file.path(output_folder, "feighted_summary_table.csv"), "\n")


##########




##----------------------------------------------------------------
##  Saving files
##----------------------------------------------------------------


# Save the aggregated results
output_file <- file.path(output_folder, "meta_statistics.csv")
write_csv(df_input, output_file)
cat("table saved", output_file, "\n")








##########
##########
# 
# df_regr <- map_dfr(files_list_regr, ~read_csv(.x, show_col_types = FALSE) %>%
#                      mutate(source = "reg"))
# # Save raw aggregated master file
# write_csv(df_sum, file.path(output_folder, "agregated_regression_table.csv"))
# 



##########

