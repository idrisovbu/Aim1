##----------------------------------------------------------------
##' Title: B3_aggregator_two_part_model.R
##'
##' Purpose:
##' This script aggregates and summarizes results from the two-part regression model 
##' for Medicare beneficiaries with HIV. It consolidates cost estimates across diseases, 
##' HIV status, and demographic groups.
##'
##' The script performs the following steps:
##'  1. Reads in all available CSV results from the two-part regression model.
##'  2. Computes mean incremental cost per disease and HIV status.
##'  3. Summarizes cost variations across race and demographic groups.
##'  4. Merges summary tables into a final structured output.
##'  5. Saves results to a dynamically created output directory.
##'
##'
##' Author: Bulat Idrisov
##----------------------------------------------------------------
# Environment setup
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,readr,purrr)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

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

##########################################
# Read in all csv files
##########################################
#defined manually
date_of_input <- "20250527"
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"


# Define input directory 
input_two_part <- file.path(base_dir, date_of_input, "04.Two_Part_Estimates") 

# Get the list of all CSV files from the input directory
files_list <- list.files(input_two_part, pattern = "\\.csv$", full.names = TRUE) 

# Define output directory
output_folder <- file.path(base_dir, date_of_input, "Agregators")
dir.create(output_folder, recursive = TRUE, showWarnings = FALSE)


# Ensure the output directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}


##############################
##############################
##############################
# Read files
df_input <- map_dfr(files_list, ~read_csv(.x, show_col_types = FALSE))

# Save the aggregated results
output_file <- file.path(output_folder, "two_part_table.csv")
write_csv(df_input, output_file)
cat("table saved", output_file, "\n")



# # Ensure figures directory exists
# dir.create(figures_folder, recursive = TRUE, showWarnings = FALSE)
# 
# # Plotting Function
# save_plot <- function(plot_obj, filename) {
#   ggsave(filename = file.path(figures_folder, filename),
#          plot = plot_obj, width = 9, height = 6, dpi = 300,
#          device = "jpeg")
# }

# Replace zeros with NA to avoid bias in averaging
df_input[df_input == 0] <- NA

##########################################
#  Summarize results: Compute mean incremental cost per disease and HIV status
##########################################


# Only keep years found in df_input
inflation_raw <- data.frame(
  year_id = 2008:2024,
  inflation_rate = c(0.001, 0.027, 0.015, 0.03, 0.017, 0.015, 0.008, 0.007, 0.021, 0.021,
                     0.019, 0.023, 0.014, 0.07, 0.065, 0.034, 0.029)
)

# Filter to match your df_input years
years_present <- sort(unique(df_input$year_id))
inflation_data <- inflation_raw %>%
  filter(year_id %in% years_present) %>%
  arrange(year_id) %>%
  mutate(
    inflation_factor = cumprod(1 + inflation_rate),
    deflator = inflation_factor / inflation_factor[year_id == 2019]  # normalize to 2019 dollars
  ) %>%
  select(year_id, deflator)


# Merge and adjust costs
df_input_adj <- df_input %>%
  left_join(inflation_data, by = "year_id")

cost_columns <- c(
  "mean_cost_neither", "mean_cost_hiv_only", "mean_cost_sud_only", "mean_cost_hiv_sud",
  "lower_ci_neither", "upper_ci_neither",
  "lower_ci_hiv_only", "upper_ci_hiv_only",
  "lower_ci_sud_only", "upper_ci_sud_only",
  "lower_ci_hiv_sud", "upper_ci_hiv_sud",
  "mean_delta_hiv_only", "mean_delta_sud_only", "mean_delta_hiv_sud",
  "lower_ci_delta_hiv_only", "upper_ci_delta_hiv_only",
  "lower_ci_delta_sud_only", "upper_ci_delta_sud_only",
  "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud"
)

df_input_adj <- df_input %>%
  left_join(inflation_data, by = "year_id") %>%
  mutate(across(all_of(cost_columns), ~ . / deflator))

master_table <- df_input_adj %>%
  group_by(acause, year_id, toc, race_cd, age_group_years_start) %>%
  summarise(
    mean_cost_neither    = weighted.mean(mean_cost_neither, avg_bin_count, na.rm = TRUE),
    mean_cost_hiv_only   = weighted.mean(mean_cost_hiv_only, avg_bin_count, na.rm = TRUE),
    mean_cost_sud_only   = weighted.mean(mean_cost_sud_only, avg_bin_count, na.rm = TRUE),
    mean_cost_hiv_sud    = weighted.mean(mean_cost_hiv_sud, avg_bin_count, na.rm = TRUE),
    
    lower_ci_neither     = weighted.mean(lower_ci_neither, avg_bin_count, na.rm = TRUE),
    upper_ci_neither     = weighted.mean(upper_ci_neither, avg_bin_count, na.rm = TRUE),
    lower_ci_hiv_only    = weighted.mean(lower_ci_hiv_only, avg_bin_count, na.rm = TRUE),
    upper_ci_hiv_only    = weighted.mean(upper_ci_hiv_only, avg_bin_count, na.rm = TRUE),
    lower_ci_sud_only    = weighted.mean(lower_ci_sud_only, avg_bin_count, na.rm = TRUE),
    upper_ci_sud_only    = weighted.mean(upper_ci_sud_only, avg_bin_count, na.rm = TRUE),
    lower_ci_hiv_sud     = weighted.mean(lower_ci_hiv_sud, avg_bin_count, na.rm = TRUE),
    upper_ci_hiv_sud     = weighted.mean(upper_ci_hiv_sud, avg_bin_count, na.rm = TRUE),
    
    mean_delta_hiv_only    = weighted.mean(mean_delta_hiv_only, avg_bin_count, na.rm = TRUE),
    mean_delta_sud_only    = weighted.mean(mean_delta_sud_only, avg_bin_count, na.rm = TRUE),
    mean_delta_hiv_sud     = weighted.mean(mean_delta_hiv_sud, avg_bin_count, na.rm = TRUE),
    lower_ci_delta_hiv_only= weighted.mean(lower_ci_delta_hiv_only, avg_bin_count, na.rm = TRUE),
    upper_ci_delta_hiv_only= weighted.mean(upper_ci_delta_hiv_only, avg_bin_count, na.rm = TRUE),
    lower_ci_delta_sud_only= weighted.mean(lower_ci_delta_sud_only, avg_bin_count, na.rm = TRUE),
    upper_ci_delta_sud_only= weighted.mean(upper_ci_delta_sud_only, avg_bin_count, na.rm = TRUE),
    lower_ci_delta_hiv_sud = weighted.mean(lower_ci_delta_hiv_sud, avg_bin_count, na.rm = TRUE),
    upper_ci_delta_hiv_sud = weighted.mean(upper_ci_delta_hiv_sud, avg_bin_count, na.rm = TRUE),
    
    total_bin_count        = sum(avg_bin_count, na.rm = TRUE),
    .groups = "drop"
  )

# df_map <- read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
#   distinct(acause,acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1)
# 
# # Join them to your summary table (master_table)
# master_table <- master_table %>%
#   left_join(df_map, by = "acause") %>%
#   relocate(acause,acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1)

# Save the aggregated results
output_file <- file.path(output_folder, "weighted_summary_two_part_table.csv")
write_csv(master_table, output_file)

cat("Master table saved as 'master_table_2019usd.csv' with all costs in 2019 dollars", output_file, "\n")
