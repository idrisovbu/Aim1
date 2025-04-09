##----------------------------------------------------------------
##' Title: 03_aggregator_two_part_model.R
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
##' Inputs:
##'  - CSV files from the two-part model (`02.Two-Part_Results/`).
##'  - Includes metadata: year, age group, and race.
##'
##' Outputs:
##'  - Aggregated CSV (`two_part_model_summary.csv`) with cost comparisons.
##'  - All outputs are saved inside a date-stamped folder under `/output_aim1/YYYYMMDD/03.Table_2(two-part_summary)/`.
##'
##' Author: Bulat Idrisov
##' Date: 2025-03-06
##' Version: 1.0
##'
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
# Define base input directory
base_input_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"

date_of_input_rx <- "20250403"
date_of_input_f2t <-"20250325"

# Get today's date in YYYYMMDD format
#today <- format(Sys.Date(), "%Y%m%d")
#defined manually
#today <- "20250325"

# Define input folder where CSV files are located
input_folder_f2t <- file.path(base_input_dir, date_of_input_f2t, "03.Two-Part_Results") #F2T
input_folder_rx <- file.path(base_input_dir, date_of_input_rx,"03.Rx_Two-Part_Results") #rx


# Define output directory
#Get today's date in YYYYMMDD format
today <- format(Sys.Date(), "%Y%m%d")

# Define dynamically created output directory
output_folder <- file.path(base_input_dir, today, "Agregator(two-part_summary)")

# Ensure output directory exists
dir.create(output_folder, recursive = TRUE, showWarnings = FALSE)

# List all CSV files in the input directory
#files_list <- list.files(input_folder, full.names = TRUE, pattern = "*.csv")
files_list_f2t <- list.files(input_folder_f2t, pattern = "\\.csv$", full.names = TRUE)
files_list_rx <- list.files(input_folder_rx, pattern = "\\.csv$", full.names = TRUE)


# Read all CSVs and combine them into one data frame
#df_input <- files_list_f2t %>%
 # map_dfr(read_csv)  # Reads and binds all files while keeping metadata columns

##############################
##############################
##############################
# Read aF2T files
df_f2t <- map_dfr(files_list_f2t, ~read_csv(.x, show_col_types = FALSE))

# Read rx files
df_rx <- map_dfr(files_list_rx, ~read_csv(.x, show_col_types = FALSE))

# Combine into one master table
df_input <- bind_rows(df_f2t, df_rx)


# **Ensure metadata columns exist**
if (!all(c("run_id", "toc", "year_id", "code_system", "age_group") %in% colnames(df_input))) {
  stop("One or more metadata columns are missing from the input files!")
}

figures_folder <- file.path(output_folder, "figures")

# Ensure figures directory exists
dir.create(figures_folder, recursive = TRUE, showWarnings = FALSE)

# Plotting Function
save_plot <- function(plot_obj, filename) {
  ggsave(filename = file.path(figures_folder, filename),
         plot = plot_obj, width = 9, height = 6, dpi = 300,
         device = "jpeg")
}

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

cost_columns <- c("mean_cost_hiv_0", "lower_ci_cost_hiv_0", "upper_ci_cost_hiv_0",
                  "mean_cost_hiv_1", "lower_ci_cost_hiv_1", "upper_ci_cost_hiv_1",
                  "mean_cost_delta", "lower_ci_delta", "upper_ci_delta")

df_input_adj <- df_input_adj %>%
  mutate(across(all_of(cost_columns), ~ . / deflator))

# Create master table with inflation-adjusted, weighted averages
master_table <- df_input_adj %>%
  group_by(acause, year_id, toc, race_cd, age_group) %>%
  summarise(
    mean_cost_hiv_0     = weighted.mean(mean_cost_hiv_0, avg_bin_count, na.rm = TRUE),
    lower_ci_cost_hiv_0 = weighted.mean(lower_ci_cost_hiv_0, avg_bin_count, na.rm = TRUE),
    upper_ci_cost_hiv_0 = weighted.mean(upper_ci_cost_hiv_0, avg_bin_count, na.rm = TRUE),
    
    mean_cost_hiv_1     = weighted.mean(mean_cost_hiv_1, avg_bin_count, na.rm = TRUE),
    lower_ci_cost_hiv_1 = weighted.mean(lower_ci_cost_hiv_1, avg_bin_count, na.rm = TRUE),
    upper_ci_cost_hiv_1 = weighted.mean(upper_ci_cost_hiv_1, avg_bin_count, na.rm = TRUE),
    
    mean_cost_delta     = weighted.mean(mean_cost_delta, avg_bin_count, na.rm = TRUE),
    lower_ci_delta      = weighted.mean(lower_ci_delta, avg_bin_count, na.rm = TRUE),
    upper_ci_delta      = weighted.mean(upper_ci_delta, avg_bin_count, na.rm = TRUE),
    
    total_bin_count     = sum(avg_bin_count, na.rm = TRUE),
    .groups = "drop"
  )



# Save the aggregated results
output_file <- file.path(output_folder, "weighted_summary_two_part_table.csv")
write_csv(master_table, output_file)

cat("aster table saved as 'master_table_2019usd.csv' with all costs in 2019 dollars", output_file, "\n")

