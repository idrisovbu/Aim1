##----------------------------------------------------------------
##' Title: file_checker.R
##'
##' Purpose: Checks to ensure we have all the files we expect for each part of the pipeline
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
## 0.1 Create directory folders 
##----------------------------------------------------------------
# Ensure the output directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Define input directory 
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"

date_summary_stats <- "20250925"
input_summary_stats <- file.path(base_dir, "01.Summary_Statistics", date_summary_stats)

date_regression <- "20250925" #9/22 was experimental run with the "has_" variables included in the model
input_regression_estimates <- file.path(base_dir, "02.Regression_Estimates", date_regression)

date_meta_stats <- "20250925"
input_meta_stats <- file.path(base_dir, "03.Meta_Statistics", date_meta_stats) 

date_tpe <- "20250925"
input_by_cause <- file.path(base_dir, "04.Two_Part_Estimates", date_tpe, "by_cause/results")

# Define output directory
date_of_output <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output)

# Ensure directory exists
ensure_dir_exists(output_folder)


##----------------------------------------------------------------
## 0.2 Helper function to Calculate weighted means for multiple columns within grouped strata.
##----------------------------------------------------------------
#' @param df         Input dataframe.
#' @param group_cols Character vector of columns to group by (strata).
#' @param value_cols Character vector of columns for which to calculate weighted means.
#' @param weight_col Character; name of the column to use as weights.
#'
#' @return A tibble/data.frame summarizing each group with weighted means for value columns
#'         and the sum of the weights (total_bin_count).
#'
#' @examples
#' result <- weighted_mean_all(
#'   df = mydata,
#'   group_cols = c("sex", "age_group", "year"),
#'   value_cols = c("cost", "utilization"),
#'   weight_col = "n_patients"
#' )
weighted_mean_all <- function(df, group_cols, value_cols, weight_col) {
  df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      across(
        all_of(value_cols),
        ~ weighted.mean(.x, .data[[weight_col]], na.rm = TRUE),
        .names = "{.col}"
      ),
      total_bin_count = sum(.data[[weight_col]], na.rm = TRUE),
      .groups = "drop"
    )
}


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