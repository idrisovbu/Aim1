##----------------------------------------------------------------
##' Title: B3_aggregator.R
##'
##' Purpose: Aggregates the outputs from the B2 scripts (output folders 01, 02 (unused ATM), 03, 04) and creates summary files
##' 
##' Outputs: /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/01.Summary_Statistics_inflation_adjusted_aggregated.csv
##           /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/03.Meta_Statistics_aggregated.csv
##           /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/03.Meta_Statistics_total_bene_by_year.csv
##           /mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/<date>/03.Meta_Statistics_total_bene_by_year_by_toc.csv
##
## TODO: Add "inflation_adjusted" Y/N column to the inflation adjusted tables/ subtables
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
input_by_cause <- file.path(base_dir, "04.Two_Part_Estimates", date_of_input, "results")

# Define output directory
date_of_output <- format(Sys.time(), "%Y%m%d")
output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output)

# Ensure directory exists
ensure_dir_exists(output_folder)


##----------------------------------------------------------------
## Helper function to Calculate weighted means for multiple columns within grouped strata.
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

### the funciton below will remove cases when raw count is zero:
# weighted_mean_all <- function(df, group_cols, value_cols, weight_col) {
#   df %>%
#     dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) %>%
#     dplyr::summarise(
#       total_bin_count = sum(.data[[weight_col]], na.rm = TRUE),
#       dplyr::across(
#         dplyr::all_of(value_cols),
#         ~ {
#           w <- .data[[weight_col]]
#           sw <- sum(w, na.rm = TRUE)
#           if (is.finite(sw) && sw > 0) weighted.mean(.x, w, na.rm = TRUE) else NA_real_
#         },
#         .names = "{.col}"
#       ),
#       .groups = "drop"
#     ) %>%
#     # optional: drop zero-weight groups entirely
#     dplyr::filter(total_bin_count > 0)
# }



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
# Source of CPI https://www.bls.gov/cpi/tables/supplemental-files/historical-cpi-u-202404.pdf

# Adjust cost variables
cost_columns <- c("avg_cost_per_bene", "max_cost_per_bene", "quantile_99_cost_per_bene","sum_cost_per_group")

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
## 1.3 Create Inflation Adjusted Subtables
##----------------------------------------------------------------

# Only numeric columns!
value_cols <- c(
  "has_hiv", "has_sud", "has_hepc", "avg_cost_per_bene", "max_cost_per_bene",
  "quantile_99_cost_per_bene", "sum_cost_per_group", "n_benes_per_group",
  "avg_encounters_per_bene", "sum_encounters_per_group"
)

by_cause_ss <- weighted_mean_all(df = df_adj_ss,group_cols = "acause_lvl2", value_cols = value_cols,weight_col = "n_benes_per_group")

by_year_ss <- weighted_mean_all(df = df_adj_ss,group_cols = "year_id",value_cols = value_cols,weight_col = "n_benes_per_group")

by_toc_ss <- weighted_mean_all(
  df = df_adj_ss,
  group_cols = "toc",
  value_cols = value_cols,
  weight_col = "n_benes_per_group"
)

by_race_ss <- weighted_mean_all(
  df = df_adj_ss,
  group_cols = "race_cd",
  value_cols = value_cols,
  weight_col = "n_benes_per_group"
)

by_age_ss <- weighted_mean_all(
  df = df_adj_ss,
  group_cols = "age_group_years_start",
  value_cols = value_cols,
  weight_col = "n_benes_per_group"
)

by_cause_year_ss <- weighted_mean_all(
  df = df_adj_ss,
  group_cols = c("acause_lvl2", "year_id"),
  value_cols = value_cols,
  weight_col = "n_benes_per_group"
)

write_csv(by_cause_ss,      file.path(output_folder, "01.Summary_Statistics_subtable_by_cause.csv"))
write_csv(by_year_ss,       file.path(output_folder, "01.Summary_Statistics_subtable_by_year.csv"))
write_csv(by_toc_ss,        file.path(output_folder, "01.Summary_Statistics_subtable_by_toc.csv"))
write_csv(by_race_ss,       file.path(output_folder, "01.Summary_Statistics_subtable_by_race.csv"))
write_csv(by_age_ss,        file.path(output_folder, "01.Summary_Statistics_subtable_by_age.csv"))
write_csv(by_cause_year_ss, file.path(output_folder, "01.Summary_Statistics_subtable_by_cause_year.csv"))

cat("All descriptive summary subtables have been saved to CSV in", output_folder, "\n")


##----------------------------------------------------------------
## 2. Aggregate & Summarize - 02.Regression_Estimates
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list_re <- list.files(input_regression_estimates, pattern = "\\.csv$", full.names = TRUE) 

# Read and combine all files into a single dataframe
df_input_re <- purrr::map_dfr(files_list_re, ~readr::read_csv(.x, show_col_types = FALSE))

# Save the aggregated Regression Estimates as CSV
output_file_re <- file.path(output_folder, "02.Regression_Estimates_aggregated.csv")
write_csv(df_input_re, output_file_re)
cat("Regression estimates table saved:", output_file_re, "\n")

##----------------------------------------------------------------
## 2.1 Regression Subtables
##----------------------------------------------------------------

#  Number of significant effects by effect type
sig_counts <- df_input_re %>%
  mutate(
    sig_logit = !is.na(p_logit) & p_logit < 0.05,
    sig_gamma = !is.na(p_gamma) & p_gamma < 0.05,
    is_interaction = grepl(":", variable)
  ) %>%
  group_by(is_interaction) %>%
  summarise(
    n = n(),
    n_sig_logit = sum(sig_logit, na.rm = TRUE),
    n_sig_gamma = sum(sig_gamma, na.rm = TRUE)
  )
write_csv(sig_counts, file.path(output_folder, "02.Regression_Estimates_subtable_sig_counts.csv"))

# Coverage table by year and age group
estimate_count <- df_input_re %>% count(year_id, age_group_years_start, name = "n_estimates")
write_csv(estimate_count, file.path(output_folder, "02.Regression_Estimates_subtable_n_estimates_by_year_age.csv"))

estimate_signif <- df_input_re %>%
  group_by(year_id, age_group_years_start) %>%
  summarise(
    n_estimates = n(),
    n_sig_logit = sum(!is.na(p_logit) & p_logit < 0.05, na.rm = TRUE),
    n_sig_gamma = sum(!is.na(p_gamma) & p_gamma < 0.05, na.rm = TRUE),
    .groups = "drop"
  )

write_csv(estimate_signif, file.path(output_folder, "02.Regression_Estimates_subtable_n_estimates_signif_by_year_age.csv"))


# Add significance flags to data
df_input_re <- df_input_re %>%
  mutate(
    sig_logit = !is.na(p_logit) & p_logit < 0.05,
    sig_gamma = !is.na(p_gamma) & p_gamma < 0.05
  )


#  By cause only
by_cause <- df_input_re %>%
  group_by(variable) %>%
  summarise(
    n_estimates = n(),
    prop_sig_logit = mean(sig_logit, na.rm = TRUE),
    prop_sig_gamma = mean(sig_gamma, na.rm = TRUE),
    .groups = "drop"
  )
write_csv(by_cause, file.path(output_folder, "02.Regression_Estimates_subtable_sig_by_cause.csv"))

colnames(df_input_re)
unique(df_input_re$variable)

by_toc <- df_input_re %>%
  filter(grepl("^toc_fact", variable)) %>%
  group_by(variable) %>%
  summarise(
    n_estimates = n(),
    prop_sig_logit = mean(sig_logit, na.rm = TRUE),
    prop_sig_gamma = mean(sig_gamma, na.rm = TRUE),
    .groups = "drop"
  )

write_csv(by_toc, file.path(output_folder, "02.Regression_Estimates_subtable_sig_by_toc.csv"))


#  By age group only
by_age <- df_input_re %>%
  group_by(age_group_years_start) %>%
  summarise(
    n_estimates = n(),
    prop_sig_logit = mean(sig_logit, na.rm = TRUE),
    prop_sig_gamma = mean(sig_gamma, na.rm = TRUE),
    .groups = "drop"
  )
write_csv(by_age, file.path(output_folder, "02.Regression_Estimates_subtable_sig_by_age_group.csv"))

# By year only
by_year <- df_input_re %>%
  group_by(year_id) %>%
  summarise(
    n_estimates = n(),
    prop_sig_logit = mean(sig_logit, na.rm = TRUE),
    prop_sig_gamma = mean(sig_gamma, na.rm = TRUE),
    .groups = "drop"
  )
write_csv(by_year, file.path(output_folder, "02.Regression_Estimates_subtable_sig_by_year.csv"))

cat("All regression significance sub-tables saved in", output_folder, "\n")


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
output_file_by_year <- file.path(output_folder, "03.Meta_Statistics_subtable_bene_by_year.csv")
write_csv(total_bene_by_year, output_file_by_year)

# Save total unique beneficiaries by year and toc
output_file_by_year_toc <- file.path(output_folder, "03.Meta_Statistics_subtable_bene_by_year_by_toc.csv")
write_csv(total_bene_by_year_toc, output_file_by_year_toc)


##----------------------------------------------------------------
## 4. Aggregate & Summarize - 04.Two_Part_Estimates: BY CAUSE
##----------------------------------------------------------------

##----------------------------------------------------------------
## 4. Aggregate & Summarize - 04.Two_Part_Estimates: BY CAUSE (exclude HIV/SUD)
##----------------------------------------------------------------

## 4.1 Read in data (exclude any HIV / SUD files by path and by content)
files_list_by_cause_all <- list.files(input_by_cause, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)

# helper: does any path segment indicate hiv or sud/subs?
path_is_hiv_or_sud <- function(x) {
  segs <- unlist(strsplit(x, "/|\\\\"))
  any(grepl("^hiv$", segs, ignore.case = TRUE) |
        grepl("^_sud$|^_subs$", segs, ignore.case = TRUE))
}

files_list_by_cause <- files_list_by_cause_all[!vapply(files_list_by_cause_all, path_is_hiv_or_sud, logical(1))]

# read in tables
df_input_by_cause <- rbindlist(
  lapply(files_list_by_cause, fread)
  #,idcol = "source_file" # uncomment this if you want to add the file number
)

# # Checking why there are NA values in certain rows / columns
# DT <- df_input_by_cause[!complete.cases(df_input_by_cause)]
# 
# View(df_input_by_cause[apply(is.na(df_input_by_cause), 1, any)])
# df_nas <- df_input_by_cause[apply(is.na(df_input_by_cause), 1, any)]
# write.csv(df_nas, file = file.path(base_dir, "df_nas.csv"))

# extra safety: if a few rows slipped in, drop them by content
if ("acause_lvl2" %in% names(df_input_by_cause)) {
  df_input_by_cause <- df_input_by_cause %>%
    dplyr::filter(!acause_lvl2 %in% c("hiv", "_sud", "_subs"))
}

## 4.2 Inflation adjustment and mapping
cost_columns <- c(
  "mean_cost","lower_ci","upper_ci",
  "mean_cost_hiv","lower_ci_hiv","upper_ci_hiv",
  "mean_cost_sud","lower_ci_sud","upper_ci_sud",
  "mean_cost_hiv_sud","lower_ci_hiv_sud","upper_ci_hiv_sud",
  "mean_delta_hiv","lower_ci_delta_hiv","upper_ci_delta_hiv",
  "mean_delta_sud","lower_ci_delta_sud","upper_ci_delta_sud",
  "mean_delta_hiv_sud","lower_ci_delta_hiv_sud","upper_ci_delta_hiv_sud"
)

if (nrow(df_input_by_cause)) {
  df_input_by_cause <- deflate(
    data        = df_input_by_cause,
    val_columns = intersect(cost_columns, names(df_input_by_cause)),
    old_year    = "year_id",
    new_year    = 2019
  )
}

# cause names
df_map <- readr::read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
  dplyr::select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
  dplyr::mutate(
    acause_lvl2     = dplyr::if_else(acause == "hiv", "hiv", acause_lvl2),
    cause_name_lvl2 = dplyr::if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
    acause_lvl2     = dplyr::if_else(acause == "std", "std", acause_lvl2),
    cause_name_lvl2 = dplyr::if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
  ) %>%
  dplyr::select(-acause) %>%
  dplyr::distinct()

if (nrow(df_input_by_cause)) {
  df_input_by_cause <- df_input_by_cause %>%
    dplyr::left_join(df_map, by = "acause_lvl2") %>%
    dplyr::relocate(acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1, .before = year_id)
}

# ensure a weight exists; NA weights -> 0 so they don’t distort means
if (!("total_row_count" %in% names(df_input_by_cause))) df_input_by_cause$total_row_count <- NA_real_
df_input_by_cause$total_row_count <- dplyr::coalesce(df_input_by_cause$total_row_count, 0)

## 4.3 Save adjusted table Two Part Estimates to CSV
output_file_by_cause_full <- file.path(output_folder, "04.By_cause_inflation_adjusted_aggregated_unfiltered.csv")
readr::write_csv(df_input_by_cause, output_file_by_cause_full)
cat("Full (all non-HIV/SUD causes) table saved to:", output_file_by_cause_full, "\n")

# optional additional exclusions beyond HIV/SUD if desired
causes_to_exclude <- c("_mental", "mater_neonat", "_rf", "_well", "_sense") # Excluding these + HIV & SUD results in 18 causes
df_input_by_cause_filtered <- df_input_by_cause %>%
  dplyr::filter(!acause_lvl2 %in% causes_to_exclude)

output_file_by_cause_filtered <- file.path(output_folder, "04.By_cause_inflation_adjusted_aggregated.csv")
readr::write_csv(df_input_by_cause_filtered, output_file_by_cause_filtered)
cat("Filtered (selected causes removed) table saved to:", output_file_by_cause_filtered, "\n")

## 4.4 Subtables from the main aggregated table
cause_cols <- c("acause_lvl1", "cause_name_lvl1", "acause_lvl2", "cause_name_lvl2")
value_cols <- c(
  "mean_cost","lower_ci","upper_ci",
  "mean_cost_hiv","lower_ci_hiv","upper_ci_hiv",
  "mean_cost_sud","lower_ci_sud","upper_ci_sud",
  "mean_cost_hiv_sud","lower_ci_hiv_sud","upper_ci_hiv_sud",
  "mean_delta_hiv_only","lower_ci_delta_hiv_only","upper_ci_delta_hiv_only",
  "mean_delta_sud_only","lower_ci_delta_sud_only","upper_ci_delta_sud_only",
  "mean_delta_hiv_sud","lower_ci_delta_hiv_sud","upper_ci_delta_hiv_sud"
)

by_cause <- weighted_mean_all(df_input_by_cause_filtered, cause_cols, value_cols, "total_row_count")
readr::write_csv(by_cause, file.path(output_folder, "04.By_cause_subtable_by_cause.csv"))

by_year <- weighted_mean_all(df_input_by_cause_filtered, c(cause_cols, "year_id"), value_cols, "total_row_count")
readr::write_csv(by_year, file.path(output_folder, "04.By_cause_subtable_by_year.csv"))

by_race <- weighted_mean_all(df_input_by_cause_filtered, c(cause_cols, "race_cd"), value_cols, "total_row_count")
readr::write_csv(by_race, file.path(output_folder, "04.By_cause_subtable_by_race.csv"))

by_age <- weighted_mean_all(df_input_by_cause_filtered, c(cause_cols, "age_group_years_start"), value_cols, "total_row_count")
readr::write_csv(by_age, file.path(output_folder, "04.By_cause_subtable_by_age.csv"))

by_cause_year <- weighted_mean_all(df_input_by_cause_filtered, c(cause_cols, "year_id"), value_cols, "total_row_count")
readr::write_csv(by_cause_year, file.path(output_folder, "04.By_cause_subtable_by_cause_year.csv"))

cat("All by-cause subtables saved in", output_folder, "\n")

# Elements `mean_delta_hiv`, `lower_ci_delta_hiv`, `upper_ci_delta_hiv`, `mean_delta_sud`, `lower_ci_delta_sud`, etc. don't exist.

##----------------------------------------------------------------
## 5. Aggregate & Summarize - HIV and SUD (by-cause folders)
##----------------------------------------------------------------

# Find all CSVs recursively under results/
all_csvs <- list.files(input_by_cause, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)

# Helper to check path segments
path_has <- function(x, patt) {
  segs <- unlist(strsplit(x, "/|\\\\"))
  any(grepl(patt, segs, ignore.case = TRUE))
}

# Partition file lists
hiv_files <- all_csvs[vapply(all_csvs, function(x) path_has(x, "^hiv$"), logical(1))]
sud_files <- all_csvs[vapply(all_csvs, function(x) path_has(x, "^_sud$|^_subs$"), logical(1))]

cat("HIV files:", length(hiv_files), " | SUD files:", length(sud_files), "\n")
if (length(hiv_files) == 0L && length(sud_files) == 0L) {
  stop("No HIV or SUD CSV files found under: ", by_cause)
}

# Read
df_hiv_raw <- if (length(hiv_files)) purrr::map_dfr(hiv_files, ~readr::read_csv(.x, show_col_types = FALSE)) else NULL
df_sud_raw <- if (length(sud_files)) purrr::map_dfr(sud_files, ~readr::read_csv(.x, show_col_types = FALSE)) else NULL

## 5.2 Deflate (only columns present)
cost_columns <- c(
  "mean_cost_hiv","lower_ci_hiv","upper_ci_hiv",
  "mean_cost_sud","lower_ci_sud","upper_ci_sud",
  "mean_cost_hiv_sud","lower_ci_hiv_sud","upper_ci_hiv_sud",
  "mean_delta_hiv_only","lower_ci_delta_hiv_only","upper_ci_delta_hiv_only",
  "mean_delta_sud_only","lower_ci_delta_sud_only","upper_ci_delta_sud_only"
)

deflate_if_present <- function(df) {
  if (is.null(df) || nrow(df) == 0L) return(df)
  deflate(df, val_columns = intersect(cost_columns, names(df)), old_year = "year_id", new_year = 2019)
}

df_hiv <- deflate_if_present(df_hiv_raw)
df_sud <- deflate_if_present(df_sud_raw)

# mapping
df_map <- readr::read_csv("/mnt/share/dex/us_county/maps/causelist_figures.csv", show_col_types = FALSE) %>%
  dplyr::select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
  dplyr::mutate(
    acause_lvl2     = dplyr::if_else(acause == "hiv", "hiv", acause_lvl2),
    cause_name_lvl2 = dplyr::if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
    acause_lvl2     = dplyr::if_else(acause == "std", "std", acause_lvl2),
    cause_name_lvl2 = dplyr::if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
  ) %>%
  dplyr::select(-acause) %>%
  dplyr::distinct()

join_and_order <- function(df) {
  if (is.null(df) || nrow(df) == 0L) return(df)
  df %>%
    dplyr::left_join(df_map, by = "acause_lvl2") %>%
    dplyr::relocate(acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1, .before = year_id)
}

df_hiv <- join_and_order(df_hiv)
df_sud <- join_and_order(df_sud)

# ensure weight exists and is usable
ensure_weight <- function(df) {
  if (is.null(df) || nrow(df) == 0L) return(df)
  if (!("total_row_count" %in% names(df))) df$total_row_count <- NA_real_
  df$total_row_count <- dplyr::coalesce(df$total_row_count, 0)
  df
}
df_hiv <- ensure_weight(df_hiv)
df_sud <- ensure_weight(df_sud)

## 5.3 Save adjusted aggregated tables (separate HIV and SUD)
if (!is.null(df_hiv) && nrow(df_hiv)) {
  out_hiv <- file.path(output_folder, "05.HIV_inflation_adjusted_aggregated.csv")
  readr::write_csv(df_hiv, out_hiv)
  cat("HIV aggregated table saved to:", out_hiv, "\n")
} else cat("No HIV rows to save.\n")

if (!is.null(df_sud) && nrow(df_sud)) {
  out_sud <- file.path(output_folder, "05.SUD_inflation_adjusted_aggregated.csv")
  readr::write_csv(df_sud, out_sud)
  cat("SUD aggregated table saved to:", out_sud, "\n")
} else cat("No SUD rows to save.\n")

## 5.4 Subtables
cause_cols <- c("acause_lvl1","cause_name_lvl1","acause_lvl2","cause_name_lvl2")

value_cols_hiv <- c("mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv", 
                    "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud", 
                    "mean_delta_sud_only", "lower_ci_delta_sud_only", "upper_ci_delta_sud_only")
value_cols_sud <- c("mean_cost_sud", "lower_ci_sud", "upper_ci_sud", 
                    "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud", 
                    "mean_delta_hiv_only", "lower_ci_delta_hiv_only", "upper_ci_delta_hiv_only")

write_subtables <- function(df, prefix, value_cols) {
  if (is.null(df) || nrow(df) == 0L) return(invisible(NULL))
  by_cause_tbl      <- weighted_mean_all(df, cause_cols, value_cols, "total_row_count")
  by_year_tbl       <- weighted_mean_all(df, c(cause_cols, "year_id"), value_cols, "total_row_count")
  by_race_tbl       <- weighted_mean_all(df, c(cause_cols, "race_cd"), value_cols, "total_row_count")
  by_age_tbl        <- weighted_mean_all(df, c(cause_cols, "age_group_years_start"), value_cols, "total_row_count")
  by_cause_year_tbl <- weighted_mean_all(df, c(cause_cols, "year_id"), value_cols, "total_row_count")
  
  readr::write_csv(by_cause_tbl,      file.path(output_folder, paste0(prefix, "_subtable_by_cause.csv")))
  readr::write_csv(by_year_tbl,       file.path(output_folder, paste0(prefix, "_subtable_by_year.csv")))
  readr::write_csv(by_race_tbl,       file.path(output_folder, paste0(prefix, "_subtable_by_race.csv")))
  readr::write_csv(by_age_tbl,        file.path(output_folder, paste0(prefix, "_subtable_by_age.csv")))
  readr::write_csv(by_cause_year_tbl, file.path(output_folder, paste0(prefix, "_subtable_by_cause_year.csv")))
}

write_subtables(df_hiv, "05.HIV", value_cols_hiv)
write_subtables(df_sud, "05.SUD", value_cols_sud)

cat("Separate HIV and SUD subtables saved in:", output_folder, "\n")

