#########################################################################################
# Title: B4_tables.R
# Description: Generates tables from the master table .csv file & from subtable csv files.
# Inputs: 05.Aggregation_Summary/bested/*.csv
# Outputs: 07.Tables/<date>/*.csv
#
# Notes:
##########################################################################################

##----------------------------------------------------------------
## 0. Clean & set environment directories, load packages
##----------------------------------------------------------------
rm(list = ls())
pacman::p_load(ggplot2, readr, tidyverse, viridis, scales, ggsci, data.table)

# Set the current date for folder naming
date_today <- format(Sys.time(), "%Y%m%d")

# Detect IHME cluster by checking for /mnt/share/limited_use
if (dir.exists("/mnt/share/limited_use")) {
  # IHME/cluster environment
  base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary", "bested")
  output_tables_dir <- file.path(base_dir, "07.Tables", date_today)
  # (add your other cluster-specific library loads here)
} else {
  # # Local development define manually
  
  # Bulat's directory
  # base_dir <- "~/Aim1WD/"
  # input_dir <- file.path(base_dir, "aim1_Input_personal_mac")
  # output_tables_dir <- file.path(base_dir, "aim1_output_personal_mac", "07.Tables/", date_today)

  # Alistair's directory
  base_dir <- "C:/Users/aches/Desktop/Stuff/Coding/Aim1WD/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary/bested/")
  output_tables_dir <- file.path(base_dir, "07.Tables/", date_today)
}

# Create output directory for today's date
if (!dir.exists(base_dir)) dir.create(base_dir, recursive = TRUE)
if (!dir.exists(output_tables_dir)) dir.create(output_tables_dir, recursive = TRUE)

##----------------------------------------------------------------
## 0.1 Functions
##----------------------------------------------------------------

# Used to convert column names in TPE tables
convert_colnames_general <- function(colnames_vec) {
  sapply(colnames_vec, function(name) {
    # Special case for cause column
    if (grepl("^acause_lvl2\\s*$", name)) return("Level 2 Cause")
    
    # Split column name
    parts <- strsplit(name, "_")[[1]]
    suffix <- tail(parts, 1)  # Last part is TOC, age, or race
    base <- paste(head(parts, -1), collapse = "_")  # The metric portion
    
    # Map known metric patterns to user-friendly labels
    label_map <- list(
      mean_cost_CI = "Mean Cost (CI)",
      mean_HIV_delta_cost_CI = "HIV Delta (CI)",
      mean_SUD_delta_cost_CI = "SUD Delta (CI)"
    )
    
    friendly_label <- label_map[[base]]
    
    # If not matched, return original name with a warning
    if (is.null(friendly_label)) {
      warning(paste("Unrecognized label pattern:", base))
      return(name)
    }
    
    paste(suffix, "-", friendly_label)
  }, USE.NAMES = FALSE)
}

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

##----------------------------------------------------------------
## 1. Read in bested 05.Aggregation_Summary data
##----------------------------------------------------------------

# Create list of available input files
file_list <- list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)

# Create a named list to store all csv files
data_list <- list()

for (file in file_list) {
  file_name <- tools::file_path_sans_ext(basename(file))
  data_list[[file_name]] <- read.csv(file, stringsAsFactors = FALSE)
}

##----------------------------------------------------------------
## 2. Create tables
##----------------------------------------------------------------

##----------------------------------------------------------------
## 2.1 MS_T1 - HIV, SUD, Hepc prevalence total, by year, toc age (percentages)
##
## This table uses the meta statistics outputs to sum the total beneficiaries by scenario (hiv, sud, hepc, and the combos)
## and calculate percentages for how many unique beneficiaries there are out of the group total, summarized by year_id
##
## TODO: Make bins not all mutually exclusive (e.g. HIV column CAN have counts for people with HIV/SUD and other combos, etc.)
##----------------------------------------------------------------

# Set df
df_ms_t1 <- data_list$`03.Meta_Statistics_aggregated`

# Cast total_unique_bene as integer type
df_ms_t1$total_unique_bene <- str_remove_all(df_ms_t1$total_unique_bene, ",")
df_ms_t1$total_unique_bene <- as.integer(df_ms_t1$total_unique_bene)

# Group by summary to get total counts
df_ms_t1 <- df_ms_t1 %>%
  group_by(year_id) %>%
  summarise(
    total_unique_bene_sum = sum(total_unique_bene),
    total_unique_hiv_bene = sum(hiv_unique_bene),
    total_unique_sud_bene = sum(sud_unique_bene),
    total_unique_hepc_bene = sum(hepc_unique_bene),
    total_unique_hiv_sud_bene = sum(hiv_and_sud_unique_bene),
    total_unique_hiv_hepc_bene = sum(hiv_and_hepc_unique_bene),
    total_unique_sud_hepc_bene = sum(sud_and_hepc_unique_bene),
    total_unique_hiv_sud_hepc_bene = sum(hiv_sud_hepc_unique_bene)
  )

# Calculate percentages
df_ms_t1 <- df_ms_t1 %>%
  mutate(
    `hiv_bene_%` = (total_unique_hiv_bene / total_unique_bene_sum),
    `sud_bene_%` = (total_unique_sud_bene / total_unique_bene_sum),
    `hepc_bene_%` = (total_unique_hepc_bene / total_unique_bene_sum), 
    `hiv_sud_bene_%` = (total_unique_hiv_sud_bene / total_unique_bene_sum), 
    `hiv_hepc_bene_%` = (total_unique_hiv_hepc_bene / total_unique_bene_sum), 
    `sud_hepc_bene_%` = (total_unique_sud_hepc_bene / total_unique_bene_sum),
    `hiv_sud_hepc_bene_%` = (total_unique_hiv_sud_hepc_bene / total_unique_bene_sum)
    )

# Format table neatly
format_ms_t1_cols <- function(count_col, percent_col) {
  formatted_cols <- paste0(format(count_col, big.mark = ","), " (", percent(percent_col, accuracy = 0.001), ")")
  return(formatted_cols)
}

df_ms_t1 <- df_ms_t1 %>% 
  mutate(
    "Total Unique Beneficiaries (count)" = format(total_unique_bene_sum, big.mark = ","),
    "Total Unique HIV Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_hiv_bene, `hiv_bene_%`),
    "Total Unique SUD Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_sud_bene, `sud_bene_%`),
    "Total Unique HEPC Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_hepc_bene, `hepc_bene_%`),
    "Total Unique HIV/SUD Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_hiv_sud_bene, `hiv_sud_bene_%`),
    "Total Unique HIV/HEPC Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_hiv_hepc_bene, `hiv_hepc_bene_%`),
    "Total Unique SUD/HEPC Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_sud_hepc_bene, `sud_hepc_bene_%`),
    "Total Unique HIV/SUD/HEPC Beneficiaries (count, %)" = format_ms_t1_cols(total_unique_hiv_sud_hepc_bene, `hiv_sud_hepc_bene_%`)
  )

# Select final columns to show in output
df_ms_t1 <- df_ms_t1[, c(1,17:24)]

# Rename "year_id" -> Year
df_ms_t1 <- rename(df_ms_t1, "Year" = "year_id")

# Save and output able as .csv
fwrite(df_ms_t1, file.path(output_tables_dir, "MS_T1.csv"))


##----------------------------------------------------------------
## 2.2 SS_T1 - Adjusted for 2019 dollars (TOC: AM, ED, HH, IP, RX drop NF) (25 (causes) x 19 (toc * 3 for none, hiv, sud))
##
## This table uses the summary statistics
##----------------------------------------------------------------

# Set df
df_ss_t1 <- data_list$`01.Summary_Statistics_inflation_adjusted_aggregated`

# Group by summary to get total counts based on toc
df_ss_t1 <- df_ss_t1 %>%
  group_by(acause_lvl2, has_hiv, has_sud, has_hepc, toc) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, w = total_unique_bene)
  )

# Add labels for scenarios 
df_ss_t1 <- df_ss_t1 %>%
  mutate(
    condition = case_when(
      has_hiv == 0 & has_sud == 0 & has_hepc == 0 ~ "None",
      has_hiv == 1 & has_sud == 0 & has_hepc == 0 ~ "HIV",
      has_hiv == 0 & has_sud == 1 & has_hepc == 0 ~ "SUD",
      has_hiv == 0 & has_sud == 0 & has_hepc == 1 ~ "HepC",
      has_hiv == 1 & has_sud == 1 & has_hepc == 0 ~ "HIV + SUD",
      has_hiv == 1 & has_sud == 0 & has_hepc == 1 ~ "HIV + HepC",
      has_hiv == 0 & has_sud == 1 & has_hepc == 1 ~ "SUD + HepC",
      has_hiv == 1 & has_sud == 1 & has_hepc == 1 ~ "HIV + SUD + HepC",
      TRUE ~ "Other"
    ),
    toc_condition = paste(toc, "-", condition)
  )

# Pivot wider based on scenario
df_ss_t1 <- df_ss_t1 %>%
  ungroup() %>%
  filter(condition %in% c("None", "HIV", "SUD")) %>%
  select(acause_lvl2, toc_condition, avg_cost_per_bene) %>%
  pivot_wider(
    names_from = toc_condition,
    values_from = avg_cost_per_bene
  )

# Define desired TOC and condition order
toc_order <- c("AM", "ED", "HH", "IP", "NF", "RX")
condition_order <- c("None", "HIV", "SUD", "HepC", "HIV + SUD", "HIV + HepC", "SUD + HepC", "HIV + SUD + HepC")

# Build column names in desired order: TOC first, then condition
ordered_cols <- unlist(lapply(toc_order, function(toc) {
  paste(toc, "-", condition_order)
}))

# Keep only the columns that exist
ordered_cols <- ordered_cols[ordered_cols %in% names(df_ss_t1)]

# Reorder columns with acause_lvl2 first
df_ss_t1 <- df_ss_t1 %>%
  select(acause_lvl2, all_of(ordered_cols))

# Covert to dollar amounts
for (col in colnames(df_ss_t1)) {
  if (col == "acause_lvl2") {
    next
  } else {
    df_ss_t1[[col]] <- dollar(df_ss_t1[[col]])
  }
}

# Rename acause_lvl2 -> Level 2 Cause
df_ss_t1 <- rename(df_ss_t1, "Level 2 Cause" = "acause_lvl2")

# Save and output able as .csv
fwrite(df_ss_t1, file.path(output_tables_dir, "SS_T1.csv"))


##----------------------------------------------------------------
## 2.3 SS_T2 - Average spending by age, all toc combined, 3 scenarios (25 * 16 (5 age groups 3 scenarios))
##
## This table uses the summary statistics
##----------------------------------------------------------------

# Set df
df_ss_t2 <- data_list$`01.Summary_Statistics_inflation_adjusted_aggregated`

# Group by summary to get total counts based on age_group_years_start
df_ss_t2 <- df_ss_t2 %>%
  group_by(acause_lvl2, has_hiv, has_sud, has_hepc, age_group_years_start) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, w = total_unique_bene)
  )

# Add labels for scenarios 
df_ss_t2 <- df_ss_t2 %>%
  mutate(
    condition = case_when(
      has_hiv == 0 & has_sud == 0 & has_hepc == 0 ~ "None",
      has_hiv == 1 & has_sud == 0 & has_hepc == 0 ~ "HIV",
      has_hiv == 0 & has_sud == 1 & has_hepc == 0 ~ "SUD",
      has_hiv == 0 & has_sud == 0 & has_hepc == 1 ~ "HepC",
      has_hiv == 1 & has_sud == 1 & has_hepc == 0 ~ "HIV + SUD",
      has_hiv == 1 & has_sud == 0 & has_hepc == 1 ~ "HIV + HepC",
      has_hiv == 0 & has_sud == 1 & has_hepc == 1 ~ "SUD + HepC",
      has_hiv == 1 & has_sud == 1 & has_hepc == 1 ~ "HIV + SUD + HepC",
      TRUE ~ "Other"
    ),
    age_condition = paste(age_group_years_start, "-", condition)
  )

# Pivot wider based on scenario
df_ss_t2 <- df_ss_t2 %>%
  ungroup() %>%
  filter(condition %in% c("None", "HIV", "SUD")) %>%
  select(acause_lvl2, age_condition, avg_cost_per_bene) %>%
  pivot_wider(
    names_from = age_condition,
    values_from = avg_cost_per_bene
  )

# Define desired TOC and condition order
age_order <- c("65", "70", "75", "80", "85")
condition_order <- c("None", "HIV", "SUD", "HepC", "HIV + SUD", "HIV + HepC", "SUD + HepC", "HIV + SUD + HepC")

# Build column names in desired order: TOC first, then condition
ordered_cols_age <- unlist(lapply(age_order, function(age) {
  paste(age, "-", condition_order)
}))

# Keep only the columns that exist
ordered_cols_age <- ordered_cols_age[ordered_cols_age %in% names(df_ss_t2)]

# Reorder columns with acause_lvl2 first
df_ss_t2 <- df_ss_t2 %>%
  select(acause_lvl2, all_of(ordered_cols_age))

# Covert to dollar amounts
for (col in colnames(df_ss_t2)) {
  if (col == "acause_lvl2") {
    next
  } else {
    df_ss_t2[[col]] <- dollar(df_ss_t2[[col]])
  }
}

# Rename acause_lvl2 -> Level 2 Cause
df_ss_t2 <- rename(df_ss_t2, "Level 2 Cause" = "acause_lvl2")

# Save and output able as .csv
fwrite(df_ss_t2, file.path(output_tables_dir, "SS_T2.csv"))


##----------------------------------------------------------------
## 2.4 SS_T3 - Average spending by race all toc all ages, 3 scenarios (25 * 10 (3 races, 3 scenarios))
##
## This table uses the summary statistics
##----------------------------------------------------------------

# Set df
df_ss_t3 <- data_list$`01.Summary_Statistics_inflation_adjusted_aggregated`

# Group by summary to get total counts based on race
df_ss_t3 <- df_ss_t3 %>%
  group_by(acause_lvl2, has_hiv, has_sud, has_hepc, race_cd) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, w = total_unique_bene)
  )

# Add labels for scenarios 
df_ss_t3 <- df_ss_t3 %>%
  mutate(
    condition = case_when(
      has_hiv == 0 & has_sud == 0 & has_hepc == 0 ~ "None",
      has_hiv == 1 & has_sud == 0 & has_hepc == 0 ~ "HIV",
      has_hiv == 0 & has_sud == 1 & has_hepc == 0 ~ "SUD",
      has_hiv == 0 & has_sud == 0 & has_hepc == 1 ~ "HepC",
      has_hiv == 1 & has_sud == 1 & has_hepc == 0 ~ "HIV + SUD",
      has_hiv == 1 & has_sud == 0 & has_hepc == 1 ~ "HIV + HepC",
      has_hiv == 0 & has_sud == 1 & has_hepc == 1 ~ "SUD + HepC",
      has_hiv == 1 & has_sud == 1 & has_hepc == 1 ~ "HIV + SUD + HepC",
      TRUE ~ "Other"
    ),
    race_condition = paste(race_cd, "-", condition)
  )

# Pivot wider based on scenario
df_ss_t3 <- df_ss_t3 %>%
  ungroup() %>%
  filter(condition %in% c("None", "HIV", "SUD")) %>%
  select(acause_lvl2, race_condition, avg_cost_per_bene) %>%
  pivot_wider(
    names_from = race_condition,
    values_from = avg_cost_per_bene
  )

# Define desired TOC and condition order
race_order <- c("WHT", "BLCK", "HISP")
condition_order <- c("None", "HIV", "SUD", "HepC", "HIV + SUD", "HIV + HepC", "SUD + HepC", "HIV + SUD + HepC")

# Build column names in desired order: TOC first, then condition
ordered_cols_race <- unlist(lapply(race_order, function(race) {
  paste(race, "-", condition_order)
}))

# Keep only the columns that exist
ordered_cols_race <- ordered_cols_race[ordered_cols_race %in% names(df_ss_t3)]

# Reorder columns with acause_lvl2 first
df_ss_t3 <- df_ss_t3 %>%
  select(acause_lvl2, all_of(ordered_cols_race))

# Covert to dollar amounts
for (col in colnames(df_ss_t3)) {
  if (col == "acause_lvl2") {
    next
  } else {
    df_ss_t3[[col]] <- dollar(df_ss_t3[[col]])
  }
}

# Rename acause_lvl2 -> Level 2 Cause
df_ss_t3 <- rename(df_ss_t3, "Level 2 Cause" = "acause_lvl2")

# Save and output able as .csv
fwrite(df_ss_t3, file.path(output_tables_dir, "SS_T3.csv"))


##----------------------------------------------------------------
## 2.5 TPE_T1 - Average spending by toc (all ages, all years) and delta (incremental cost) by HIV, SUD - Adjusted for 2019 dollars 
##
## This table uses the Two Part Estimates outputs
##----------------------------------------------------------------

# Set df
df_tpe_t1 <- data_list$`04.Two_Part_Estimates_inflation_adjusted_aggregated`

# Group by summary to get 
df_tpe_t1 <- df_tpe_t1 %>%
  group_by(acause_lvl2, toc) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud = weighted.mean(upper_ci_hiv_sud, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t1)) {
  if (col == "acause_lvl2" | col == "toc") {
    next
  } else {
    df_tpe_t1[[col]] <- dollar(df_tpe_t1[[col]])
  }
}

# Create mean + CI columns
df_tpe_t1 <- df_tpe_t1 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_hiv_sud, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", 
                          "upper_ci_hiv_sud", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud")

df_tpe_t1_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI")

df_tpe_t1 <- df_tpe_t1 %>%
  select(-c(cols_to_exclude)) %>%
  pivot_wider(
    names_from = toc,
    values_from = all_of(df_tpe_t1_value_cols)
  )

# Define desired TOC and condition order
toc_order <- c("AM", "ED", "HH", "IP", "RX", "NF")
condition_order <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI")

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t1 <- unlist(
  lapply(toc_order, function(toc)
    {paste0(condition_order, "_", toc)}
    )
  )

# Reorder columns with acause_lvl2 first
df_tpe_t1 <- df_tpe_t1 %>%
  select(acause_lvl2, all_of(ordered_cols_tpe_t1))

converted_names <- convert_colnames_general(colnames(df_tpe_t1))

colnames(df_tpe_t1) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t1, file.path(output_tables_dir, "TPE_T1.csv"))


##----------------------------------------------------------------
## 2.6 TPE_T2 - Average spending by age, all toc all years combined, 3 scenarios
##
## This table uses the Two Part Estimates outputs
##----------------------------------------------------------------

# Set df
df_tpe_t2 <- data_list$`04.Two_Part_Estimates_inflation_adjusted_aggregated`

# Group by summary to get 
df_tpe_t2 <- df_tpe_t2 %>%
  group_by(acause_lvl2, age_group_years_start) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud = weighted.mean(upper_ci_hiv_sud, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t2)) {
  if (col == "acause_lvl2" | col == "age_group_years_start") {
    next
  } else {
    df_tpe_t2[[col]] <- dollar(df_tpe_t2[[col]])
  }
}

# Create mean + CI columns
df_tpe_t2 <- df_tpe_t2 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_hiv_sud, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", 
                     "upper_ci_hiv_sud", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud")

df_tpe_t2_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI")

df_tpe_t2 <- df_tpe_t2 %>%
  select(-c(cols_to_exclude)) %>%
  pivot_wider(
    names_from = age_group_years_start,
    values_from = all_of(df_tpe_t2_value_cols)
  )

# Define desired TOC and condition order
age_order <- c("65", "70", "75", "80", "85")
condition_order <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI")

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t2 <- unlist(
  lapply(age_order, function(age)
  {paste0(condition_order, "_", age)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t2 <- df_tpe_t2 %>%
  select(acause_lvl2, all_of(ordered_cols_tpe_t2))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t2))

colnames(df_tpe_t2) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t2, file.path(output_tables_dir, "TPE_T2.csv"))


##----------------------------------------------------------------
## 2.7 TPE_T3 - Average spending by race, all toc years ages combined, 3 scenarios
##
## This table uses the Two Part Estimates outputs
##----------------------------------------------------------------

# Set df
df_tpe_t3 <- data_list$`04.Two_Part_Estimates_inflation_adjusted_aggregated`

# Group by summary to get 
df_tpe_t3 <- df_tpe_t3 %>%
  group_by(acause_lvl2, race_cd) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud = weighted.mean(upper_ci_hiv_sud, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t3)) {
  if (col == "acause_lvl2" | col == "race_cd") {
    next
  } else {
    df_tpe_t3[[col]] <- dollar(df_tpe_t3[[col]])
  }
}

# Create mean + CI columns
df_tpe_t3 <- df_tpe_t3 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_hiv_sud, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", 
                     "upper_ci_hiv_sud", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud")

df_tpe_t3_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI")

df_tpe_t3 <- df_tpe_t3 %>%
  select(-c(cols_to_exclude)) %>%
  pivot_wider(
    names_from = race_cd,
    values_from = all_of(df_tpe_t3_value_cols)
  )

# Define desired TOC and condition order
race_order <- c("WHT", "BLCK", "HISP")
condition_order <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI")

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t3 <- unlist(
  lapply(race_order, function(race)
  {paste0(condition_order, "_", race)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t3 <- df_tpe_t3 %>%
  select(acause_lvl2, all_of(ordered_cols_tpe_t3))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t3))

colnames(df_tpe_t3) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t3, file.path(output_tables_dir, "TPE_T3.csv"))





