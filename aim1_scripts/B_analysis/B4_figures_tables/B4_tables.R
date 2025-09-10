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
  date_of_input <- "20250910"
  base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary", date_of_input)
  output_tables_dir <- file.path(base_dir, "07.Tables", date_today)
  # (add your other cluster-specific library loads here)
} else {
  # # Local development define manually
  
  # Mac directory
  # base_dir <- "~/Aim1WD/"
  # input_dir <- file.path(base_dir, "aim1_Input_personal_mac")
  # output_tables_dir <- file.path(base_dir, "aim1_output_personal_mac", "07.Tables/", date_today)

  # Windows directory
  base_dir <- "C:/Users/aches/Desktop/Stuff/Coding/Aim1WD/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary/bested/")
  output_tables_dir <- file.path(base_dir, "07.Tables/", date_today)
  resources_dir <- file.path(base_dir, "resources/")
  if (!dir.exists(resources_dir)) dir.create(resources_dir, recursive = TRUE)
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
    if (grepl("^cause_name_lvl2\\s*$", name)) return("Level 2 Cause")
    
    # Split column name
    parts <- strsplit(name, "_")[[1]]
    suffix <- tail(parts, 1)  # Last part is TOC, age, or race
    base <- paste(head(parts, -1), collapse = "_")  # The metric portion
    
    # Map known metric patterns to user-friendly labels
    label_map <- list(
      mean_cost_CI = "Mean Cost (CI)",
      mean_HIV_delta_cost_CI = "HIV Delta (CI)",
      mean_SUD_delta_cost_CI = "SUD Delta (CI)",
      mean_HIV_SUD_delta_cost_CI = "HIV + SUD Delta (CI)"
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
## 0.2 Load in Cause Map File
##----------------------------------------------------------------

# Load and clean the mapping file
# Detect IHME cluster by checking for /mnt/share/limited_use
if (dir.exists("/mnt/share/limited_use")) {
  fp_cause_map <- "/mnt/share/dex/us_county/maps/causelist_figures.csv"
} else {
  # Mac
  # TBD
  
  # Windows
  fp_cause_map <- file.path(resources_dir, "acause_mapping_table.csv")
}

# Read in cause_map
df_map <- read_csv(fp_cause_map, show_col_types = FALSE) %>%
  select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
  mutate(
    acause_lvl2      = if_else(acause == "hiv", "hiv", acause_lvl2),
    cause_name_lvl2  = if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
    acause_lvl2      = if_else(acause == "std", "std", acause_lvl2),
    cause_name_lvl2  = if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
  ) %>% select(-acause) %>% unique()

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
## Rows: Year
## Columns: Total Unique Bene, Total Unique HIV Bene (count / %), Total Unique SUD Bene (count / %)
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

# Join with df_map (cause map table)
df_ss_t1 <- df_ss_t1 %>%
  left_join(df_map, by = "acause_lvl2") 

# Group by summary to get total counts based on toc
df_ss_t1 <- df_ss_t1 %>%
  group_by(acause_lvl2, cause_name_lvl2, has_hiv, has_sud, has_hepc, toc) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, w = total_unique_bene),
    beneficiary_count = sum(total_unique_bene)
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
  filter(condition %in% c("None", "HIV", "SUD", "HIV + SUD")) %>%
  select(acause_lvl2, cause_name_lvl2, toc_condition, avg_cost_per_bene, beneficiary_count) %>%
  pivot_wider(
    names_from = toc_condition,
    values_from = c(avg_cost_per_bene, beneficiary_count)
  )

# Covert avg_cost_per_bene columns to dollar amounts
for (col in colnames(df_ss_t1)) {
  if (col == "cause_name_lvl2" | col == "acause_lvl2") {
    next
  } 
  else if (str_detect(col, "beneficiary_count") == TRUE) { 
    next
  }
  else {
    df_ss_t1[[col]] <- dollar(df_ss_t1[[col]])
  }
}

# Drop "acause_lvl2" column
df_ss_t1 <- df_ss_t1 %>% select(-c(acause_lvl2))

# Format columns
ss_t1_keep_bin_counts <- T # Set to F if want to omit bin counts from cells

toc_order <- c("AM", "ED", "HH", "IP", "NF", "RX")
group_order <- c("None", "HIV", "SUD", "HIV + SUD")

ss_t1_new_cols <- list()

for (toc in toc_order) {
  for (grp in group_order) {
    avg_col <- paste0("avg_cost_per_bene_", toc, " - ", grp)
    count_col <- paste0("beneficiary_count_", toc, " - ", grp)
    
    # Check if both columns exist
    if (all(c(avg_col, count_col) %in% colnames(df_ss_t1))) {
      # Format dollar values and combine with count
      if (ss_t1_keep_bin_counts) {
        ss_t1_new_cols[[paste0(toc, " - ", grp)]] <- paste0(df_ss_t1[[avg_col]], " (n = ", df_ss_t1[[count_col]], ")")
      } else {
        ss_t1_new_cols[[paste0(toc, " - ", grp)]] <- paste0(df_ss_t1[[avg_col]])
      }
      
    }
  }
}

df_ss_t1 <- as.data.frame(ss_t1_new_cols, check.names = FALSE) %>% bind_cols(df_ss_t1["cause_name_lvl2"])

# Reorder columns
df_ss_t1 <- df_ss_t1 %>% select(cause_name_lvl2, everything())

# Rename acause_lvl2 -> Level 2 Cause
df_ss_t1 <- rename(df_ss_t1, "Level 2 Cause" = "cause_name_lvl2")

# Save and output able as .csv
fwrite(df_ss_t1, file.path(output_tables_dir, "SS_T1.csv"))


##----------------------------------------------------------------
## 2.3 SS_T2 - Average spending by age, all toc combined, 3 scenarios (25 * 16 (5 age groups 3 scenarios))
##
## This table uses the summary statistics
##----------------------------------------------------------------

# Set df
df_ss_t2 <- data_list$`01.Summary_Statistics_inflation_adjusted_aggregated`

# Join with df_map (cause map table)
df_ss_t2 <- df_ss_t2 %>%
  left_join(df_map, by = "acause_lvl2") 

# Group by summary to get total counts based on age_group_years_start
df_ss_t2 <- df_ss_t2 %>%
  group_by(acause_lvl2, cause_name_lvl2, has_hiv, has_sud, has_hepc, age_group_years_start) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, w = total_unique_bene),
    beneficiary_count = sum(total_unique_bene),
    .groups = "drop"
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
  filter(condition %in% c("None", "HIV", "SUD", "HIV + SUD")) %>%
  select(acause_lvl2,cause_name_lvl2, age_condition, avg_cost_per_bene, beneficiary_count) %>%
  pivot_wider(
    names_from = age_condition,
    values_from = c(avg_cost_per_bene, beneficiary_count)
  )

# Covert avg_cost_per_bene columns to dollar amounts
for (col in colnames(df_ss_t2)) {
  if (col == "cause_name_lvl2" | col == "acause_lvl2") {
    next
  } 
  else if (str_detect(col, "beneficiary_count") == TRUE) { 
    next
  }
  else {
    df_ss_t2[[col]] <- dollar(df_ss_t2[[col]])
  }
}

# Drop "acause_lvl2" column
df_ss_t2 <- df_ss_t2 %>% select(-c(acause_lvl2))

# Format columns
ss_t2_keep_bin_counts <- T # Set to F if want to omit bin counts from cells

age_order <- c("65", "70", "75", "80", "85")
group_order <- c("None", "HIV", "SUD", "HIV + SUD")

ss_t2_new_cols <- list()

for (age in age_order) {
  for (grp in group_order) {
    avg_col <- paste0("avg_cost_per_bene_", age, " - ", grp)
    count_col <- paste0("beneficiary_count_", age, " - ", grp)
    
    # Check if both columns exist
    if (all(c(avg_col, count_col) %in% colnames(df_ss_t2))) {
      # Format dollar values and combine with count
      if (ss_t2_keep_bin_counts) {
        ss_t2_new_cols[[paste0(age, " - ", grp)]] <- paste0(df_ss_t2[[avg_col]], " (n = ", df_ss_t2[[count_col]], ")")
      } else {
        ss_t2_new_cols[[paste0(age, " - ", grp)]] <- paste0(df_ss_t2[[avg_col]])
      }
      
    }
  }
}

df_ss_t2 <- as.data.frame(ss_t2_new_cols, check.names = FALSE) %>% bind_cols(df_ss_t2["cause_name_lvl2"])

# Reorder columns
df_ss_t2 <- df_ss_t2 %>% select(cause_name_lvl2, everything())

# Rename acause_lvl2 -> Level 2 Cause
df_ss_t2 <- rename(df_ss_t2, "Level 2 Cause" = "cause_name_lvl2")

# Save and output able as .csv
fwrite(df_ss_t2, file.path(output_tables_dir, "SS_T2.csv"))


##----------------------------------------------------------------
## 2.4 SS_T3 - Average spending by race all toc all ages, 3 scenarios (25 * 10 (3 races, 3 scenarios))
##
## This table uses the summary statistics
##----------------------------------------------------------------

# Set df
df_ss_t3 <- data_list$`01.Summary_Statistics_inflation_adjusted_aggregated`

# Join with df_map to bring in cause names
df_ss_t3 <- df_ss_t3 %>%
  left_join(df_map, by = "acause_lvl2") 

# Group by summary to get total counts based on race
df_ss_t3 <- df_ss_t3 %>%
  group_by(acause_lvl2,cause_name_lvl2, has_hiv, has_sud, has_hepc, race_cd) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, w = total_unique_bene),
    beneficiary_count = sum(total_unique_bene)
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
  filter(condition %in% c("None", "HIV", "SUD", "HIV + SUD")) %>%
  select(acause_lvl2, cause_name_lvl2, race_condition, avg_cost_per_bene, beneficiary_count) %>%
  pivot_wider(
    names_from = race_condition,
    values_from = c(avg_cost_per_bene, beneficiary_count)
  )

# Covert avg_cost_per_bene columns to dollar amounts
for (col in colnames(df_ss_t3)) {
  if (col == "cause_name_lvl2" | col == "acause_lvl2") {
    next
  } 
  else if (str_detect(col, "beneficiary_count") == TRUE) { 
    next
  }
  else {
    df_ss_t3[[col]] <- dollar(df_ss_t3[[col]])
  }
}

# Drop "acause_lvl2" column
df_ss_t3 <- df_ss_t3 %>% select(-c(acause_lvl2))

# Format columns
ss_t3_keep_bin_counts <- T # Set to F if want to omit bin counts from cells

race_order <- c("WHT", "BLCK", "HISP")
group_order <- c("None", "HIV", "SUD", "HIV + SUD")

ss_t3_new_cols <- list()

for (race in race_order) {
  for (grp in group_order) {
    avg_col <- paste0("avg_cost_per_bene_", race, " - ", grp)
    count_col <- paste0("beneficiary_count_", race, " - ", grp)
    
    # Check if both columns exist
    if (all(c(avg_col, count_col) %in% colnames(df_ss_t3))) {
      # Format dollar values and combine with count
      if (ss_t3_keep_bin_counts) {
        ss_t3_new_cols[[paste0(race, " - ", grp)]] <- paste0(df_ss_t3[[avg_col]], " (n = ", df_ss_t3[[count_col]], ")")
      } else {
        ss_t3_new_cols[[paste0(race, " - ", grp)]] <- paste0(df_ss_t3[[avg_col]])
      }
      
    }
  }
}

df_ss_t3 <- as.data.frame(ss_t3_new_cols, check.names = FALSE) %>% bind_cols(df_ss_t3["cause_name_lvl2"])

# Reorder columns
df_ss_t3 <- df_ss_t3 %>% select(cause_name_lvl2, everything())

# Rename acause_lvl2 -> Level 2 Cause
df_ss_t3 <- rename(df_ss_t3, "Level 2 Cause" = "cause_name_lvl2")

# Save and output able as .csv
fwrite(df_ss_t3, file.path(output_tables_dir, "SS_T3.csv"))





##----------------------------------------------------------------
## TPE_T1 — Average spending by cause (all ages, races, years combined)
## Columns: Mean Cost (CI), HIV Delta (CI), SUD Delta (CI), HIV + SUD Delta (CI)
## Source: 04.By_cause_inflation_adjusted_aggregated_unfiltered
##----------------------------------------------------------------

# Set df
df_tpe_t1 <- data_list$`04.By_cause_inflation_adjusted_aggregated_unfiltered`

# Aggregate across race/age/TOC/year, keeping disease conditions in rows
df_tpe_t1 <- df_tpe_t1 %>%
  group_by(cause_name_lvl2) %>%
  summarise(
    mean_cost              = weighted.mean(mean_cost,               w = total_row_count, na.rm = TRUE),
    lower_ci               = weighted.mean(lower_ci,                w = total_row_count, na.rm = TRUE),
    upper_ci               = weighted.mean(upper_ci,                w = total_row_count, na.rm = TRUE),
    
    mean_delta_hiv         = weighted.mean(mean_delta_hiv_only,     w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv     = weighted.mean(lower_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv     = weighted.mean(upper_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    
    mean_delta_sud         = weighted.mean(mean_delta_sud_only,     w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud     = weighted.mean(lower_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud     = weighted.mean(upper_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    
    mean_delta_hiv_sud     = weighted.mean(mean_delta_hiv_sud,      w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv_sud = weighted.mean(lower_ci_delta_hiv_sud,  w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv_sud = weighted.mean(upper_ci_delta_hiv_sud,  w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Build display columns (dollar-formatted value with CI)
df_tpe_t1 <- df_tpe_t1 %>%
  mutate(
    `Mean Cost (CI)`        = paste0(dollar(mean_cost),          " (", dollar(lower_ci),               " - ", dollar(upper_ci),               ")"),
    `HIV Delta (CI)`        = paste0(dollar(mean_delta_hiv),     " (", dollar(lower_ci_delta_hiv),     " - ", dollar(upper_ci_delta_hiv),     ")"),
    `SUD Delta (CI)`        = paste0(dollar(mean_delta_sud),     " (", dollar(lower_ci_delta_sud),     " - ", dollar(upper_ci_delta_sud),     ")"),
    `HIV + SUD Delta (CI)`  = paste0(dollar(mean_delta_hiv_sud), " (", dollar(lower_ci_delta_hiv_sud), " - ", dollar(upper_ci_delta_hiv_sud), ")")
  ) %>%
  select(
    cause_name_lvl2,
    `Mean Cost (CI)`,
    `HIV Delta (CI)`,
    `SUD Delta (CI)`,
    `HIV + SUD Delta (CI)`
  ) %>%
  rename(`Level 2 Cause` = cause_name_lvl2)

# Save
fwrite(df_tpe_t1, file.path(output_tables_dir, "TPE_T1.csv"))




##----------------------------------------------------------------
## 2.6 TPE_T2 - Average spending by age, all toc all years combined, 3 scenarios
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t2 <- data_list$`04.By_cause_inflation_adjusted_aggregated_unfiltered`

# Group by summary to get 
df_tpe_t2 <- df_tpe_t2 %>%
  group_by(cause_name_lvl2, age_group_years_start) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv = weighted.mean(upper_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv_sud = weighted.mean(mean_delta_hiv_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv_sud = weighted.mean(lower_ci_delta_hiv_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv_sud = weighted.mean(upper_ci_delta_hiv_sud, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t2)) {
  if (col == "cause_name_lvl2" | col == "age_group_years_start") {
    next
  } else {
    df_tpe_t2[[col]] <- dollar(df_tpe_t2[[col]])
  }
}

# Create mean + CI columns
df_tpe_t2 <- df_tpe_t2 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_delta_hiv, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")"),
    mean_HIV_SUD_delta_cost_CI = paste0(mean_delta_hiv_sud, " (", lower_ci_delta_hiv_sud, " - ", upper_ci_delta_hiv_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", 
                     "upper_ci_delta_hiv", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud",
                     "mean_delta_hiv_sud", "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud")

df_tpe_t2_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI", "mean_HIV_SUD_delta_cost_CI")

df_tpe_t2 <- df_tpe_t2 %>%
  select(-c(all_of(cols_to_exclude))) %>%
  pivot_wider(
    names_from = age_group_years_start,
    values_from = all_of(df_tpe_t2_value_cols)
  )

# Define desired TOC and condition order
age_order <- c("65", "70", "75", "80", "85")
condition_order <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI", "mean_HIV_SUD_delta_cost_CI")

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t2 <- unlist(
  lapply(age_order, function(age)
  {paste0(condition_order, "_", age)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t2 <- df_tpe_t2 %>%
  select(cause_name_lvl2, all_of(ordered_cols_tpe_t2))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t2))

colnames(df_tpe_t2) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t2, file.path(output_tables_dir, "TPE_T2_by_age.csv"))


##----------------------------------------------------------------
## 2.7 TPE_T3 - Average spending by race, all toc years ages combined, 3 scenarios
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t3 <- data_list$`04.By_cause_inflation_adjusted_aggregated_unfiltered`

# Group by summary to get 
df_tpe_t3 <- df_tpe_t3 %>%
  group_by(cause_name_lvl2, race_cd) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv = weighted.mean(upper_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv_sud = weighted.mean(mean_delta_hiv_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv_sud = weighted.mean(lower_ci_delta_hiv_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv_sud = weighted.mean(upper_ci_delta_hiv_sud, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t3)) {
  if (col == "cause_name_lvl2" | col == "race_cd") {
    next
  } else {
    df_tpe_t3[[col]] <- dollar(df_tpe_t3[[col]])
  }
}

# Create mean + CI columns
df_tpe_t3 <- df_tpe_t3 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_delta_hiv, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")"),
    mean_HIV_SUD_delta_cost_CI = paste0(mean_delta_hiv_sud, " (", lower_ci_delta_hiv_sud, " - ", upper_ci_delta_hiv_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", 
                     "upper_ci_delta_hiv", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud",
                     "mean_delta_hiv_sud", "lower_ci_delta_hiv_sud", "upper_ci_delta_hiv_sud")

df_tpe_t3_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI", "mean_HIV_SUD_delta_cost_CI")

df_tpe_t3 <- df_tpe_t3 %>%
  select(-c(cols_to_exclude)) %>%
  pivot_wider(
    names_from = race_cd,
    values_from = all_of(df_tpe_t3_value_cols)
  )

# Define desired TOC and condition order
race_order <- c("WHT", "BLCK", "HISP")
condition_order <- c("mean_cost_CI", "mean_HIV_delta_cost_CI", "mean_SUD_delta_cost_CI", "mean_HIV_SUD_delta_cost_CI")

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t3 <- unlist(
  lapply(race_order, function(race)
  {paste0(condition_order, "_", race)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t3 <- df_tpe_t3 %>%
  select(cause_name_lvl2, all_of(ordered_cols_tpe_t3))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t3))

colnames(df_tpe_t3) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t3, file.path(output_tables_dir, "TPE_T3_by_race.csv"))

##----------------------------------------------------------------
## 2.8 TPE_T4 - Average spending by age, all toc all years combined, 3 scenarios - HIV only
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t4 <- data_list$`05.HIV_inflation_adjusted_aggregated`

# Remove rows with bad data (remove later)
df_tpe_t4 <- df_tpe_t4 %>% 
  filter(mean_cost_hiv < 500000 & mean_cost_hiv > 1) %>%
  filter(mean_cost_hiv_sud < 500000 & mean_cost_hiv_sud > 1)

# Group by summary to get 
df_tpe_t4 <- df_tpe_t4 %>%
  group_by(cause_name_lvl2, age_group_years_start) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost_hiv, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_hiv, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud_only, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t4)) {
  if (col == "cause_name_lvl2" | col == "age_group_years_start") {
    next
  } else {
    df_tpe_t4[[col]] <- dollar(df_tpe_t4[[col]])
  }
}

# Create mean + CI columns
df_tpe_t4 <- df_tpe_t4 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud")

df_tpe_t4_value_cols <- c("mean_cost_CI", "mean_SUD_delta_cost_CI")

df_tpe_t4 <- df_tpe_t4 %>%
  select(-c(all_of(cols_to_exclude))) %>%
  pivot_wider(
    names_from = age_group_years_start,
    values_from = all_of(df_tpe_t4_value_cols)
  )

# Define desired TOC and condition order
age_order <- c("65", "70", "75", "80", "85")
condition_order <- df_tpe_t4_value_cols

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t4 <- unlist(
  lapply(age_order, function(age)
  {paste0(condition_order, "_", age)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t4 <- df_tpe_t4 %>%
  select(cause_name_lvl2, all_of(ordered_cols_tpe_t4))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t4))

colnames(df_tpe_t4) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t4, file.path(output_tables_dir, "TPE_T4_hiv_by_age.csv"))


##----------------------------------------------------------------
## 2.9 TPE_T5 - Average spending by race, all toc years ages combined, 3 scenarios - HIV only
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t5 <- data_list$`05.HIV_inflation_adjusted_aggregated`

# Remove rows with bad data (remove later)
df_tpe_t5 <- df_tpe_t5 %>% 
  filter(mean_cost_hiv < 500000 & mean_cost_hiv > 1) %>%
  filter(mean_cost_hiv_sud < 500000 & mean_cost_hiv_sud > 1)

# Group by summary to get 
df_tpe_t5 <- df_tpe_t5 %>%
  group_by(cause_name_lvl2, race_cd) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost_hiv, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_hiv, w = total_row_count, na.rm = TRUE),
    mean_delta_sud = weighted.mean(mean_delta_sud_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_sud = weighted.mean(lower_ci_delta_sud_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_sud = weighted.mean(upper_ci_delta_sud_only, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t5)) {
  if (col == "cause_name_lvl2" | col == "race_cd") {
    next
  } else {
    df_tpe_t5[[col]] <- dollar(df_tpe_t5[[col]])
  }
}

# Create mean + CI columns
df_tpe_t5 <- df_tpe_t5 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_SUD_delta_cost_CI = paste0(mean_delta_sud, " (", lower_ci_delta_sud, " - ", upper_ci_delta_sud, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_sud", "lower_ci_delta_sud", "upper_ci_delta_sud")

df_tpe_t5_value_cols <- c("mean_cost_CI", "mean_SUD_delta_cost_CI")

df_tpe_t5 <- df_tpe_t5 %>%
  select(-c(cols_to_exclude)) %>%
  pivot_wider(
    names_from = race_cd,
    values_from = all_of(df_tpe_t5_value_cols)
  )

# Define desired TOC and condition order
race_order <- c("WHT", "BLCK", "HISP")
condition_order <- df_tpe_t5_value_cols

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t5 <- unlist(
  lapply(race_order, function(race)
  {paste0(condition_order, "_", race)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t5 <- df_tpe_t5 %>%
  select(cause_name_lvl2, all_of(ordered_cols_tpe_t5))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t5))

colnames(df_tpe_t5) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t5, file.path(output_tables_dir, "TPE_T5_hiv_by_race.csv"))

##----------------------------------------------------------------
## 2.10 TPE_T6 - Average spending by age, all toc all years combined, 3 scenarios - SUD only
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t6 <- data_list$`05.SUD_inflation_adjusted_aggregated`

# Group by summary
df_tpe_t6 <- df_tpe_t6 %>%
  group_by(cause_name_lvl2, age_group_years_start) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost_sud, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_sud, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_sud, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv = weighted.mean(upper_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t6)) {
  if (col == "cause_name_lvl2" | col == "age_group_years_start") {
    next
  } else {
    df_tpe_t6[[col]] <- dollar(df_tpe_t6[[col]])
  }
}

# Create mean + CI columns
df_tpe_t6 <- df_tpe_t6 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_delta_hiv, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", "upper_ci_delta_hiv")

df_tpe_t6_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI")

df_tpe_t6 <- df_tpe_t6 %>%
  select(-c(all_of(cols_to_exclude))) %>%
  pivot_wider(
    names_from = age_group_years_start,
    values_from = all_of(df_tpe_t6_value_cols)
  )

# Define desired TOC and condition order
age_order <- c("65", "70", "75", "80", "85")
condition_order <- df_tpe_t6_value_cols

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t6 <- unlist(
  lapply(age_order, function(age)
  {paste0(condition_order, "_", age)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t6 <- df_tpe_t6 %>%
  select(cause_name_lvl2, all_of(ordered_cols_tpe_t6))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t6))

colnames(df_tpe_t6) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t6, file.path(output_tables_dir, "TPE_T6_sud_by_age.csv"))


##----------------------------------------------------------------
## 2.11 TPE_T7 - Average spending by race, all toc years ages combined, 3 scenarios - SUD only
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t7 <- data_list$`05.SUD_inflation_adjusted_aggregated`

# Group by summary to get 
df_tpe_t7 <- df_tpe_t7 %>%
  group_by(cause_name_lvl2, race_cd) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost_sud, w = total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_sud, w = total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_sud, w = total_row_count, na.rm = TRUE),
    mean_delta_hiv = weighted.mean(mean_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    lower_ci_delta_hiv = weighted.mean(lower_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    upper_ci_delta_hiv = weighted.mean(upper_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE)
  )

# Covert to dollar amounts
for (col in colnames(df_tpe_t7)) {
  if (col == "cause_name_lvl2" | col == "race_cd") {
    next
  } else {
    df_tpe_t7[[col]] <- dollar(df_tpe_t7[[col]])
  }
}

# Create mean + CI columns
df_tpe_t7 <- df_tpe_t7 %>%
  mutate(
    mean_cost_CI = paste0(mean_cost, " (", lower_ci, " - ", upper_ci, ")"),
    mean_HIV_delta_cost_CI = paste0(mean_delta_hiv, " (", lower_ci_delta_hiv, " - ", upper_ci_delta_hiv, ")")
  )

# Pivot table wider based on toc
cols_to_exclude <- c("mean_cost", "lower_ci", "upper_ci", "mean_delta_hiv", "lower_ci_delta_hiv", "upper_ci_delta_hiv")

df_tpe_t7_value_cols <- c("mean_cost_CI", "mean_HIV_delta_cost_CI")

df_tpe_t7 <- df_tpe_t7 %>%
  select(-c(cols_to_exclude)) %>%
  pivot_wider(
    names_from = race_cd,
    values_from = all_of(df_tpe_t7_value_cols)
  )

# Define desired TOC and condition order
race_order <- c("WHT", "BLCK", "HISP")
condition_order <- df_tpe_t7_value_cols

# Build column names in desired order: TOC first, then condition
ordered_cols_tpe_t7 <- unlist(
  lapply(race_order, function(race)
  {paste0(condition_order, "_", race)}
  )
)

# Reorder columns with acause_lvl2 first
df_tpe_t7 <- df_tpe_t7 %>%
  select(cause_name_lvl2, all_of(ordered_cols_tpe_t7))

# Rename column names
converted_names <- convert_colnames_general(colnames(df_tpe_t7))

colnames(df_tpe_t7) <- converted_names

# Save and output able as .csv
fwrite(df_tpe_t7, file.path(output_tables_dir, "TPE_T7_sud_by_race.csv"))


##----------------------------------------------------------------
## 2.12 TPE_T8 - HIV / SUD all races all age groups combined, by year table
##
## This table uses the By-cause two part estimates
##----------------------------------------------------------------

# Set df
df_tpe_t8_hiv <- data_list$`05.HIV_subtable_by_year` %>%
  select(c("cause_name_lvl2", 
           "year_id", "mean_cost_hiv", "lower_ci_hiv", "upper_ci_hiv", "mean_cost_hiv_sud", 
           "lower_ci_hiv_sud", "upper_ci_hiv_sud"))
df_tpe_t8_sud <- data_list$`05.SUD_subtable_by_year` %>%
  select(c("cause_name_lvl2", 
           "year_id", "mean_cost_sud", "lower_ci_sud", "upper_ci_sud", "mean_cost_hiv_sud", 
           "lower_ci_hiv_sud", "upper_ci_hiv_sud"))

# Covert to dollar amounts
for (col in colnames(df_tpe_t8_hiv)) {
  if (col == "cause_name_lvl2" | col == "year_id" | col == "total_bin_count") {
    next
  } else {
    df_tpe_t8_hiv[[col]] <- dollar(df_tpe_t8_hiv[[col]])
  }
}
for (col in colnames(df_tpe_t8_sud)) {
  if (col == "cause_name_lvl2" | col == "year_id" | col == "total_bin_count") {
    next
  } else {
    df_tpe_t8_sud[[col]] <- dollar(df_tpe_t8_sud[[col]])
  }
}

# Create mean + CI columns
df_tpe_t8_hiv <- df_tpe_t8_hiv %>%
  mutate(
    `HIV - Mean Cost CI` = paste0(mean_cost_hiv, " (", lower_ci_hiv, " - ", upper_ci_hiv, ")"),
    `HIV + SUD Mean Cost CI` = paste0(mean_cost_hiv_sud, " (", lower_ci_hiv_sud, " - ", upper_ci_hiv_sud, ")")
  ) %>%
  select(c("year_id", "HIV - Mean Cost CI", "HIV + SUD Mean Cost CI")) %>% 
  setnames(old = c("year_id"),
           new = c("Year"))

df_tpe_t8_sud <- df_tpe_t8_sud %>%
  mutate(
    `SUD - Mean Cost CI` = paste0(mean_cost_sud, " (", lower_ci_sud, " - ", upper_ci_sud, ")"),
    `SUD + HIV Mean Cost CI` = paste0(mean_cost_hiv_sud, " (", lower_ci_hiv_sud, " - ", upper_ci_hiv_sud, ")")
  ) %>%
  select(c("year_id", "SUD - Mean Cost CI", "SUD + HIV Mean Cost CI")) %>% 
  setnames(old = c("year_id"),
           new = c("Year"))

# Combine dfs - Join on "Year" 
df_tpe_t8 <- full_join(x = df_tpe_t8_hiv, y = df_tpe_t8_sud, by = c("Year"))

# Save and output able as .csv
fwrite(df_tpe_t8, file.path(output_tables_dir, "TPE_T8.csv"))

##----------------------------------------------------------------
## 2.13 TPE_T9 
#   All years, all toc, all races, all age groups
# Rows: 25 diseases (including HIV and SUD)
# Columns:
#   Mean Cost Summary (CI - no CI for ss), Mean Cost Modeled (CI), Delta, 
# HIV Cost Summary (CI), HIV Cost Modeled (CI), Delta
# same thing for SUD, same thing for HIV + SUD
##
## This table uses the By-cause two part estimates and Summary stats
##----------------------------------------------------------------


# Create summary stats data
df_tpe9_ss <- data_list$`01.Summary_Statistics_inflation_adjusted_aggregated`

df_tpe9_ss <- df_tpe9_ss %>%
  group_by(acause_lvl2, has_hiv, has_sud) %>%
  summarise(
    mean_cost = weighted.mean(avg_cost_per_bene_winsorized, w = total_unique_bene, na.rm = TRUE) # comparing with winsorized mean from SS output
  )

df_tpe9_ss <- left_join(x = df_tpe9_ss, y = df_map, by = "acause_lvl2") %>%
  ungroup() %>%
  select(-c("acause_lvl2", "acause_lvl1", "cause_name_lvl1"))

df_tpe9_ss <- df_tpe9_ss %>%
  mutate(combo = paste0(has_hiv, has_sud)) %>%   # e.g. 00, 01, 10, 11
  pivot_wider(
    id_cols = cause_name_lvl2,                   # what stays as identifier
    names_from = combo,                          # new column names from combos
    values_from = mean_cost                      # values to spread
  ) %>%
  setnames(old = c("cause_name_lvl2","00", "01", "10", "11"),
           new = c("Level 2 Cause","Mean Cost Summary", "SUD Cost Summary", "HIV Cost Summary", "HIV + SUD Cost Summary"))


# Create TPE data

# By-cause (w/o HIV SUD) df
# Set df
df_tpe9_tpe <- data_list$`04.By_cause_inflation_adjusted_aggregated_unfiltered`

# Aggregate across race/age/TOC/year, keeping disease conditions in rows
df_tpe9_tpe <- df_tpe9_tpe %>%
  group_by(cause_name_lvl2) %>%
  summarise(
    mean_cost              = weighted.mean(mean_cost,               w = total_row_count, na.rm = TRUE),
    lower_ci               = weighted.mean(lower_ci,                w = total_row_count, na.rm = TRUE),
    upper_ci               = weighted.mean(upper_ci,                w = total_row_count, na.rm = TRUE),
    
    mean_cost_hiv         = weighted.mean(mean_cost_hiv,     w = total_row_count, na.rm = TRUE),
    lower_ci_hiv     = weighted.mean(lower_ci_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv     = weighted.mean(upper_ci_hiv, w = total_row_count, na.rm = TRUE),
    
    mean_cost_sud         = weighted.mean(mean_cost_sud,     w = total_row_count, na.rm = TRUE),
    lower_ci_sud     = weighted.mean(lower_ci_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_sud     = weighted.mean(upper_ci_sud, w = total_row_count, na.rm = TRUE),
    
    mean_cost_hiv_sud     = weighted.mean(mean_cost_hiv_sud,      w = total_row_count, na.rm = TRUE),
    lower_ci_hiv_sud = weighted.mean(lower_ci_hiv_sud,  w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud = weighted.mean(upper_ci_hiv_sud,  w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# By-cause (w/ HIV SUD) df

# SUD
# Set df
df_tpe9_tpe_sud <- data_list$`05.SUD_inflation_adjusted_aggregated`

# Group by summary to get 
df_tpe9_tpe_sud <- df_tpe9_tpe_sud %>%
  group_by(cause_name_lvl2) %>%
  summarise(
    mean_cost_sud = weighted.mean(mean_cost_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_sud = weighted.mean(lower_ci_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_sud = weighted.mean(upper_ci_sud, w = total_row_count, na.rm = TRUE),
    
    mean_cost_hiv_sud = weighted.mean(mean_cost_hiv_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_hiv_sud = weighted.mean(lower_ci_hiv_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud = weighted.mean(upper_ci_hiv_sud, w = total_row_count, na.rm = TRUE)
  )

# HIV
# Set df
df_tpe9_tpe_hiv <- data_list$`05.HIV_inflation_adjusted_aggregated`

# Group by summary to get 
df_tpe9_tpe_hiv <- df_tpe9_tpe_hiv %>%
  group_by(cause_name_lvl2) %>%
  summarise(
    mean_cost_hiv = weighted.mean(mean_cost_hiv, w = total_row_count, na.rm = TRUE),
    lower_ci_hiv = weighted.mean(lower_ci_hiv, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv = weighted.mean(upper_ci_hiv, w = total_row_count, na.rm = TRUE),
    
    mean_cost_hiv_sud = weighted.mean(mean_cost_hiv_sud, w = total_row_count, na.rm = TRUE),
    lower_ci_hiv_sud = weighted.mean(lower_ci_hiv_sud, w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud = weighted.mean(upper_ci_hiv_sud, w = total_row_count, na.rm = TRUE)
  )


# Row bind the three by-cause dfs
df_tpe9 <- bind_rows(df_tpe9_tpe, df_tpe9_tpe_sud, df_tpe9_tpe_hiv)

# Build display columns (dollar-formatted value with CI)
df_tpe9 <- df_tpe9 %>%
  mutate(
    `Mean Cost Modeled (CI)` = paste0(dollar(mean_cost),          " (", dollar(lower_ci),               " - ", dollar(upper_ci),               ")"),
    `HIV Cost Modeled (CI)`  = paste0(dollar(mean_cost_hiv),     " (", dollar(lower_ci_hiv),     " - ", dollar(upper_ci_hiv),     ")"),
    `SUD Cost Modeled (CI)`        = paste0(dollar(mean_cost_sud),     " (", dollar(lower_ci_sud),     " - ", dollar(upper_ci_sud),     ")"),
    `HIV + SUD Cost Modeled (CI)`  = paste0(dollar(mean_cost_hiv_sud), " (", dollar(lower_ci_hiv_sud), " - ", dollar(upper_ci_hiv_sud), ")")
  ) 

# column bind the SS data
df_tpe9_join <- left_join(x = df_tpe9, y = df_tpe9_ss, by = c("cause_name_lvl2" = "Level 2 Cause"))

# Calculate deltas between summary and modeled 
df_tpe9_join <- df_tpe9_join %>%
  mutate(
    `Mean Cost Delta` = `Mean Cost Summary` - `mean_cost`,
    `HIV Cost Delta` = `HIV Cost Summary` - `mean_cost_hiv`,
    `SUD Cost Delta` = `SUD Cost Summary` - `mean_cost_sud`,
    `HIV + SUD Cost Delta` = `HIV + SUD Cost Summary` - `mean_cost_hiv_sud`
  ) 

# Convert columns to dollars
cols_to_convert <- c("mean_cost", "lower_ci", "upper_ci", "mean_cost_hiv", 
                     "lower_ci_hiv", "upper_ci_hiv", "mean_cost_sud", "lower_ci_sud", 
                     "upper_ci_sud", "mean_cost_hiv_sud", "lower_ci_hiv_sud", "upper_ci_hiv_sud", 
                      "Mean Cost Summary", "SUD Cost Summary", 
                     "HIV Cost Summary", "HIV + SUD Cost Summary", "Mean Cost Delta", 
                     "HIV Cost Delta", "SUD Cost Delta", "HIV + SUD Cost Delta")

cols_to_omit <- c("cause_name_lvl2", "Mean Cost Modeled (CI)", "HIV Cost Modeled (CI)", "SUD Cost Modeled (CI)", 
                  "HIV + SUD Cost Modeled (CI)")

# Covert to dollar amounts
for (col in cols_to_convert) {
  df_tpe9_join[[col]] <- dollar(df_tpe9_join[[col]])
}

# Arrange columns in final desired order + drop unnecessary columns
df_tpe9_col_order <- c("cause_name_lvl2", 
                       "Mean Cost Summary", "Mean Cost Modeled (CI)",  "Mean Cost Delta", 
                       "HIV Cost Summary", "HIV Cost Modeled (CI)",  "HIV Cost Delta",
                       "SUD Cost Summary", "SUD Cost Modeled (CI)", "SUD Cost Delta",
                       "HIV + SUD Cost Summary", "HIV + SUD Cost Modeled (CI)",  "HIV + SUD Cost Delta")

df_tpe9_final <- df_tpe9_join %>% select(all_of(df_tpe9_col_order))
                      
df_tpe9_final <- df_tpe9_final %>% setnames(old = "cause_name_lvl2", new = "Level 2 Cause")               

# Manually set NA where NA values should belong
df_tpe9_final$`Mean Cost Modeled (CI)`[df_tpe9_final$`Level 2 Cause` == "HIV/AIDS"] <- NA
df_tpe9_final$`Mean Cost Modeled (CI)`[df_tpe9_final$`Level 2 Cause` == "Substance use disorders"] <- NA
df_tpe9_final$`SUD Cost Modeled (CI)`[df_tpe9_final$`Level 2 Cause` == "HIV/AIDS"] <- NA
df_tpe9_final$`HIV Cost Modeled (CI)`[df_tpe9_final$`Level 2 Cause` == "Substance use disorders"] <- NA

# Save and output able as .csv
fwrite(df_tpe9_final, file.path(output_tables_dir, "TPE_T9.csv"))









