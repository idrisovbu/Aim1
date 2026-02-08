#########################################################################################
# Title: B4_manuscript_tables.R
# Description: Generates manuscript-ready tables for main paper and appendix
#              Tables follow Emily's formatting advice (separate CI columns)
#              Names correspond directly to manuscript table numbers
#
# Inputs: 05.Aggregation_Summary/<date>/*.csv
# Outputs: 08.Manuscript_Tables/<date>/*.csv
#
# Main Paper Tables:
#   - Table1_Population_Characteristics.csv
#   - Table2_HIV_SUD_Primary_Costs_by_Year.csv  
#   - Table3_Incremental_Costs_by_Condition.csv
#
# Appendix Tables:
#   - AppTable1_Population_Characteristics_by_Race.csv
#   - AppTable2_Winsorization_by_Condition.csv
#
#########################################################################################

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
  date_of_input <- "20260107"
  
  # Whether the data has counterfactual all 0s, or the has_* variables have all 1's
  counterfactual_0 <- TRUE
  counterfactual_string <- ifelse(counterfactual_0, "has_0", "has_1")
  
  base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary", date_of_input, counterfactual_string)
  output_dir <- file.path(base_dir, "08.Manuscript_Tables", date_today, counterfactual_string)
  fp_cause_map <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/A_data_preparation/Cause_list/acause_mapping_table.csv"
  
} else {
  # Local development (Windows)
  base_dir <- "C:/Users/aches/Desktop/Stuff/Coding/Aim1WD/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary/bested/")
  output_dir <- file.path(base_dir, "08.Manuscript_Tables/", date_today)
  resources_dir <- file.path(base_dir, "resources/")
  fp_cause_map <- file.path(resources_dir, "acause_mapping_table.csv")
  
  if (!dir.exists(resources_dir)) dir.create(resources_dir, recursive = TRUE)
}

# Create output directory
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("==================================================================\n")
cat("B5_manuscript_tables.R\n")
cat("==================================================================\n")
cat("Input directory:", input_dir, "\n")
cat("Output directory:", output_dir, "\n")
cat("==================================================================\n\n")

##----------------------------------------------------------------
## 0.1 Helper Functions
##----------------------------------------------------------------

# Format count and percentage columns
format_count_percent <- function(count_col, percent_col) {
  paste0(format(count_col, big.mark = ","), " (", percent(percent_col, accuracy = 0.01), ")")
}

# Weighted median function
weighted_median <- function(x, w) {
  if (all(is.na(x)) || all(is.na(w))) return(NA_real_)
  valid <- !is.na(x) & !is.na(w) & w > 0
  x <- x[valid]
  w <- w[valid]
  if (length(x) == 0) return(NA_real_)
  ord <- order(x)
  x <- x[ord]
  w <- w[ord]
  cum_w <- cumsum(w) / sum(w)
  idx <- which(cum_w >= 0.5)[1]
  return(x[idx])
}

##----------------------------------------------------------------
## 0.2 Load Cause Map
##----------------------------------------------------------------
df_map <- read_csv(fp_cause_map, show_col_types = FALSE) %>%
  select(acause, acause_lvl2, cause_name_lvl2, acause_lvl1, cause_name_lvl1) %>%
  mutate(
    acause_lvl2 = if_else(acause == "hiv", "hiv", acause_lvl2),
    cause_name_lvl2 = if_else(acause == "hiv", "HIV/AIDS", cause_name_lvl2),
    acause_lvl2 = if_else(acause == "std", "std", acause_lvl2),
    cause_name_lvl2 = if_else(acause == "std", "Sexually transmitted infections", cause_name_lvl2)
  ) %>% 
  select(-acause) %>% 
  unique()

##----------------------------------------------------------------
## 0.3 Load All Data Files
##----------------------------------------------------------------
file_list <- list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)
data_list <- list()

for (file in file_list) {
  file_name <- tools::file_path_sans_ext(basename(file))
  data_list[[file_name]] <- read.csv(file, stringsAsFactors = FALSE)
}

cat("Loaded", length(data_list), "data files\n\n")


##########################################################################
##########################################################################
##
## MAIN PAPER TABLES
##
##########################################################################
##########################################################################

##########################################################################
# TABLE 1: Population Characteristics by Year
##########################################################################
#
# Shows annual Medicare FFS beneficiary counts and prevalence of:
#   - HIV
#   - SUD  
#   - HIV + SUD (co-occurring)
#
# Rows: Years (2010, 2014, 2015, 2016, 2019)
# Note: HEPC columns removed per manuscript decision
#
##########################################################################

cat("Creating Table 1: Population Characteristics by Year...\n")

df_table1 <- data_list[["03.Meta_Statistics_aggregated"]]

# Clean and convert total_unique_bene
df_table1$total_unique_bene <- as.integer(str_remove_all(df_table1$total_unique_bene, ","))

# Aggregate by year
df_table1 <- df_table1 %>%
  group_by(year_id) %>%
  dplyr::summarise(
    total_unique_bene_sum = sum(total_unique_bene, na.rm = TRUE),
    total_unique_hiv_bene = sum(hiv_unique_bene, na.rm = TRUE),
    total_unique_sud_bene = sum(sud_unique_bene, na.rm = TRUE),
    total_unique_hiv_sud_bene = sum(hiv_and_sud_unique_bene, na.rm = TRUE),
    .groups = "drop"
  )

# Calculate percentages
df_table1 <- df_table1 %>%
  mutate(
    hiv_pct = total_unique_hiv_bene / total_unique_bene_sum,
    sud_pct = total_unique_sud_bene / total_unique_bene_sum,
    hiv_sud_pct = total_unique_hiv_sud_bene / total_unique_bene_sum
  )

# Format final table
Table1 <- df_table1 %>%
  transmute(
    Year = year_id,
    `Total Beneficiaries` = format(total_unique_bene_sum, big.mark = ","),
    `HIV Beneficiaries (n, %)` = format_count_percent(total_unique_hiv_bene, hiv_pct),
    `SUD Beneficiaries (n, %)` = format_count_percent(total_unique_sud_bene, sud_pct),
    `HIV + SUD Beneficiaries (n, %)` = format_count_percent(total_unique_hiv_sud_bene, hiv_sud_pct)
  )

# Save
fwrite(Table1, file.path(output_dir, "Table1_Population_Characteristics.csv"))
cat("  Created: Table1_Population_Characteristics.csv\n\n")


##########################################################################
# TABLE 2: HIV and SUD as Primary Conditions - Costs and Deltas by Year
##########################################################################
#
# Shows:
#   - Baseline predicted cost when HIV is primary (no SUD)
#   - Incremental cost (delta) of adding SUD to HIV
#   - Baseline predicted cost when SUD is primary (no HIV)
#   - Incremental cost (delta) of adding HIV to SUD
#
# All estimates from g-computation with other comorbidities set to zero.
# Confidence intervals in separate columns per Emily's advice.
#
##########################################################################

cat("Creating Table 2: HIV and SUD Primary Costs by Year...\n")

# Read aggregated HIV and SUD files
df_hiv_agg <- read_csv(file.path(input_dir, "05.HIV_inflation_adjusted_aggregated.csv"),
                       show_col_types = FALSE)
df_sud_agg <- read_csv(file.path(input_dir, "05.SUD_inflation_adjusted_aggregated.csv"),
                       show_col_types = FALSE)

# Aggregate HIV data by year
df_hiv_by_year <- df_hiv_agg %>%
  group_by(year_id) %>%
  dplyr::summarise(
    hiv_baseline_mean = weighted.mean(mean_cost_hiv, total_row_count, na.rm = TRUE),
    hiv_baseline_lower = weighted.mean(lower_ci_hiv, total_row_count, na.rm = TRUE),
    hiv_baseline_upper = weighted.mean(upper_ci_hiv, total_row_count, na.rm = TRUE),
    sud_delta_mean = weighted.mean(mean_delta_sud_only, total_row_count, na.rm = TRUE),
    sud_delta_lower = weighted.mean(lower_ci_delta_sud_only, total_row_count, na.rm = TRUE),
    sud_delta_upper = weighted.mean(upper_ci_delta_sud_only, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Aggregate SUD data by year
df_sud_by_year <- df_sud_agg %>%
  group_by(year_id) %>%
  dplyr::summarise(
    sud_baseline_mean = weighted.mean(mean_cost_sud, total_row_count, na.rm = TRUE),
    sud_baseline_lower = weighted.mean(lower_ci_sud, total_row_count, na.rm = TRUE),
    sud_baseline_upper = weighted.mean(upper_ci_sud, total_row_count, na.rm = TRUE),
    hiv_delta_mean = weighted.mean(mean_delta_hiv_only, total_row_count, na.rm = TRUE),
    hiv_delta_lower = weighted.mean(lower_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    hiv_delta_upper = weighted.mean(upper_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Join HIV and SUD summaries
df_table2_raw <- df_hiv_by_year %>%
  left_join(df_sud_by_year, by = "year_id")

# Format Table 2 with separate CI columns
Table2 <- df_table2_raw %>%
  transmute(
    Year = year_id,
    
    # HIV as primary condition
    `Baseline Predicted Cost (HIV Alone)` = dollar(round(hiv_baseline_mean, 2)),
    `HIV Baseline 95% CI` = sprintf("(%s, %s)", 
                                    dollar(round(hiv_baseline_lower, 2)),
                                    dollar(round(hiv_baseline_upper, 2))),
    
    `Cost Delta when SUD Present` = dollar(round(sud_delta_mean, 2)),
    `SUD Delta 95% CI` = sprintf("(%s, %s)", 
                                 dollar(round(sud_delta_lower, 2)),
                                 dollar(round(sud_delta_upper, 2))),
    
    # SUD as primary condition
    `Baseline Predicted Cost (SUD Alone)` = dollar(round(sud_baseline_mean, 2)),
    `SUD Baseline 95% CI` = sprintf("(%s, %s)", 
                                    dollar(round(sud_baseline_lower, 2)),
                                    dollar(round(sud_baseline_upper, 2))),
    
    `Cost Delta when HIV Present` = dollar(round(hiv_delta_mean, 2)),
    `HIV Delta 95% CI` = sprintf("(%s, %s)", 
                                 dollar(round(hiv_delta_lower, 2)),
                                 dollar(round(hiv_delta_upper, 2)))
  )

# Save
fwrite(Table2, file.path(output_dir, "Table2_HIV_SUD_Primary_Costs_by_Year.csv"))
cat("  Created: Table2_HIV_SUD_Primary_Costs_by_Year.csv\n\n")


##########################################################################
# TABLE 3: Incremental Costs of HIV and SUD Across 23 GBD Conditions
##########################################################################
#
# Shows for each of 23 GBD conditions (j):
#   - Baseline predicted cost for condition j alone (no HIV, no SUD)
#   - Incremental cost (delta) of adding HIV to condition j
#   - Incremental cost (delta) of adding SUD to condition j
#   - Incremental cost (delta) of adding both HIV and SUD to condition j
#
# All estimates from g-computation with other comorbidities set to zero.
# Confidence intervals in separate columns per Emily's advice.
#
##########################################################################

cat("Creating Table 3: Incremental Costs Across Conditions...\n")

# Read the by-cause aggregated file
df_cause_agg <- read_csv(file.path(input_dir, "04.By_cause_subtable_by_cause.csv"),
                         show_col_types = FALSE)

# Format Table 3 with separate CI columns
Table3 <- df_cause_agg %>%
  # Filter out HIV and SUD (they are in Table 2)
  filter(!acause_lvl2 %in% c("hiv", "_subs")) %>%
  transmute(
    Condition = cause_name_lvl2,
    
    # Baseline cost (condition j alone)
    `Baseline Predicted Cost (Condition Alone)` = dollar(round(mean_cost, 2)),
    `Baseline 95% CI` = sprintf("(%s, %s)", 
                                dollar(round(lower_ci, 2)),
                                dollar(round(upper_ci, 2))),
    
    # HIV delta
    `Cost Delta when HIV Present` = dollar(round(mean_delta_hiv_only, 2)),
    `HIV Delta 95% CI` = sprintf("(%s, %s)", 
                                 dollar(round(lower_ci_delta_hiv_only, 2)),
                                 dollar(round(upper_ci_delta_hiv_only, 2))),
    
    # SUD delta
    `Cost Delta when SUD Present` = dollar(round(mean_delta_sud_only, 2)),
    `SUD Delta 95% CI` = sprintf("(%s, %s)", 
                                 dollar(round(lower_ci_delta_sud_only, 2)),
                                 dollar(round(upper_ci_delta_sud_only, 2))),
    
    # HIV + SUD delta
    `Cost Delta when HIV + SUD Present` = dollar(round(mean_delta_hiv_sud, 2)),
    `HIV+SUD Delta 95% CI` = sprintf("(%s, %s)", 
                                     dollar(round(lower_ci_delta_hiv_sud, 2)),
                                     dollar(round(upper_ci_delta_hiv_sud, 2)))
  ) %>%
  # Sort by baseline cost descending
  arrange(desc(parse_number(`Baseline Predicted Cost (Condition Alone)`)))

# Save
fwrite(Table3, file.path(output_dir, "Table3_Incremental_Costs_by_Condition.csv"))
cat("  Created: Table3_Incremental_Costs_by_Condition.csv\n\n")


##########################################################################
##########################################################################
##
## APPENDIX TABLES
##
##########################################################################
##########################################################################

##########################################################################
# APPENDIX TABLE 1: Population Characteristics by Race and Year
##########################################################################
#
# Same as Table 1 but stratified by race/ethnicity
# Rows: Race × Year combinations
# Note: HEPC columns removed per manuscript decision
#
##########################################################################

cat("Creating Appendix Table 1: Population Characteristics by Race...\n")

df_apptable1 <- data_list[["03.Meta_Statistics_aggregated"]]

# Clean and convert
df_apptable1$total_unique_bene <- as.integer(str_remove_all(df_apptable1$total_unique_bene, ","))

# Aggregate by race and year
df_apptable1 <- df_apptable1 %>%
  group_by(race_cd, year_id) %>%
  dplyr::summarise(
    total_unique_bene_sum = sum(total_unique_bene, na.rm = TRUE),
    total_unique_hiv_bene = sum(hiv_unique_bene, na.rm = TRUE),
    total_unique_sud_bene = sum(sud_unique_bene, na.rm = TRUE),
    total_unique_hiv_sud_bene = sum(hiv_and_sud_unique_bene, na.rm = TRUE),
    .groups = "drop"
  )

# Calculate percentages
df_apptable1 <- df_apptable1 %>%
  mutate(
    hiv_pct = total_unique_hiv_bene / total_unique_bene_sum,
    sud_pct = total_unique_sud_bene / total_unique_bene_sum,
    hiv_sud_pct = total_unique_hiv_sud_bene / total_unique_bene_sum
  )

# Format final table
AppTable1 <- df_apptable1 %>%
  transmute(
    Race = race_cd,
    Year = year_id,
    `Total Beneficiaries` = format(total_unique_bene_sum, big.mark = ","),
    `HIV Beneficiaries (n, %)` = format_count_percent(total_unique_hiv_bene, hiv_pct),
    `SUD Beneficiaries (n, %)` = format_count_percent(total_unique_sud_bene, sud_pct),
    `HIV + SUD Beneficiaries (n, %)` = format_count_percent(total_unique_hiv_sud_bene, hiv_sud_pct)
  ) %>%
  arrange(Race, Year)

# Save
fwrite(AppTable1, file.path(output_dir, "AppTable1_Population_Characteristics_by_Race.csv"))
cat("  Created: AppTable1_Population_Characteristics_by_Race.csv\n\n")


##########################################################################
# APPENDIX TABLE 2: Winsorization Comparison by Disease Category
##########################################################################
#
# Distribution of Annual Per-Beneficiary Spending Before and After 
# 99.5th-Percentile Winsorization, by Disease Category
#
# Columns:
#   - Median/Mean/Max of max_cost_per_bene (BEFORE winsorization)
#   - Median/Mean/Max after 99.5th percentile cap (AFTER winsorization)
#
# Rows: 25 diseases (including HIV and SUD)
#
##########################################################################

cat("Creating Appendix Table 2: Winsorization Comparison...\n")

df_apptable2 <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]

# Merge with cause names
df_apptable2 <- left_join(
  x = df_apptable2,
  y = df_map %>% select(acause_lvl2, cause_name_lvl2),
  by = "acause_lvl2"
)

# Create winsorized max cost column
# Winsorization caps max_cost_per_bene at quantile_99_cost_per_bene
df_apptable2 <- df_apptable2 %>%
  mutate(
    max_cost_per_bene_winsorized = pmin(max_cost_per_bene, quantile_99_cost_per_bene, na.rm = TRUE)
  )

# Aggregate to one row per cause
df_apptable2_agg <- df_apptable2 %>%
  group_by(cause_name_lvl2) %>%
  dplyr::summarise(
    # Weighted median
    median_max_cost = weighted_median(max_cost_per_bene, total_unique_bene),
    median_max_cost_winsor = weighted_median(max_cost_per_bene_winsorized, total_unique_bene),
    
    # Weighted mean
    mean_max_cost = weighted.mean(max_cost_per_bene, total_unique_bene, na.rm = TRUE),
    mean_max_cost_winsor = weighted.mean(max_cost_per_bene_winsorized, total_unique_bene, na.rm = TRUE),
    
    # True max across all strata
    max_max_cost = max(max_cost_per_bene, na.rm = TRUE),
    max_max_cost_winsor = max(max_cost_per_bene_winsorized, na.rm = TRUE),
    
    .groups = "drop"
  )

# Format as dollar amounts
AppTable2 <- df_apptable2_agg %>%
  transmute(
    `Disease Category` = cause_name_lvl2,
    `Median Max Cost per Bene` = dollar(round(median_max_cost, 2)),
    `Median Max Cost per Bene (Winsorized)` = dollar(round(median_max_cost_winsor, 2)),
    `Mean Max Cost per Bene` = dollar(round(mean_max_cost, 2)),
    `Mean Max Cost per Bene (Winsorized)` = dollar(round(mean_max_cost_winsor, 2)),
    `Max Cost per Bene` = dollar(round(max_max_cost, 0)),
    `Max Cost per Bene (Winsorized)` = dollar(round(max_max_cost_winsor, 0))
  ) %>%
  arrange(`Disease Category`)

# Save
fwrite(AppTable2, file.path(output_dir, "AppTable2_Winsorization_by_Condition.csv"))
cat("  Created: AppTable2_Winsorization_by_Condition.csv\n\n")




##########################################################################
# APPENDIX TABLE 3: Summary Statistics of Spending and Encounters by Race
##########################################################################
#
# Shows for each disease category, stratified by race and HIV/SUD status:
#   - Average cost per beneficiary (with sample size n)
#   - Average encounters per beneficiary
#
# Columns: Race (WHT/BLCK/HISP) × HIV/SUD status (None/HIV/SUD/HIV+SUD)
# Rows: 25 disease categories
#
# Source: 01.Summary_Statistics_inflation_adjusted_aggregated.csv
#
##########################################################################

cat("Creating Appendix Table 3: Summary Statistics by Race...\n")

df_apptable3 <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]

# Merge with cause names
df_apptable3 <- left_join(
  x = df_apptable3,
  y = df_map %>% select(acause_lvl2, cause_name_lvl2),
  by = "acause_lvl2"
)

# Create HIV/SUD status variable
df_apptable3 <- df_apptable3 %>%
  mutate(
    hiv_sud_status = case_when(
      has_hiv == 1 & has_sud == 1 ~ "HIV + SUD",
      has_hiv == 1 & has_sud == 0 ~ "HIV",
      has_hiv == 0 & has_sud == 1 ~ "SUD",
      has_hiv == 0 & has_sud == 0 ~ "None",
      TRUE ~ NA_character_
    )
  )

# Aggregate by cause, race, and HIV/SUD status
df_apptable3_agg <- df_apptable3 %>%
  group_by(cause_name_lvl2, race_cd, hiv_sud_status) %>%
  dplyr::summarise(
    avg_cost = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_n = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  )

# Format cost with sample size
df_apptable3_agg <- df_apptable3_agg %>%
  mutate(
    cost_formatted = ifelse(
      is.na(avg_cost) | total_n == 0,
      "NA (n = NA)",
      sprintf("%s (n = %s)", dollar(round(avg_cost, 2)), format(total_n, big.mark = ","))
    ),
    encounters_formatted = ifelse(
      is.na(avg_encounters),
      "",
      round(avg_encounters, 2)
    )
  )

# Pivot to wide format
# First for costs
df_costs_wide <- df_apptable3_agg %>%
  select(cause_name_lvl2, race_cd, hiv_sud_status, cost_formatted) %>%
  pivot_wider(
    names_from = c(race_cd, hiv_sud_status),
    values_from = cost_formatted,
    names_sep = " - "
  )

# Then for encounters
df_encounters_wide <- df_apptable3_agg %>%
  select(cause_name_lvl2, race_cd, hiv_sud_status, encounters_formatted) %>%
  pivot_wider(
    names_from = c(race_cd, hiv_sud_status),
    values_from = encounters_formatted,
    names_sep = " - ",
    names_prefix = "enc_"
  )

# Join costs and encounters
df_apptable3_final <- df_costs_wide %>%
  left_join(df_encounters_wide, by = "cause_name_lvl2")

# Reorder columns to match manuscript format: 
# For each race: None (cost, enc), HIV (cost, enc), SUD (cost, enc), HIV+SUD (cost, enc)
# Order: WHT, BLCK, HISP

# Define column order
race_order <- c("WHT", "BLCK", "HISP")
status_order <- c("None", "HIV", "SUD", "HIV + SUD")

# Build column order
col_order <- c("cause_name_lvl2")
for (race in race_order) {
  for (status in status_order) {
    cost_col <- paste(race, "-", status)
    enc_col <- paste0("enc_", race, " - ", status)
    col_order <- c(col_order, cost_col, enc_col)
  }
}

# Select and reorder columns (only those that exist)
existing_cols <- col_order[col_order %in% names(df_apptable3_final)]
df_apptable3_final <- df_apptable3_final %>%
  select(all_of(existing_cols))

# Rename columns to match manuscript format
new_names <- c("Level 2 Cause")
for (race in race_order) {
  for (status in status_order) {
    new_names <- c(new_names, 
                   paste(race, "-", status),
                   paste(race, "-", status, "Avg Encounters per Bene"))
  }
}

# Only rename if we have the right number of columns
if (length(names(df_apptable3_final)) == length(new_names)) {
  names(df_apptable3_final) <- new_names
}

# Save
fwrite(df_apptable3_final, file.path(output_dir, "AppTable3_Summary_Stats_by_Race.csv"))
cat("  Created: AppTable3_Summary_Stats_by_Race.csv\n\n")


##########################################################################
## SUMMARY
##########################################################################

cat("==================================================================\n")
cat("MANUSCRIPT TABLES CREATED SUCCESSFULLY\n")
cat("==================================================================\n")
cat("Output saved to:", output_dir, "\n")
cat("==================================================================\n")