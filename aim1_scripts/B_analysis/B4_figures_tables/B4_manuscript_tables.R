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


##############################################
##########################################################################

##########################################################################
# TABLE 3: Incremental Costs of HIV and SUD Across GBD Conditions
##########################################################################
cat("Creating Table 3: Incremental Costs Across Conditions...\n")

df_cause_agg <- read_csv(
  file.path(input_dir, "04.By_cause_inflation_adjusted_aggregated_unfiltered.csv"),
  show_col_types = FALSE
)

# Aggregate across race/age/TOC/year with weighted means
df_cause_agg <- df_cause_agg %>%
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

# Format with separate CI columns, exclude HIV and SUD rows
Table3 <- df_cause_agg %>%
  filter(!cause_name_lvl2 %in% c("HIV/AIDS and sexually transmitted infections",
                                 "Substance use disorders")) %>%
  transmute(
    Condition = cause_name_lvl2,
    
    `Baseline Predicted Cost (Condition Alone)` = dollar(round(mean_cost, 2)),
    `Baseline 95% CI` = sprintf("(%s, %s)",
                                dollar(round(lower_ci, 2)),
                                dollar(round(upper_ci, 2))),
    
    `Cost Delta when HIV Present` = dollar(round(mean_delta_hiv, 2)),
    `HIV Delta 95% CI` = sprintf("(%s, %s)",
                                 dollar(round(lower_ci_delta_hiv, 2)),
                                 dollar(round(upper_ci_delta_hiv, 2))),
    
    `Cost Delta when SUD Present` = dollar(round(mean_delta_sud, 2)),
    `SUD Delta 95% CI` = sprintf("(%s, %s)",
                                 dollar(round(lower_ci_delta_sud, 2)),
                                 dollar(round(upper_ci_delta_sud, 2))),
    
    `Cost Delta when HIV + SUD Present` = dollar(round(mean_delta_hiv_sud, 2)),
    `HIV+SUD Delta 95% CI` = sprintf("(%s, %s)",
                                     dollar(round(lower_ci_delta_hiv_sud, 2)),
                                     dollar(round(upper_ci_delta_hiv_sud, 2)))
  ) %>%
  arrange(desc(parse_number(`Baseline Predicted Cost (Condition Alone)`)))

fwrite(Table3, file.path(output_dir, "Table3_Incremental_Costs_by_Condition.csv"))
cat("  Created: Table3_Incremental_Costs_by_Condition.csv\n\n")

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


##################
# to replace table 1 in the paper 
##############

##########################################################################
# FIGURE: HIV, SUD, and HIV+SUD Prevalence Trends by Race/Ethnicity
##########################################################################
#
# Creates a publication-ready figure showing temporal trends in prevalence
# of HIV, SUD, and co-occurring HIV+SUD by race/ethnicity (2010-2019)
#
# Can be used in MAIN MANUSCRIPT (replaces or complements AppTable1)
#
##########################################################################

cat("Creating Figure: Prevalence Trends by Race...\n")

# Prepare data for plotting (use df_apptable1 from the table code above)
df_fig_prevalence <- df_apptable1 %>%
  # Create nice race labels
  mutate(
    Race = case_when(
      race_cd == "WHT" ~ "White",
      race_cd == "BLCK" ~ "Black",
      race_cd == "HISP" ~ "Hispanic",
      TRUE ~ race_cd
    ),
    Race = factor(Race, levels = c("White", "Black", "Hispanic"))
  ) %>%
  # Convert to percentages (multiply by 100 for display)
  mutate(
    hiv_pct_display = hiv_pct * 100,
    sud_pct_display = sud_pct * 100,
    hiv_sud_pct_display = hiv_sud_pct * 100
  ) %>%
  # Pivot to long format for faceting
  pivot_longer(
    cols = c(hiv_pct_display, sud_pct_display, hiv_sud_pct_display),
    names_to = "condition",
    values_to = "prevalence"
  ) %>%
  mutate(
    Condition = case_when(
      condition == "hiv_pct_display" ~ "HIV",
      condition == "sud_pct_display" ~ "Substance Use Disorder",
      condition == "hiv_sud_pct_display" ~ "HIV + SUD"
    ),
    Condition = factor(Condition, levels = c("HIV", "SUD", "HIV + SUD"))
  )

# Define color palette (colorblind-friendly)
race_colors <- c("White" = "#00A1D5", "Black" = "#374E55", "Hispanic" = "#DF8F44")

# Create faceted line plot
fig_prevalence <- ggplot(df_fig_prevalence, 
                         aes(x = year_id, y = prevalence, color = Race, group = Race)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2.5) +
  facet_wrap(~ Condition, scales = "free_y", nrow = 1) +
  scale_color_manual(values = race_colors) +
  scale_x_continuous(breaks = c(2010, 2014, 2015, 2016, 2019)) +
  scale_y_continuous(labels = function(x) paste0(x, "%")) +
  labs(
    title = "Treated HIV, Substance Use Disorder (SUD), and Co-Occurring HIV/SUD\nAmong Medicare Beneficiaries by Race/Ethnicity, 2010–2019",
    x = "Year",
    y = "Treated Prevalence (%)",
    color = "Race/Ethnicity",
    caption = "Note: Medicare Fee-for-Service beneficiaries aged 65–89 years. Sample sizes range from 2.9–3.4 million annually."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold", size = 12, hjust = 0.5),
    strip.text = element_text(face = "bold", size = 11),
    legend.position = "bottom",
    legend.title = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(angle = 45, hjust = 1, size = 9),
    plot.caption = element_text(size = 8, hjust = 0, color = "grey50")
  )

# Save figure
ggsave(file.path(output_dir, "Fig1_Prevalence_Trends_by_Race.png"),
       fig_prevalence, width = 12, height = 5, dpi = 300)
# ggsave(file.path(output_dir, "Fig1_Prevalence_Trends_by_Race.pdf"),
#        fig_prevalence, width = 12, height = 5)

cat("  Created: Fig1_Prevalence_Trends_by_Race.png\n")






  


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

# cat("Creating Appendix Table 2: Winsorization Comparison...\n")
# 
# df_apptable2 <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]
# 
# # Merge with cause names
# df_apptable2 <- left_join(
#   x = df_apptable2,
#   y = df_map %>% select(acause_lvl2, cause_name_lvl2),
#   by = "acause_lvl2"
# )
# 
# # Create winsorized max cost column
# # Winsorization caps max_cost_per_bene at quantile_99_cost_per_bene
# df_apptable2 <- df_apptable2 %>%
#   mutate(
#     max_cost_per_bene_winsorized = pmin(max_cost_per_bene, quantile_99_cost_per_bene, na.rm = TRUE)
#   )
# 
# # Aggregate to one row per cause
# df_apptable2_agg <- df_apptable2 %>%
#   group_by(cause_name_lvl2) %>%
#   dplyr::summarise(
#     # Weighted median
#     median_max_cost = weighted_median(max_cost_per_bene, total_unique_bene),
#     median_max_cost_winsor = weighted_median(max_cost_per_bene_winsorized, total_unique_bene),
#     
#     # Weighted mean
#     mean_max_cost = weighted.mean(max_cost_per_bene, total_unique_bene, na.rm = TRUE),
#     mean_max_cost_winsor = weighted.mean(max_cost_per_bene_winsorized, total_unique_bene, na.rm = TRUE),
#     
#     # True max across all strata
#     max_max_cost = max(max_cost_per_bene, na.rm = TRUE),
#     max_max_cost_winsor = max(max_cost_per_bene_winsorized, na.rm = TRUE),
#     
#     .groups = "drop"
#   )
# 
# # Format as dollar amounts
# AppTable2 <- df_apptable2_agg %>%
#   transmute(
#     `Disease Category` = cause_name_lvl2,
#     `Median Max Cost per Bene` = dollar(round(median_max_cost, 2)),
#     `Median Max Cost per Bene (Winsorized)` = dollar(round(median_max_cost_winsor, 2)),
#     `Mean Max Cost per Bene` = dollar(round(mean_max_cost, 2)),
#     `Mean Max Cost per Bene (Winsorized)` = dollar(round(mean_max_cost_winsor, 2)),
#     `Max Cost per Bene` = dollar(round(max_max_cost, 0)),
#     `Max Cost per Bene (Winsorized)` = dollar(round(max_max_cost_winsor, 0))
#   ) %>%
#   arrange(`Disease Category`)
# 
# # Save
# fwrite(AppTable2, file.path(output_dir, "AppTable2_Winsorization_by_Condition.csv"))
# cat("  Created: AppTable2_Winsorization_by_Condition.csv\n\n")

#### check this

##----------------------------------------------------------------
## SS_T7 - CORRECTED TO MATCH ORIGINAL LOGIC
##----------------------------------------------------------------

df_ss_t7 <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]

# Merge with cause names
df_ss_t7 <- left_join(
  x = df_ss_t7,
  y = df_map %>% select(acause_lvl2, cause_name_lvl2),
  by = "acause_lvl2"
)

# Aggregate using SIMPLE (unweighted) summary statistics
# This matches the original table logic
df_ss_t7_final <- df_ss_t7 %>%
  group_by(cause_name_lvl2) %>%
  dplyr::summarise(
    # Simple median/mean/max across all strata (original approach)
    median_max_cost = median(max_cost_per_bene, na.rm = TRUE),
    median_max_cost_winsor = median(avg_cost_per_bene_winsorized, na.rm = TRUE),
    mean_max_cost = mean(max_cost_per_bene, na.rm = TRUE),
    mean_max_cost_winsor = mean(avg_cost_per_bene_winsorized, na.rm = TRUE),
    max_max_cost = max(max_cost_per_bene, na.rm = TRUE),
    max_max_cost_winsor = max(avg_cost_per_bene_winsorized, na.rm = TRUE),
    .groups = "drop"
  )

# Format as dollar amounts
df_ss_t7_formatted <- df_ss_t7_final %>%
  transmute(
    `Disease Category` = cause_name_lvl2,
    `Median Max Cost per Bene` = dollar(round(median_max_cost, 2)),
    `Median Max Cost per Bene (Winsorized)` = dollar(round(median_max_cost_winsor, 2)),
    `Mean Max Cost per Bene` = dollar(round(mean_max_cost, 2)),
    `Mean Max Cost per Bene (Winsorized)` = dollar(round(mean_max_cost_winsor, 2)),
    `Max Cost per Bene` = dollar(round(max_max_cost, 0)),
    `Max Cost per Bene (Winsorized)` = dollar(round(max_max_cost_winsor, 0))
  )

fwrite(df_ss_t7_formatted, file.path(output_dir, "AppTable2_Winsorization_by_Condition.csv"))

###check this 


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
# APPENDIX TABLE 3: Summary Statistics by Race - SPLIT TABLES + HEATMAP
##########################################################################
#
# Creates:
#   1. Three separate tables split by race (WHT, BLCK, HISP)
#   2. Heatmap figure showing costs by condition × HIV/SUD status × race
#
# Source: 01.Summary_Statistics_inflation_adjusted_aggregated.csv
#
##########################################################################

cat("Creating Appendix Table 3: Split Tables by Race + Heatmap...\n")

##########################################################################
# PART 1: PREPARE DATA
##########################################################################

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
    ),
    # Create ordered factor for proper ordering in figures
    hiv_sud_status_f = factor(hiv_sud_status, 
                              levels = c("None", "HIV", "SUD", "HIV + SUD"))
  )

# Aggregate by cause, race, and HIV/SUD status
df_agg <- df_apptable3 %>%
  group_by(cause_name_lvl2, race_cd, hiv_sud_status, hiv_sud_status_f) %>%
  dplyr::summarise(
    avg_cost = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_n = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  )

# Format cost with sample size for tables
df_agg <- df_agg %>%
  mutate(
    cost_formatted = ifelse(
      is.na(avg_cost) | total_n == 0,
      "—",
      sprintf("%s (n = %s)", dollar(round(avg_cost, 2)), format(total_n, big.mark = ","))
    ),
    encounters_formatted = ifelse(
      is.na(avg_encounters),
      "—",
      sprintf("%.2f", round(avg_encounters, 2))
    )
  )


##########################################################################
# PART 2: SPLIT TABLES BY RACE
##########################################################################

# Function to create race-specific table
create_race_table <- function(data, race_code, race_label) {
  
  df_race <- data %>%
    filter(race_cd == race_code) %>%
    select(cause_name_lvl2, hiv_sud_status, cost_formatted, encounters_formatted)
  
  # Pivot costs wide
  df_costs <- df_race %>%
    select(cause_name_lvl2, hiv_sud_status, cost_formatted) %>%
    pivot_wider(
      names_from = hiv_sud_status,
      values_from = cost_formatted,
      names_prefix = ""
    )
  
  # Pivot encounters wide  
  df_enc <- df_race %>%
    select(cause_name_lvl2, hiv_sud_status, encounters_formatted) %>%
    pivot_wider(
      names_from = hiv_sud_status,
      values_from = encounters_formatted,
      names_prefix = "Enc_"
    )
  
  # Join and reorder columns
  df_final <- df_costs %>%
    left_join(df_enc, by = "cause_name_lvl2")
  
  # Reorder columns: Condition, then for each status: cost, encounters
  status_order <- c("None", "HIV", "SUD", "HIV + SUD")
  col_order <- c("cause_name_lvl2")
  
  for (status in status_order) {
    if (status %in% names(df_final)) {
      col_order <- c(col_order, status)
    }
    enc_col <- paste0("Enc_", status)
    if (enc_col %in% names(df_final)) {
      col_order <- c(col_order, enc_col)
    }
  }
  
  df_final <- df_final %>%
    select(any_of(col_order))
  
  # Rename columns for clarity
  new_names <- c("Condition")
  for (status in status_order) {
    if (status %in% names(df_final) || paste0("Enc_", status) %in% names(df_final)) {
      if (status %in% names(df_final)) {
        new_names <- c(new_names, paste0(status, " Cost (n)"))
      }
      if (paste0("Enc_", status) %in% names(df_final)) {
        new_names <- c(new_names, paste0(status, " Avg Enc"))
      }
    }
  }
  
  if (length(names(df_final)) == length(new_names)) {
    names(df_final) <- new_names
  }
  
  # Sort by condition name
  df_final <- df_final %>% arrange(Condition)
  
  return(df_final)
}

# Create three race-specific tables
table_wht <- create_race_table(df_agg, "WHT", "White")
table_blck <- create_race_table(df_agg, "BLCK", "Black")
table_hisp <- create_race_table(df_agg, "HISP", "Hispanic")

# Save tables
fwrite(table_wht, file.path(output_dir, "AppTable3a_Summary_Stats_White.csv"))
fwrite(table_blck, file.path(output_dir, "AppTable3b_Summary_Stats_Black.csv"))
fwrite(table_hisp, file.path(output_dir, "AppTable3c_Summary_Stats_Hispanic.csv"))

cat("  Created: AppTable3a_Summary_Stats_White.csv\n")
cat("  Created: AppTable3b_Summary_Stats_Black.csv\n")
cat("  Created: AppTable3c_Summary_Stats_Hispanic.csv\n")

##########################################################################
# TABLE: Summary Statistics by Race/Ethnicity and HIV/SUD Status
# (Collapsed across all disease categories)
##########################################################################
#
# Shows average cost per beneficiary and average encounters per beneficiary
# by race/ethnicity and HIV/SUD status, aggregated across all disease 
# categories, types of care, years, and age groups.
#
# This table supports text describing racial/ethnic differences in spending
#
##########################################################################

cat("Creating Table: Summary Statistics by Race (Collapsed)...\n")

# Use summary statistics data
df_race_collapsed <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]

# Filter to ages 65+ 
df_race_collapsed <- df_race_collapsed %>%
  filter(age_group_years_start >= 65)

# Create HIV/SUD status variable
df_race_collapsed <- df_race_collapsed %>%
  mutate(
    hiv_sud_status = case_when(
      has_hiv == 1 & has_sud == 1 ~ "HIV + SUD",
      has_hiv == 1 & has_sud == 0 ~ "HIV",
      has_hiv == 0 & has_sud == 1 ~ "SUD",
      has_hiv == 0 & has_sud == 0 ~ "None",
      TRUE ~ NA_character_
    )
  )

##########################################################################
# PART 1: Overall by Race (collapsed across HIV/SUD status)
##########################################################################

df_race_overall <- df_race_collapsed %>%
  group_by(race_cd) %>%
  dplyr::summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    race_label = case_when(
      race_cd == "WHT" ~ "White",
      race_cd == "BLCK" ~ "Black",
      race_cd == "HISP" ~ "Hispanic",
      TRUE ~ race_cd
    )
  ) %>%
  arrange(desc(avg_cost_per_bene))

cat("\n=== OVERALL SPENDING BY RACE (all causes, all HIV/SUD status) ===\n")
print(df_race_overall)

##########################################################################
# PART 2: By Race AND HIV/SUD Status
##########################################################################

df_race_by_status <- df_race_collapsed %>%
  group_by(race_cd, hiv_sud_status) %>%
  dplyr::summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    race_label = case_when(
      race_cd == "WHT" ~ "White",
      race_cd == "BLCK" ~ "Black",
      race_cd == "HISP" ~ "Hispanic",
      TRUE ~ race_cd
    ),
    hiv_sud_status = factor(hiv_sud_status, levels = c("None", "HIV", "SUD", "HIV + SUD"))
  )

##########################################################################
# PART 3: Create Compact Table (Race as rows, Status as columns)
##########################################################################

# Pivot to wide format for costs
df_costs_wide <- df_race_by_status %>%
  select(race_label, hiv_sud_status, avg_cost_per_bene) %>%
  pivot_wider(
    names_from = hiv_sud_status,
    values_from = avg_cost_per_bene,
    names_prefix = "Cost_"
  )

# Pivot to wide format for encounters
df_enc_wide <- df_race_by_status %>%
  select(race_label, hiv_sud_status, avg_encounters_per_bene) %>%
  pivot_wider(
    names_from = hiv_sud_status,
    values_from = avg_encounters_per_bene,
    names_prefix = "Enc_"
  )

# Pivot to wide format for sample sizes
df_n_wide <- df_race_by_status %>%
  select(race_label, hiv_sud_status, total_bene) %>%
  pivot_wider(
    names_from = hiv_sud_status,
    values_from = total_bene,
    names_prefix = "N_"
  )

# Combine all
df_race_compact <- df_race_overall %>%
  select(race_label, 
         overall_cost = avg_cost_per_bene, 
         overall_enc = avg_encounters_per_bene,
         overall_n = total_bene) %>%
  left_join(df_costs_wide, by = "race_label") %>%
  left_join(df_enc_wide, by = "race_label") %>%
  left_join(df_n_wide, by = "race_label")

# Format final table
AppTable_Race_Collapsed <- df_race_compact %>%
  arrange(desc(overall_cost)) %>%
  transmute(
    `Race/Ethnicity` = race_label,
    `Overall Avg Cost` = dollar(round(overall_cost, 2)),
    `Overall Avg Enc` = sprintf("%.2f", overall_enc),
    `Overall N` = format(overall_n, big.mark = ","),
    `None Cost` = dollar(round(Cost_None, 2)),
    `None Enc` = sprintf("%.2f", Enc_None),
    `None N` = format(N_None, big.mark = ","),
    `HIV Cost` = dollar(round(Cost_HIV, 2)),
    `HIV Enc` = sprintf("%.2f", Enc_HIV),
    `HIV N` = format(N_HIV, big.mark = ","),
    `SUD Cost` = dollar(round(Cost_SUD, 2)),
    `SUD Enc` = sprintf("%.2f", Enc_SUD),
    `SUD N` = format(N_SUD, big.mark = ","),
    `HIV+SUD Cost` = dollar(round(`Cost_HIV + SUD`, 2)),
    `HIV+SUD Enc` = sprintf("%.2f", `Enc_HIV + SUD`),
    `HIV+SUD N` = format(`N_HIV + SUD`, big.mark = ",")
  )

# Print table
cat("\n=== COMPACT TABLE: SPENDING BY RACE AND HIV/SUD STATUS ===\n")
print(AppTable_Race_Collapsed)

# Save table
fwrite(AppTable_Race_Collapsed, file.path(output_dir, "AppTable_Summary_Stats_by_Race_Collapsed.csv"))
cat("\n  Created: AppTable_Summary_Stats_by_Race_Collapsed.csv\n")


##########################################################################
# PART 4: Simpler Table Format (matching your example)
##########################################################################

# Create simpler format with cost (n) and encounters
AppTable_Race_Simple <- df_race_by_status %>%
  arrange(race_label, hiv_sud_status) %>%
  transmute(
    `Race/Ethnicity` = race_label,
    `HIV/SUD Status` = hiv_sud_status,
    `Avg Cost per Bene (n)` = sprintf("%s (n = %s)", 
                                      dollar(round(avg_cost_per_bene, 2)),
                                      format(total_bene, big.mark = ",")),
    `Avg Encounters per Bene` = sprintf("%.2f", avg_encounters_per_bene)
  )

# Save simple table
fwrite(AppTable_Race_Simple, file.path(output_dir, "AppTable_Summary_Stats_by_Race_Simple.csv"))
cat("  Created: AppTable_Summary_Stats_by_Race_Simple.csv\n")



#
##########################################################################
# PART 3: HEATMAP FIGURE
##########################################################################

##########################################################################
# HEATMAP FIGURES WITH COSTS AND ENCOUNTERS
# 
# Two heatmaps:
#   1. Absolute costs heatmap (with encounters shown in parentheses)
#   2. Cost ratio heatmap (with encounter ratios shown in parentheses)
#
##########################################################################

##########################################################################
# PART 3: ABSOLUTE COSTS HEATMAP (with encounters)
##########################################################################

# Prepare data for heatmap
df_heatmap <- df_agg %>%
  filter(!is.na(avg_cost), !is.na(cause_name_lvl2)) %>%
  mutate(
    # Create nice race labels
    race_label = case_when(
      race_cd == "WHT" ~ "White",
      race_cd == "BLCK" ~ "Black", 
      race_cd == "HISP" ~ "Hispanic",
      TRUE ~ race_cd
    ),
    race_label = factor(race_label, levels = c("White", "Black", "Hispanic")),
    # Log transform costs for better color scaling
    log_cost = log10(avg_cost + 1),
    # Truncate very long condition names
    condition_short = case_when(
      nchar(cause_name_lvl2) > 25 ~ paste0(substr(cause_name_lvl2, 1, 22), "..."),
      TRUE ~ cause_name_lvl2
    ),
    # Create label with cost and encounters
    cell_label = sprintf("%s\n(%.1f enc)", 
                         dollar(round(avg_cost, 0)), 
                         round(avg_encounters, 1))
  )

# Order conditions by average cost across all groups (descending)
condition_order <- df_heatmap %>%
  group_by(cause_name_lvl2) %>%
  dplyr::summarise(mean_cost = mean(avg_cost, na.rm = TRUE), .groups = "drop") %>%
  arrange(desc(mean_cost)) %>%
  pull(cause_name_lvl2)

df_heatmap <- df_heatmap %>%
  mutate(cause_name_lvl2 = factor(cause_name_lvl2, levels = rev(condition_order)))

# Calculate total sample size for caption
total_n <- df_agg %>%
  filter(!is.na(total_n)) %>%
  summarise(total = sum(total_n, na.rm = TRUE)) %>%
  pull(total)

total_n_formatted <- format(total_n, big.mark = ",")

# Create heatmap with costs and encounters
heatmap_fig <- ggplot(df_heatmap, aes(x = hiv_sud_status_f, y = cause_name_lvl2, fill = avg_cost)) +
  geom_tile(color = "white", linewidth = 0.3) +
  geom_text(aes(label = cell_label), 
            size = 1.8, color = "black", lineheight = 0.8) +
  facet_wrap(~ race_label, nrow = 1) +
  scale_fill_gradient2(
    low = "#f7fbff",
    mid = "#6baed6", 
    high = "#08306b",
    midpoint = median(df_heatmap$avg_cost, na.rm = TRUE),
    name = "Avg Cost\nper Bene",
    labels = dollar_format(),
    na.value = "grey90"
  ) +
  labs(
    title = "Average Annual Per-Beneficiary Spending and Utilization\nby Disease Category, HIV/SUD Status, and Race/Ethnicity",
    subtitle = "Medicare Fee-for-Service Beneficiaries Ages 65+, 2010-2019",
    x = "HIV/SUD Status",
    y = NULL,
    caption = paste0("Note: Values show average cost per beneficiary with average encounters per beneficiary in parentheses.\n",
                     "Costs inflation-adjusted to 2019 USD. 'None' = no HIV or SUD diagnosis.\n",
                     "Total N = ", total_n_formatted, " beneficiary-condition-year observations.")
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 8),
    axis.text.y = element_text(size = 7),
    strip.text = element_text(face = "bold", size = 10),
    panel.grid = element_blank(),
    legend.position = "right",
    legend.key.height = unit(1.5, "cm"),
    plot.title = element_text(face = "bold", size = 11, hjust = 0.5),
    plot.subtitle = element_text(size = 9, hjust = 0.5, color = "grey40"),
    plot.caption = element_text(size = 7, hjust = 0, color = "grey50")
  )

# Save heatmap
ggsave(file.path(output_dir, "AppFig_Heatmap_Costs_Encounters_by_Race_Status.png"),
       heatmap_fig, width = 14, height = 10, dpi = 300)

cat("  Created: AppFig_Heatmap_Costs_Encounters_by_Race_Status.png\n")


##########################################################################
# PART 4: COST RATIO HEATMAP WITH ENCOUNTER RATIOS
##########################################################################


# Appendix Figure Y. Cost and encounter multipliers for beneficiaries with HIV and/or SUD relative to those without, by disease category and race/ethnicity.
# Values show ratio compared to 'None' baseline (e.g., 2.0x = twice the cost; 1.5x enc = 1.5 times the encounters).
# Descriptive statistics from Medicare FFS beneficiaries aged ≥65 years (2010-2019).
# HIV/AIDS and SUD rows excluded because no 'None' baseline exists for these conditions.


# ---- 1) Build baseline table: "None" within each cause × race ----
baseline_tbl <- df_agg %>%
  dplyr::filter(!is.na(avg_cost), hiv_sud_status == "None") %>%
  dplyr::group_by(cause_name_lvl2, race_cd) %>%
  dplyr::summarise(
    baseline_cost = mean(avg_cost, na.rm = TRUE),
    baseline_encounters = mean(avg_encounters, na.rm = TRUE),
    baseline_n = sum(total_n, na.rm = TRUE),
    .groups = "drop"
  )

# ---- 2) Compute ratios for non-baseline statuses; drop groups without baseline ----
df_ratios <- df_agg %>%
  dplyr::filter(!is.na(avg_cost), hiv_sud_status != "None") %>%
  dplyr::left_join(baseline_tbl, by = c("cause_name_lvl2", "race_cd")) %>%
  dplyr::filter(!is.na(baseline_cost), baseline_cost > 0) %>%
  dplyr::mutate(
    cost_ratio = avg_cost / baseline_cost,
    encounter_ratio = avg_encounters / baseline_encounters,
    race_label = dplyr::case_when(
      race_cd == "WHT"  ~ "White",
      race_cd == "BLCK" ~ "Black",
      race_cd == "HISP" ~ "Hispanic",
      TRUE              ~ as.character(race_cd)
    ),
    race_label = factor(race_label, levels = c("White", "Black", "Hispanic")),
    cause_name_lvl2 = factor(cause_name_lvl2, levels = rev(condition_order)),
    # Create label with cost ratio and encounter ratio
    cell_label = sprintf("%.1fx\n(%.1fx enc)", 
                         round(cost_ratio, 1), 
                         round(encounter_ratio, 1))
  )

# Calculate sample sizes for caption
n_with_hivsud <- df_agg %>%
  filter(!is.na(total_n), hiv_sud_status != "None") %>%
  summarise(total = sum(total_n, na.rm = TRUE)) %>%
  pull(total)

n_without_hivsud <- df_agg %>%
  filter(!is.na(total_n), hiv_sud_status == "None") %>%
  summarise(total = sum(total_n, na.rm = TRUE)) %>%
  pull(total)

# Warn if anything got dropped
n_missing_baseline <- df_agg %>%
  dplyr::filter(!is.na(avg_cost), hiv_sud_status != "None") %>%
  dplyr::anti_join(baseline_tbl, by = c("cause_name_lvl2", "race_cd")) %>%
  dplyr::distinct(cause_name_lvl2, race_cd) %>%
  nrow()

if (n_missing_baseline > 0) {
  message("NOTE: Dropped ", n_missing_baseline,
          " cause×race groups with no 'None' baseline (e.g., HIV/AIDS, SUD).")
}

# ---- 3) Create cost ratio heatmap with encounter ratios ----
ratio_heatmap <- ggplot(df_ratios, aes(x = hiv_sud_status_f, y = cause_name_lvl2, fill = cost_ratio)) +
  geom_tile(color = "white", linewidth = 0.3) +
  geom_text(aes(label = cell_label),
            size = 1.9, color = "black", lineheight = 0.8) +
  facet_wrap(~ race_label, nrow = 1) +
  scale_fill_gradient2(
    low = "#fee8c8",
    mid = "#fdbb84",
    high = "#b30000",
    midpoint = 2,
    limits = c(1, NA),
    name = "Cost\nMultiplier",
    na.value = "grey90"
  ) +
  labs(
    title = "Cost and Utilization Multiplier Effects of HIV and SUD",
    subtitle = "Relative to beneficiaries with same condition but no HIV/SUD | Medicare FFS Ages 65+, 2010-2019",
    x = "HIV/SUD Status",
    y = NULL,
    caption = paste0("Note: Values show cost multiplier with encounter multiplier in parentheses, relative to 'None' baseline.\n",
                     "Example: 2.0x (1.5x enc) means twice the cost and 1.5 times the encounters.\n",
                     "Costs inflation-adjusted to 2019 USD. HIV/AIDS and SUD rows excluded (no 'None' baseline).\n",
                     "Total N = ", format(n_without_hivsud, big.mark = ","), " (None); ",
                     format(n_with_hivsud, big.mark = ","), " (HIV/SUD groups).")
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 9),
    axis.text.y = element_text(size = 7),
    strip.text = element_text(face = "bold", size = 10),
    panel.grid = element_blank(),
    legend.position = "right",
    legend.key.height = unit(1.5, "cm"),
    plot.title = element_text(face = "bold", size = 11, hjust = 0.5),
    plot.subtitle = element_text(size = 9, hjust = 0.5, color = "grey40"),
    plot.caption = element_text(size = 7, hjust = 0, color = "grey50")
  )

# ---- 4) Save ratio heatmap ----
#not used for now in the paper 

# ggsave(file.path(output_dir, "AppFig_Heatmap_CostEncounterRatios_by_Race_Status.png"),
#        ratio_heatmap, width = 12, height = 10, dpi = 300)
# 
# cat("  Created: AppFig_Heatmap_CostEncounterRatios_by_Race_Status.png\n")


##########################################################################
# APPENDIX TABLE: Population Characteristics by Age Group
##########################################################################
#
# Shows HIV, SUD, and HIV+SUD prevalence by age group (65-89 years)
# Also shows average cost and encounters per beneficiary by age
#
# This table supports the text describing age patterns in prevalence
#
##########################################################################

cat("Creating Table: Population Characteristics by Age Group...\n")

# Use the meta statistics data
df_by_age <- data_list[["03.Meta_Statistics_aggregated"]]

# Clean and convert
df_by_age$total_unique_bene <- as.integer(str_remove_all(df_by_age$total_unique_bene, ","))

# Filter to ages 65+ only and exclude RX (pharmacy) data for cleaner estimates
df_by_age <- df_by_age %>%
  filter(age_group_years_start >= 65, 
         file_type == "F2T")  # Fee-for-service claims only

# Aggregate by age group (across all years and TOC)
df_age_summary <- df_by_age %>%
  group_by(age_group_years_start) %>%
  dplyr::summarise(
    total_unique_bene_sum = sum(total_unique_bene, na.rm = TRUE),
    total_unique_hiv_bene = sum(hiv_unique_bene, na.rm = TRUE),
    total_unique_sud_bene = sum(sud_unique_bene, na.rm = TRUE),
    total_unique_hiv_sud_bene = sum(hiv_and_sud_unique_bene, na.rm = TRUE),
    .groups = "drop"
  )

# Calculate percentages
df_age_summary <- df_age_summary %>%
  mutate(
    hiv_pct = total_unique_hiv_bene / total_unique_bene_sum * 100,
    sud_pct = total_unique_sud_bene / total_unique_bene_sum * 100,
    hiv_sud_pct = total_unique_hiv_sud_bene / total_unique_bene_sum * 100,
    # Create age group labels
    age_group_label = case_when(
      age_group_years_start == 65 ~ "65–69",
      age_group_years_start == 70 ~ "70–74",
      age_group_years_start == 75 ~ "75–79",
      age_group_years_start == 80 ~ "80–84",
      age_group_years_start == 85 ~ "85–89",
      TRUE ~ as.character(age_group_years_start)
    )
  )

# Print summary for text
cat("\n=== PREVALENCE BY AGE GROUP (65+) ===\n")
cat("Age Group | HIV % | SUD % | HIV+SUD %\n")
cat("----------|-------|-------|----------\n")
for (i in 1:nrow(df_age_summary)) {
  cat(sprintf("%s     | %.2f%% | %.2f%% | %.3f%%\n",
              df_age_summary$age_group_label[i],
              df_age_summary$hiv_pct[i],
              df_age_summary$sud_pct[i],
              df_age_summary$hiv_sud_pct[i]))
}

# Format final table
AppTable_Age <- df_age_summary %>%
  transmute(
    `Age Group` = age_group_label,
    `Total Beneficiaries` = format(total_unique_bene_sum, big.mark = ","),
    `HIV Beneficiaries (n, %)` = sprintf("%s (%.2f%%)", 
                                         format(total_unique_hiv_bene, big.mark = ","),
                                         hiv_pct),
    `SUD Beneficiaries (n, %)` = sprintf("%s (%.2f%%)", 
                                         format(total_unique_sud_bene, big.mark = ","),
                                         sud_pct),
    `HIV + SUD Beneficiaries (n, %)` = sprintf("%s (%.3f%%)", 
                                               format(total_unique_hiv_sud_bene, big.mark = ","),
                                               hiv_sud_pct)
  )

# Save table
fwrite(AppTable_Age, file.path(output_dir, "AppTable_Population_Characteristics_by_Age.csv"))
cat("\n  Created: AppTable_Population_Characteristics_by_Age.csv\n\n")


##########################################################################
# PART 2: Add Average Cost and Encounters by Age
# Using Summary Statistics data
##########################################################################

cat("Adding cost and encounter data by age...\n")

# Use summary statistics data for cost/encounters
df_ss <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]

# Filter to ages 65+ 
df_ss_age <- df_ss %>%
  filter(age_group_years_start >= 65)

# Aggregate by age group
df_cost_age <- df_ss_age %>%
  group_by(age_group_years_start) %>%
  dplyr::summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    age_group_label = case_when(
      age_group_years_start == 65 ~ "65–69",
      age_group_years_start == 70 ~ "70–74",
      age_group_years_start == 75 ~ "75–79",
      age_group_years_start == 80 ~ "80–84",
      age_group_years_start == 85 ~ "85–89",
      TRUE ~ as.character(age_group_years_start)
    )
  )

# Print summary for text
cat("\n=== COST AND ENCOUNTERS BY AGE GROUP (65+) ===\n")
cat("Age Group | Avg Cost/Bene | Avg Encounters/Bene\n")
cat("----------|---------------|--------------------\n")
for (i in 1:nrow(df_cost_age)) {
  cat(sprintf("%s     | $%,.2f      | %.2f\n",
              df_cost_age$age_group_label[i],
              df_cost_age$avg_cost_per_bene[i],
              df_cost_age$avg_encounters_per_bene[i]))
}

# Combine prevalence and cost data
df_combined_age <- df_age_summary %>%
  left_join(df_cost_age %>% select(age_group_years_start, avg_cost_per_bene, avg_encounters_per_bene),
            by = "age_group_years_start")

# Format combined table
AppTable_Age_Full <- df_combined_age %>%
  transmute(
    `Age Group` = age_group_label,
    `Total Beneficiaries` = format(total_unique_bene_sum, big.mark = ","),
    `HIV (%)` = sprintf("%.2f%%", hiv_pct),
    `SUD (%)` = sprintf("%.2f%%", sud_pct),
    `HIV+SUD (%)` = sprintf("%.3f%%", hiv_sud_pct),
    `Avg Cost per Bene` = dollar(round(avg_cost_per_bene, 2)),
    `Avg Encounters per Bene` = sprintf("%.2f", avg_encounters_per_bene)
  )

# Save combined table
fwrite(AppTable_Age_Full, file.path(output_dir, "AppTable_Population_by_Age_Full.csv"))
cat("\n  Created: AppTable_Population_by_Age_Full.csv\n\n")


##########################################################################
# SUMMARY TEXT FOR MANUSCRIPT
##########################################################################

# Extract key values for manuscript text
hiv_65 <- df_age_summary$hiv_pct[df_age_summary$age_group_years_start == 65]
hiv_85 <- df_age_summary$hiv_pct[df_age_summary$age_group_years_start == 85]
sud_65 <- df_age_summary$sud_pct[df_age_summary$age_group_years_start == 65]
sud_85 <- df_age_summary$sud_pct[df_age_summary$age_group_years_start == 85]

cost_65 <- df_cost_age$avg_cost_per_bene[df_cost_age$age_group_years_start == 65]
cost_85 <- df_cost_age$avg_cost_per_bene[df_cost_age$age_group_years_start == 85]
enc_65 <- df_cost_age$avg_encounters_per_bene[df_cost_age$age_group_years_start == 65]
enc_85 <- df_cost_age$avg_encounters_per_bene[df_cost_age$age_group_years_start == 85]

cat("==================================================================\n")
cat("SUGGESTED TEXT FOR MANUSCRIPT (updated for 65+ population):\n")
cat("==================================================================\n\n")

cat(sprintf(
  '"The prevalence of HIV claims differed across age with greater representation\n in younger Medicare beneficiaries (i.e., %.2f%% among beneficiaries aged 65–69\n years to %.2f%% among those aged 85–89 years), SUD decreased steadily across\n age groups (%.1f%% to %.1f%%). Average annual healthcare spending decreased\n modestly with age, ranging from $%,.0f per beneficiary at ages 65–69 to $%,.0f\n at ages 85–89, while mean utilization remained relatively stable across age\n groups, with approximately %.1f–%.1f encounters per beneficiary."\n',
  hiv_65, hiv_85, sud_65, sud_85, cost_65, cost_85, 
  min(df_cost_age$avg_encounters_per_bene), max(df_cost_age$avg_encounters_per_bene)
))

cat("\n==================================================================\n")



##########################################################################
# TABLE: Average Spending and Utilization by Type of Care and HIV/SUD Status
##########################################################################
#
# Shows average cost per beneficiary and average encounters per beneficiary
# by type of care (Inpatient, Ambulatory, ED, Home Health, Rx)
# Stratified by HIV/SUD status (None, HIV only, SUD only, HIV+SUD)
#
# This table supports the text describing care utilization patterns
#
##########################################################################

cat("Creating Table: Spending and Utilization by Type of Care...\n")

# Use summary statistics data
df_toc <- data_list[["01.Summary_Statistics_inflation_adjusted_aggregated"]]

# Filter to ages 65+ 
df_toc <- df_toc %>%
  filter(age_group_years_start >= 65)

# Create HIV/SUD status variable
df_toc <- df_toc %>%
  mutate(
    hiv_sud_status = case_when(
      has_hiv == 1 & has_sud == 1 ~ "HIV + SUD",
      has_hiv == 1 & has_sud == 0 ~ "HIV",
      has_hiv == 0 & has_sud == 1 ~ "SUD",
      has_hiv == 0 & has_sud == 0 ~ "None",
      TRUE ~ NA_character_
    )
  )

# Create nice TOC labels
df_toc <- df_toc %>%
  mutate(
    toc_label = case_when(
      toc == "IP" ~ "Inpatient",
      toc == "AM" ~ "Ambulatory",
      toc == "ED" ~ "Emergency Department",
      toc == "HH" ~ "Home Health",
      toc == "RX" ~ "Prescription Drugs",
      TRUE ~ toc
    ),
    toc_label = factor(toc_label, levels = c("Inpatient", "Home Health", "Ambulatory", 
                                             "Emergency Department", "Prescription Drugs"))
  )

##########################################################################
# PART 1: Overall by Type of Care (All Beneficiaries)
##########################################################################

df_toc_overall <- df_toc %>%
  group_by(toc_label) %>%
  dplyr::summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(avg_cost_per_bene))

cat("\n=== OVERALL SPENDING BY TYPE OF CARE ===\n")
print(df_toc_overall)

##########################################################################
# PART 2: By Type of Care AND HIV/SUD Status
##########################################################################

df_toc_by_status <- df_toc %>%
  group_by(toc_label, hiv_sud_status) %>%
  dplyr::summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, total_unique_bene, na.rm = TRUE),
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  )

# Pivot to wide format for costs
df_costs_wide <- df_toc_by_status %>%
  select(toc_label, hiv_sud_status, avg_cost_per_bene) %>%
  pivot_wider(
    names_from = hiv_sud_status,
    values_from = avg_cost_per_bene,
    names_prefix = "Cost_"
  )

# Pivot to wide format for encounters
df_enc_wide <- df_toc_by_status %>%
  select(toc_label, hiv_sud_status, avg_encounters_per_bene) %>%
  pivot_wider(
    names_from = hiv_sud_status,
    values_from = avg_encounters_per_bene,
    names_prefix = "Enc_"
  )

# Combine
df_toc_combined <- df_costs_wide %>%
  left_join(df_enc_wide, by = "toc_label")

##########################################################################
# PART 3: Format Final Table
##########################################################################

# Format table with costs and encounters side by side
AppTable_TOC <- df_toc_by_status %>%
  mutate(
    hiv_sud_status = factor(hiv_sud_status, levels = c("None", "HIV", "SUD", "HIV + SUD"))
  ) %>%
  arrange(toc_label, hiv_sud_status) %>%
  transmute(
    `Type of Care` = as.character(toc_label),
    `HIV/SUD Status` = hiv_sud_status,
    `Avg Cost per Bene` = dollar(round(avg_cost_per_bene, 2)),
    `Avg Encounters per Bene` = sprintf("%.2f", avg_encounters_per_bene),
    `Total Beneficiaries` = format(total_bene, big.mark = ",")
  )

# Save detailed table
fwrite(AppTable_TOC, file.path(output_dir, "AppTable_Spending_by_TOC_and_Status.csv"))
cat("\n  Created: AppTable_Spending_by_TOC_and_Status.csv\n")

##########################################################################
# PART 4: Create Compact Summary Table (TOC as rows, Status as columns)
##########################################################################

# Create compact table with all info
AppTable_TOC_Compact <- df_toc_overall %>%
  left_join(
    df_toc_by_status %>% 
      filter(hiv_sud_status == "None") %>%
      select(toc_label, cost_none = avg_cost_per_bene, enc_none = avg_encounters_per_bene),
    by = "toc_label"
  ) %>%
  left_join(
    df_toc_by_status %>% 
      filter(hiv_sud_status == "HIV") %>%
      select(toc_label, cost_hiv = avg_cost_per_bene, enc_hiv = avg_encounters_per_bene),
    by = "toc_label"
  ) %>%
  left_join(
    df_toc_by_status %>% 
      filter(hiv_sud_status == "SUD") %>%
      select(toc_label, cost_sud = avg_cost_per_bene, enc_sud = avg_encounters_per_bene),
    by = "toc_label"
  ) %>%
  left_join(
    df_toc_by_status %>% 
      filter(hiv_sud_status == "HIV + SUD") %>%
      select(toc_label, cost_hiv_sud = avg_cost_per_bene, enc_hiv_sud = avg_encounters_per_bene),
    by = "toc_label"
  ) %>%
  arrange(desc(avg_cost_per_bene)) %>%
  transmute(
    `Type of Care` = as.character(toc_label),
    `Overall Avg Cost` = dollar(round(avg_cost_per_bene, 2)),
    `Overall Avg Enc` = sprintf("%.2f", avg_encounters_per_bene),
    `None Cost` = dollar(round(cost_none, 2)),
    `None Enc` = sprintf("%.2f", enc_none),
    `HIV Cost` = dollar(round(cost_hiv, 2)),
    `HIV Enc` = sprintf("%.2f", enc_hiv),
    `SUD Cost` = dollar(round(cost_sud, 2)),
    `SUD Enc` = sprintf("%.2f", enc_sud),
    `HIV+SUD Cost` = dollar(round(cost_hiv_sud, 2)),
    `HIV+SUD Enc` = sprintf("%.2f", enc_hiv_sud)
  )

# Print compact table
cat("\n=== COMPACT TABLE: SPENDING BY TYPE OF CARE AND HIV/SUD STATUS ===\n")
print(AppTable_TOC_Compact)

# Save compact table
fwrite(AppTable_TOC_Compact, file.path(output_dir, "AppTable_Spending_by_TOC_Compact.csv"))
cat("\n  Created: AppTable_Spending_by_TOC_Compact.csv\n")


##########################################################################
# PRINT VALUES FOR MANUSCRIPT TEXT
##########################################################################

cat("\n==================================================================\n")
cat("VALUES FOR MANUSCRIPT TEXT:\n")
cat("==================================================================\n\n")

# Extract key values
ip <- df_toc_overall %>% filter(toc_label == "Inpatient")
hh <- df_toc_overall %>% filter(toc_label == "Home Health")
am <- df_toc_overall %>% filter(toc_label == "Ambulatory")
ed <- df_toc_overall %>% filter(toc_label == "Emergency Department")
rx <- df_toc_overall %>% filter(toc_label == "Prescription Drugs")

cat("OVERALL (all beneficiaries, all causes):\n")
cat(sprintf("  Inpatient:          Avg Cost = $%,.2f, Avg Enc = %.2f\n", ip$avg_cost_per_bene, ip$avg_encounters_per_bene))
cat(sprintf("  Home Health:        Avg Cost = $%,.2f, Avg Enc = %.2f\n", hh$avg_cost_per_bene, hh$avg_encounters_per_bene))
cat(sprintf("  Ambulatory:         Avg Cost = $%,.2f, Avg Enc = %.2f\n", am$avg_cost_per_bene, am$avg_encounters_per_bene))
cat(sprintf("  Emergency Dept:     Avg Cost = $%,.2f, Avg Enc = %.2f\n", ed$avg_cost_per_bene, ed$avg_encounters_per_bene))
cat(sprintf("  Prescription Drugs: Avg Cost = $%,.2f, Avg Enc = %.2f\n", rx$avg_cost_per_bene, rx$avg_encounters_per_bene))

# HIV vs SUD patterns
hiv_costs <- df_toc_by_status %>% filter(hiv_sud_status == "HIV")
sud_costs <- df_toc_by_status %>% filter(hiv_sud_status == "SUD")

cat("\nHIV-SPECIFIC COSTS:\n")
for (i in 1:nrow(hiv_costs)) {
  cat(sprintf("  %s: $%,.2f (%.2f enc)\n", 
              hiv_costs$toc_label[i], hiv_costs$avg_cost_per_bene[i], hiv_costs$avg_encounters_per_bene[i]))
}

cat("\nSUD-SPECIFIC COSTS:\n")
for (i in 1:nrow(sud_costs)) {
  cat(sprintf("  %s: $%,.2f (%.2f enc)\n", 
              sud_costs$toc_label[i], sud_costs$avg_cost_per_bene[i], sud_costs$avg_encounters_per_bene[i]))
}

cat("\n")
cat("SUGGESTED TEXT:\n")
cat(sprintf(
  '"Across all beneficiaries for all causes, inpatient care accounted for the\nhighest average annual per-beneficiary spending ($%,.0f), followed by home\nhealth services ($%,.0f), whereas ambulatory and emergency department care\ncontributed lower average spending per beneficiary ($%,.0f and $%,.0f,\nrespectively). Prescription drug claims accounted for the highest mean number\nof encounters per beneficiary (%.2f) but lower average annual spending ($%,.0f)."\n',
  ip$avg_cost_per_bene, hh$avg_cost_per_bene, am$avg_cost_per_bene, ed$avg_cost_per_bene,
  rx$avg_encounters_per_bene, rx$avg_cost_per_bene
))

cat("\n==================================================================\n")




##########################################################################
# TABLES: HIV and SUD Primary Condition Costs by Race and Age
##########################################################################
#
# Creates 2 tables:
#   1. HIV/SUD primary costs by race (for first text block)
#   2. HIV/SUD primary costs by age (for first text block)
#
# Source: 05_HIV and 05_SUD aggregated files
#
##########################################################################



##########################################################################
# PART 1: Load data
##########################################################################

df_hiv <- data_list[["05.HIV_inflation_adjusted_aggregated"]]
df_sud <- data_list[["05.SUD_inflation_adjusted_aggregated"]]

df_hiv <- df_hiv %>% filter(age_group_years_start >= 65)
df_sud <- df_sud %>% filter(age_group_years_start >= 65)

##########################################################################
# PART 2: Table - HIV/SUD Primary Costs by Race
##########################################################################

# HIV by race
df_hiv_race <- df_hiv %>%
  group_by(race_cd) %>%
  dplyr::summarise(
    hiv_cost = weighted.mean(mean_cost_hiv, total_row_count, na.rm = TRUE),
    hiv_lower = weighted.mean(lower_ci_hiv, total_row_count, na.rm = TRUE),
    hiv_upper = weighted.mean(upper_ci_hiv, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# SUD by race
df_sud_race <- df_sud %>%
  group_by(race_cd) %>%
  dplyr::summarise(
    sud_cost = weighted.mean(mean_cost_sud, total_row_count, na.rm = TRUE),
    sud_lower = weighted.mean(lower_ci_sud, total_row_count, na.rm = TRUE),
    sud_upper = weighted.mean(upper_ci_sud, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Combine
AppTable_Primary_by_Race <- df_hiv_race %>%
  left_join(df_sud_race, by = "race_cd") %>%
  mutate(
    race_label = case_when(
      race_cd == "WHT" ~ "White",
      race_cd == "BLCK" ~ "Black",
      race_cd == "HISP" ~ "Hispanic"
    )
  ) %>%
  arrange(desc(hiv_cost)) %>%
  transmute(
    `Race/Ethnicity` = race_label,
    `HIV Baseline Cost` = dollar(round(hiv_cost, 2)),
    `HIV 95% CI` = sprintf("(%s - %s)", dollar(round(hiv_lower, 2)), dollar(round(hiv_upper, 2))),
    `SUD Baseline Cost` = dollar(round(sud_cost, 2)),
    `SUD 95% CI` = sprintf("(%s - %s)", dollar(round(sud_lower, 2)), dollar(round(sud_upper, 2)))
  )

fwrite(AppTable_Primary_by_Race, file.path(output_dir, "AppTable_HIV_SUD_Primary_by_Race.csv"))
cat("  Created: AppTable_HIV_SUD_Primary_by_Race.csv\n")

##########################################################################
# PART 3: Table - HIV/SUD Primary Costs by Age
##########################################################################

# HIV by age
df_hiv_age <- df_hiv %>%
  group_by(age_group_years_start) %>%
  dplyr::summarise(
    hiv_cost = weighted.mean(mean_cost_hiv, total_row_count, na.rm = TRUE),
    hiv_lower = weighted.mean(lower_ci_hiv, total_row_count, na.rm = TRUE),
    hiv_upper = weighted.mean(upper_ci_hiv, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# SUD by age
df_sud_age <- df_sud %>%
  group_by(age_group_years_start) %>%
  dplyr::summarise(
    sud_cost = weighted.mean(mean_cost_sud, total_row_count, na.rm = TRUE),
    sud_lower = weighted.mean(lower_ci_sud, total_row_count, na.rm = TRUE),
    sud_upper = weighted.mean(upper_ci_sud, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Combine
AppTable_Primary_by_Age <- df_hiv_age %>%
  left_join(df_sud_age, by = "age_group_years_start") %>%
  arrange(age_group_years_start) %>%
  transmute(
    `Age Group` = paste0(age_group_years_start, "+"),
    `HIV Baseline Cost` = dollar(round(hiv_cost, 2)),
    `HIV 95% CI` = sprintf("(%s - %s)", dollar(round(hiv_lower, 2)), dollar(round(hiv_upper, 2))),
    `SUD Baseline Cost` = dollar(round(sud_cost, 2)),
    `SUD 95% CI` = sprintf("(%s - %s)", dollar(round(sud_lower, 2)), dollar(round(sud_upper, 2)))
  )

fwrite(AppTable_Primary_by_Age, file.path(output_dir, "AppTable_HIV_SUD_Primary_by_Age.csv"))
cat("  Created: AppTable_HIV_SUD_Primary_by_Age.csv\n\n")
