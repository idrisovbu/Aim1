############################################################################
# Title: B4_figures.R
# Description: Generates plots from the master table .csv file & from subtable csv files.
# Inputs: 05.Aggregation_Summary/bested/*.csv
# Outputs: 06.Figures/<date>/*.png
############################################################################

##########################################################################
# 0. Setup environment
##########################################################################
rm(list = ls())
pacman::p_load(ggplot2, readr, tidyverse, viridis, scales, ggsci,viridis, plotly, htmlwidgets,data.table) 


# Set the current date for folder naming
date_today <- format(Sys.time(), "%Y%m%d")

# Detect IHME cluster by checking for /mnt/share/limited_use
if (dir.exists("/mnt/share/limited_use")) {
  # IHME/cluster environment
  date_of_input <- "20251108" # last run on 20250914
  base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary", date_of_input)
  figures_dir <- file.path(base_dir, "06.Figures")
  # (add your other cluster-specific library loads here)
} else {
  # Local development define manually
  base_dir <- "~/Aim1WD/"
  input_dir <- file.path(base_dir, "aim1_Input_personal_mac")
  figures_dir <- file.path(base_dir, "aim1_output_personal_mac", "06.Figures")
}

# Create output directory for today's date
output_dir <- file.path(figures_dir, date_today)
if (!dir.exists(figures_dir)) dir.create(figures_dir, recursive = TRUE)
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

#  list available input files
list.files(input_dir, pattern = "\\.csv$", full.names = TRUE)


##########################################################################
# 0.1 Functions for saving the plots 
##########################################################################

save_plot <- function(ggplot_obj, ggplot_name, width = 12, height = 10, dpi = 500, path = ".") {
  
  # Build full file path
  file_path <- file.path(output_dir, paste0(ggplot_name, ".png"))
  
  # Save the plot
  ggsave(
    filename = file_path,
    plot = ggplot_obj + 
      theme(
        panel.background = element_rect(fill = "white", color = NA),
        plot.background = element_rect(fill = "white", color = NA)
      ),
    width = width,
    height = height,
    dpi = dpi,
    units = "in"
  )
  
  message("Plot saved as: ", normalizePath(file_path))
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

##########################################################################
# 1. Two-parts estimates figures analysis
##########################################################################

##########################################################################
# 1.1 Load in By-Cause Two Part Estimate data
##########################################################################

df_master <- read.csv(file = file.path(input_dir, "04.By_cause_inflation_adjusted_aggregated_unfiltered.csv"))
# This is the master analytic table for Aim 1 (most granular: cause, year, type of care, race, age group).

df_sub_age        <- read_csv(file.path(input_dir, "04.By_cause_subtable_by_age.csv"))
# Each row summarizes cost and incremental cost ("delta") by cause (lvl1/lvl2), stratified by age group, across all races, years, type of care, etc.

df_sub_cause      <- read_csv(file.path(input_dir, "04.By_cause_subtable_by_cause.csv"))
# Aggregated by disease cause (lvl1/lvl2) only, pooling across years, ages, race, and type of care. Shows overall average costs and deltas for each disease category.

df_sub_cause_year <- read_csv(file.path(input_dir, "04.By_cause_subtable_by_cause_year.csv"))
# Summarizes cost/deltas by cause and by year, so you can see temporal trends for each disease category.

df_sub_race       <- read_csv(file.path(input_dir, "04.By_cause_subtable_by_race.csv"))
# Summarizes cost and delta by race (and by cause lvl1/lvl2), pooling over years, age, type of care, etc.

df_sub_year       <- read_csv(file.path(input_dir, "04.By_cause_subtable_by_year.csv"))
# Aggregated by year and disease cause, pooling across age, race, and type of care.


##########################################################################
# 1.2 Create Figures
##########################################################################

##########################################################################
# 1.21 Figure 1 - Average cost for general population
##########################################################################

### change colors, and add CI
F1 <- df_master %>%
  group_by(acause_lvl2, cause_name_lvl2) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(mean_cost, total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = reorder(cause_name_lvl2, avg_cost_per_bene), y = avg_cost_per_bene, fill = cause_name_lvl2)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = scales::dollar(avg_cost_per_bene)), hjust = 1.1, color = "white", size = 4) +
  scale_fill_viridis_d(option = "plasma") +
  coord_flip() +
  labs(
    title = "Average Cost per Beneficiary by Disease Category - No HIV / SUD",
    x = "Disease Category", y = "Average Cost (2019 USD)"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(F1, "F1_totalcost")


##########################################################################
# 1.22 Figure 2 - Mean delta comparison by cause (HIV, SUD, HIV+SUD)
##########################################################################


# Prep long -> wide with correct column names
df_F2 <- df_sub_cause %>%
  select(
    acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2,
    mean_delta_hiv_only,  lower_ci_delta_hiv_only,  upper_ci_delta_hiv_only,
    mean_delta_sud_only,  lower_ci_delta_sud_only,  upper_ci_delta_sud_only,
    mean_delta_hiv_sud,   lower_ci_delta_hiv_sud,   upper_ci_delta_hiv_sud
  ) %>%
  pivot_longer(
    cols = c(
      mean_delta_hiv_only,  lower_ci_delta_hiv_only,  upper_ci_delta_hiv_only,
      mean_delta_sud_only,  lower_ci_delta_sud_only,  upper_ci_delta_sud_only,
      mean_delta_hiv_sud,   lower_ci_delta_hiv_sud,   upper_ci_delta_hiv_sud
    ),
    names_to   = c("stat", "scenario"),
    names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv_only|sud_only|hiv_sud)",
    values_to  = "value"
  ) %>%
  pivot_wider(
    names_from  = stat,
    values_from = value
  ) %>%
  mutate(
    scenario = factor(
      scenario,
      levels = c("hiv_only", "sud_only", "hiv_sud"),
      labels = c("HIV", "SUD", "HIV + SUD")
    )
  )

# Plot
F2 <- ggplot(df_F2, aes(x = mean, y = reorder(cause_name_lvl2, mean), fill = scenario)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  geom_errorbar(
    aes(xmin = lower_ci, xmax = upper_ci),
    width = 0.2,
    position = position_dodge(width = 0.8)
  ) +
  geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +
  scale_fill_jama() +
  scale_x_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Mean Delta per Disease Category (HIV, SUD, and HIV + SUD)",
    x = "Change in Cost vs No HIV & No SUD (2019 USD)",
    y = "Disease Category",
    fill = "Scenario"
  ) +
  guides(fill = guide_legend(reverse = TRUE)) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.y  = element_text(size = 10),
    plot.title   = element_text(hjust = 0.5)
  )

save_plot(F2, "F2.Aim1c_delta_costs_HIV_SUD_vs_none.png")




##########################################################################
# 1.23 Figure 3 - Mean delta by comorbidity, facet by race
##########################################################################

##########################################################################
# 1.23 Figure 3 - Mean delta by disease & comorbidity (facet by scenario)
##########################################################################

# quick check
# colnames(df_sub_race)

df_F3 <- df_sub_race %>%
  select(
    acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2, race_cd,
    mean_delta_hiv_only,     lower_ci_delta_hiv_only,     upper_ci_delta_hiv_only,
    mean_delta_sud_only,     lower_ci_delta_sud_only,     upper_ci_delta_sud_only,
    mean_delta_hiv_sud,      lower_ci_delta_hiv_sud,      upper_ci_delta_hiv_sud
  ) %>%
  pivot_longer(
    cols = c(
      mean_delta_hiv_only,  lower_ci_delta_hiv_only,  upper_ci_delta_hiv_only,
      mean_delta_sud_only,  lower_ci_delta_sud_only,  upper_ci_delta_sud_only,
      mean_delta_hiv_sud,   lower_ci_delta_hiv_sud,   upper_ci_delta_hiv_sud
    ),
    names_to   = c("stat", "scenario"),
    names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv_only|sud_only|hiv_sud)",
    values_to  = "value"
  ) %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  mutate(
    scenario = factor(
      scenario,
      levels = c("hiv_only", "sud_only", "hiv_sud"),
      labels = c("HIV", "SUD", "HIV + SUD")
    )
  )

# #(optional) keep top 15 by absolute mean within each scenario for cleaner plot
# df_F3 <- df_F3 %>%
#   group_by(scenario) %>%
#   slice_max(order_by = abs(mean), n = 15, with_ties = FALSE) %>%
#   ungroup()

F3 <- ggplot(
  df_F3,
  aes(x = mean, y = reorder(cause_name_lvl2, mean), fill = race_cd)
) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8), width = 0.7) +
  geom_errorbar(
    aes(xmin = lower_ci, xmax = upper_ci),
    width = 0.2,
    position = position_dodge(width = 0.8)
  ) +
  geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +
  scale_fill_jama() +
  scale_x_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Mean Delta by Disease and Comorbidity (by Race)",
    x = "Change in Cost vs No HIV & No SUD (2019 USD)",
    y = "Level 2 Disease Category",
    fill = "Race"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.y  = element_text(size = 10),
    plot.title   = element_text(hjust = 0.5)
  ) +
  facet_wrap(~ scenario)

save_plot(F3, "F3.Aim1c_delta_costs_by_race_facet.png")



##########################################################################
# 1.24 Figure 4 - Delta HIV over time all race
##########################################################################
colnames(df_master)

df_ribbon <- df_master %>%
  group_by(year_id, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_hiv_only, total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

F4 <- ggplot(
  df_ribbon,
  aes(x = as.integer(year_id), y = mean_delta, color = race_cd, group = race_cd, fill = race_cd)
) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.22, color = NA) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  scale_color_jama() +
  scale_fill_jama() +
  scale_x_continuous(breaks = unique(df_ribbon$year_id)) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Cost Delta by Race Over Time (All Ages)",
    subtitle = "Weighted difference in cost (HIV+ vs HIV−); 95% CI; 2019 USD",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Race", fill = "Race"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(F4, "F4.Aim1c_trends_HIV_SUD_by_race_age.png")

##########################################################################
# 1.25 Figure 5 - HIV delta_cost_by_year_race_cd_facet_race
##########################################################################

F5 <- df_master %>%
  group_by(year_id, age_group_years_start, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_hiv_only, total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(age_group_years_start = factor(age_group_years_start)) %>%  # convert to discrete
  ggplot(aes(x = factor(year_id), 
             y = mean_delta, 
             color = age_group_years_start, 
             group = age_group_years_start, 
             fill = age_group_years_start)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, group = age_group_years_start, fill = age_group_years_start), alpha = 0.2, color = NA) +
  scale_color_jama() +
  scale_fill_jama() +
  scale_y_continuous(labels = scales::dollar_format()) +
  facet_wrap(~ race_cd) +
  labs(
    title = "Cost Delta by Age Group and Race Over Time",
    subtitle = "Weighted difference in cost (HIV+ vs HIV−); 95% CI; 2019 USD",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Age", fill = "Age"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(F5, "F5")


##########################################################################
# 1.26 Figure 6 - Delta MSUD over time all race_cd
##########################################################################
##########################################################################
# 1.26 Figure 6 - Delta SUD over time by race (all ages)
##########################################################################

# Build the summarized frame (note *_sud_only column names)
df_ribbon_msud <- df_master %>%
  group_by(year_id, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_sud_only,       w = total_row_count, na.rm = TRUE),
    lower_ci   = weighted.mean(lower_ci_delta_sud_only,   w = total_row_count, na.rm = TRUE),
    upper_ci   = weighted.mean(upper_ci_delta_sud_only,   w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Make the plot
F6 <- ggplot(
  df_ribbon_msud,
  aes(x = as.integer(year_id), y = mean_delta, color = race_cd, group = race_cd, fill = race_cd)
) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.22, color = NA) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  scale_color_jama() +
  scale_fill_jama() +
  scale_x_continuous(breaks = sort(unique(as.integer(df_ribbon_msud$year_id)))) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Cost Delta Over Time by Race (All Ages)",
    subtitle = "Weighted difference in cost (SUD+ vs SUD−); 95% CI; 2019 USD",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Race", fill = "Race"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Save
save_plot(F6, "F6")


##########################################################################
# 1.27 Figure 7 - Cost Delta by Age and Race Over Time (SUD)
##########################################################################
# ---- Figure 7: Cost Delta by Age and Race Over Time (SUD) ----
F7 <- df_master %>%
  group_by(year_id, age_group_years_start, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_sud_only,     total_row_count, na.rm = TRUE),
    lower_ci   = weighted.mean(lower_ci_delta_sud_only, total_row_count, na.rm = TRUE),
    upper_ci   = weighted.mean(upper_ci_delta_sud_only, total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(age_group_years_start = factor(age_group_years_start,
                                        levels = sort(unique(age_group_years_start)))) %>%
  ggplot(aes(x = factor(year_id),
             y = mean_delta,
             color = age_group_years_start,
             group = age_group_years_start,
             fill  = age_group_years_start)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci,
                  group = age_group_years_start, fill = age_group_years_start),
              alpha = 0.2, color = NA) +
  scale_color_jama() +
  scale_fill_jama() +
  scale_y_continuous(labels = scales::dollar_format()) +
  facet_wrap(~ race_cd) +
  labs(
    title = "Cost Delta by Age and Race Over Time (SUD)",
    subtitle = "Weighted difference in cost (SUD+ vs SUD−); 95% CI; 2019 USD",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Age", fill = "Age"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(F7, "F7")



##########################################################################
# 1.28 Figure 8 - Modeled Incremental Cost (HIV+ vs HIV−) by Year, Race, and Age
##########################################################################

F8 <- df_master %>%
  filter(!is.na(year_id), !is.na(race_cd), !is.na(age_group_years_start)) %>%
  group_by(year_id, race_cd, age_group_years_start) %>%
  summarise(
    delta = weighted.mean(mean_delta_hiv_only,     w = total_row_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(year_id), y = delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  facet_wrap(~ age_group_years_start) +
  scale_color_jama() +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Modeled Incremental Cost (HIV+ vs HIV−) by Year, Race, and Age",
    subtitle = "Weighted difference in cost; faceted by Age",
    x = "Year", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(F8, "F8.Aim1c_modeled_HIV_by_age_race.png")


##########################################################################
# 1.29 Figure 9 - Percent of HIV-Attributable Spending per Disease, by Race
##########################################################################

hiv_percent_race <- df_sub_race %>%
  group_by(cause_name_lvl2) %>%
  mutate(total_hiv = sum(mean_cost_hiv, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(percent_mean_cost_hiv = if_else(total_hiv > 0,
                                         100 * mean_cost_hiv / total_hiv,
                                         NA_real_))

F9 <- ggplot(
  hiv_percent_race,
  aes(x = reorder(cause_name_lvl2, cause_name_lvl2),
      y = percent_mean_cost_hiv,
      fill = factor(race_cd))
) +
  geom_bar(stat = "identity", position = "stack") +
  labs(
    title = "Percent of HIV-Attributable Spending per Disease, by Race",
    x = "Level 2 Disease Category",
    y = "Percentage of Total HIV-Attributable Spending",
    fill = "Race"
  ) +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  scale_fill_jama() +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

save_plot(F9, "F9.Aim1c_percent_HIV_spending_by_race.png")


##########################################################################
# 1.30 (your 1.3) Figure 10 - Percent of HIV Spending per Beneficiary by Age Group
##########################################################################

hiv_percent_age <- df_sub_age %>%
  group_by(cause_name_lvl2) %>%
  mutate(total_hiv = sum(mean_cost_hiv, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(percent_mean_cost_hiv = if_else(total_hiv > 0,
                                         100 * mean_cost_hiv / total_hiv,
                                         NA_real_))

F10 <- ggplot(
  hiv_percent_age,
  aes(x = reorder(cause_name_lvl2, cause_name_lvl2),
      y = percent_mean_cost_hiv,
      fill = factor(age_group_years_start))
) +
  geom_bar(stat = "identity", position = "stack") +
  labs(
    title = "Percent of Spending per Beneficiary, HIV, by Age Group",
    x = "Level 2 Disease Category",
    y = "Percentage of Total Spending per Disease",
    fill = "Age Group (Years)"
  ) +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  scale_fill_jama() +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

save_plot(F10, "F10.Aim1c_percent_HIV_spending_by_age.png")

##########################################################################
# 1.31 Figure 11 - Average Cost per Beneficiary Over Time by Disease (with CI)
##########################################################################


F11 <- df_master %>%
  filter(!is.na(year_id), !is.na(cause_name_lvl2)) %>%
  group_by(year_id, cause_name_lvl2) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, w = total_row_count, na.rm = TRUE),
    lower_ci  = weighted.mean(lower_ci,  w = total_row_count, na.rm = TRUE),
    upper_ci  = weighted.mean(upper_ci,  w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id),
             y = mean_cost,
             color = cause_name_lvl2,
             group = cause_name_lvl2,
             fill = cause_name_lvl2)) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.17, color = NA) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  scale_color_viridis_d(option = "turbo") +
  scale_fill_viridis_d(option = "turbo") +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = sort(unique(df_master$year_id))) +
  labs(
    title = "Average Cost per Beneficiary Over Time by Disease Category",
    x = "Year", y = "Mean Cost (USD)", color = "Disease", fill = "Disease"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom",
    legend.box = "horizontal"
  )


save_plot(F11,
          "F11.Aim1c_mean_costs_over_time_by_disease.png")


##----------------------------------------------------------------
## Load HIV- and SUD-specific analytic tables (05.* files)
##----------------------------------------------------------------

# HIV datasets
df_hiv_master     <- read_csv(file.path(input_dir, "05.HIV_inflation_adjusted_aggregated.csv"))
df_hiv_sub_age    <- read_csv(file.path(input_dir, "05.HIV_subtable_by_age.csv"))
df_hiv_sub_cause_year <- read_csv(file.path(input_dir, "05.HIV_subtable_by_cause_year.csv"))
df_hiv_sub_cause  <- read_csv(file.path(input_dir, "05.HIV_subtable_by_cause.csv"))
df_hiv_sub_race   <- read_csv(file.path(input_dir, "05.HIV_subtable_by_race.csv"))
df_hiv_sub_year   <- read_csv(file.path(input_dir, "05.HIV_subtable_by_year.csv"))

# SUD datasets
df_sud_master     <- read_csv(file.path(input_dir, "05.SUD_inflation_adjusted_aggregated.csv"))
df_sud_sub_age    <- read_csv(file.path(input_dir, "05.SUD_subtable_by_age.csv"))
df_sud_sub_cause_year <- read_csv(file.path(input_dir, "05.SUD_subtable_by_cause_year.csv"))
df_sud_sub_cause  <- read_csv(file.path(input_dir, "05.SUD_subtable_by_cause.csv"))
df_sud_sub_race   <- read_csv(file.path(input_dir, "05.SUD_subtable_by_race.csv"))
df_sud_sub_year   <- read_csv(file.path(input_dir, "05.SUD_subtable_by_year.csv"))


##########################################################################

##########################################################################

##########################################################################
# Figure 13: Mean cost over time — SUD alone vs SUD + HIV (with 95% CIs)
##########################################################################

# 1) Aggregate across race and age by year using total_row_count as weights
df_sud_agg <- df_sud_master %>%
  group_by(year_id) %>%
  summarise(
    mean_cost_sud      = weighted.mean(mean_cost_sud,      w = total_row_count, na.rm = TRUE),
    lower_ci_sud       = weighted.mean(lower_ci_sud,       w = total_row_count, na.rm = TRUE),
    upper_ci_sud       = weighted.mean(upper_ci_sud,       w = total_row_count, na.rm = TRUE),
    mean_cost_hiv_sud  = weighted.mean(mean_cost_hiv_sud,  w = total_row_count, na.rm = TRUE),
    lower_ci_hiv_sud   = weighted.mean(lower_ci_hiv_sud,   w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud   = weighted.mean(upper_ci_hiv_sud,   w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# 2) Reshape to long for plotting
df_sud_plot <- df_sud_agg %>%
  pivot_longer(
    cols = -year_id,
    names_to = c("stat", "group"),
    names_pattern = "(mean_cost|lower_ci|upper_ci)_(sud|hiv_sud)",
    values_to = "value"
  ) %>%
  mutate(stat = recode(stat, mean_cost = "mean", .default = stat)) %>%  # rename for consistency
  pivot_wider(
    names_from  = stat,
    values_from = value
  ) %>%
  mutate(
    group = factor(group,
                   levels = c("sud",      "hiv_sud"),
                   labels = c("SUD", "SUD + HIV"))
  ) %>%
  arrange(group, year_id)

# 3) Plot
plot_sud_vs_hiv_sud <- ggplot(
  df_sud_plot,
  aes(x = as.integer(year_id), y = mean, color = group, fill = group, group = group)
) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.18, color = NA) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  scale_color_jama() +
  scale_fill_jama() +
  scale_x_continuous(breaks = sort(unique(df_sud_plot$year_id))) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Mean Cost per Beneficiary: SUD alone vs SUD + HIV (95% CI)",
    subtitle = "Medicare beneficiaries 65–85; aggregated across race and age; 2019 USD",
    x = "Year",
    y = "Mean Cost per Beneficiary (USD)",
    color = "Group",
    fill  = "Group"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# 4) Save
save_plot(plot_sud_vs_hiv_sud, "F13.Aim1a_SUD_vs_SUD_HIV.png")

##---------------------------------------------------------------
## Figure 14: HIV vs HIV+SUD mean cost over time (with 95% CI)
## Aggregates across race/age/file_type using total_row_count
##---------------------------------------------------------------

df_hiv_year <- df_hiv_master %>%
  group_by(year_id) %>%
  summarise(
    mean_cost_hiv      = weighted.mean(mean_cost_hiv,      total_row_count, na.rm = TRUE),
    lower_ci_hiv       = weighted.mean(lower_ci_hiv,       total_row_count, na.rm = TRUE),
    upper_ci_hiv       = weighted.mean(upper_ci_hiv,       total_row_count, na.rm = TRUE),
    mean_cost_hiv_sud  = weighted.mean(mean_cost_hiv_sud,  total_row_count, na.rm = TRUE),
    lower_ci_hiv_sud   = weighted.mean(lower_ci_hiv_sud,   total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud   = weighted.mean(upper_ci_hiv_sud,   total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Build tidy long data (no pivots -> no list-cols)
df_plot <- bind_rows(
  df_hiv_year %>%
    transmute(
      year_id = as.integer(year_id),
      group   = "HIV",
      mean    = mean_cost_hiv,
      lower   = lower_ci_hiv,
      upper   = upper_ci_hiv
    ),
  df_hiv_year %>%
    transmute(
      year_id = as.integer(year_id),
      group   = "HIV + SUD",
      mean    = mean_cost_hiv_sud,
      lower   = lower_ci_hiv_sud,
      upper   = upper_ci_hiv_sud
    )
)

plot_hiv_vs_hiv_sud <- ggplot(df_plot,
                              aes(x = year_id, y = mean, color = group, fill = group, group = group)
) +
  geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.15, color = NA) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = sort(unique(df_plot$year_id))) +
  scale_color_jama() +
  scale_fill_jama() +
  labs(
    title = "Mean Cost per Beneficiary: HIV vs HIV + SUD (95% CI)",
    subtitle = "Medicare beneficiaries 65–85; aggregated across race and age; 2019 USD",
    x = "Year", y = "Mean Cost per Beneficiary(USD)", color = "Group", fill = "Group"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot_hiv_vs_hiv_sud, "F14.Aim1a_HIV_vs_HIV_SUD.png")

##########################################################################
# Figure 15: Using SS plot spending by disease over years to investigate 2016 drop (per beneficiary)
# Rows = years, columns = diseases (neither, hiv, sud, hiv+sud, so four lines total), all age groups, all races, all sexes combined
##########################################################################

# Load SS data
df_f15 <- read.csv(file = file.path(input_dir, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"))

# Group by summary to get mean_cost for all races all years all age groups
df_f15 <- df_f15 %>%
  group_by(acause_lvl2, year_id, has_hiv, has_sud) %>%
  summarise(
    mean_cost = weighted.mean(avg_cost_per_bene, w = total_unique_bene, na.rm = TRUE)
  )

# Merge with mapping table for cause names
df_f15 <- left_join(x = df_f15, y = df_map, by = "acause_lvl2") %>%
  ungroup() %>%
  select(-c("acause_lvl2", "acause_lvl1", "cause_name_lvl1"))

# Pivot wider
df_f15 <- df_f15 %>%
  mutate(combo = paste0(has_hiv, has_sud)) %>%   # e.g. 00, 01, 10, 11
  pivot_wider(
    id_cols = c(cause_name_lvl2, year_id),                   # what stays as identifier
    names_from = combo,                          # new column names from combos
    values_from = mean_cost                      # values to spread
  ) %>%
  setnames(old = c("cause_name_lvl2", "year_id", "00", "01", "10", "11"),
           new = c("Level 2 Cause", "Year","Mean Cost Summary", "SUD Cost Summary", "HIV Cost Summary", "HIV + SUD Cost Summary"))

# Convert to dollars
for (col in colnames(df_f15)) {
  if (col == "Level 2 Cause" | col == "Year") {
    next
  } else {
    df_f15[[col]] <- dollar(df_f15[[col]])
  }
}

# Pivot long for plot
df_f15_long <- df_f15 %>%
  mutate(
    `Mean Cost Summary`      = parse_number(`Mean Cost Summary`),
    `SUD Cost Summary`       = parse_number(`SUD Cost Summary`),
    `HIV Cost Summary`       = parse_number(`HIV Cost Summary`),
    `HIV + SUD Cost Summary` = parse_number(`HIV + SUD Cost Summary`)
  ) %>%
  pivot_longer(
    cols = c(`Mean Cost Summary`, `SUD Cost Summary`, `HIV Cost Summary`, `HIV + SUD Cost Summary`),
    names_to = "cost_type",
    values_to = "cost_usd"
  ) %>%
  mutate(
    cost_type = recode(cost_type,
                       `Mean Cost Summary`      = "Mean",
                       `SUD Cost Summary`       = "SUD",
                       `HIV Cost Summary`       = "HIV",
                       `HIV + SUD Cost Summary` = "HIV + SUD"
    )
  )

# Make a ggplot and convert to plotly
p15 <- ggplot(df_f15_long, aes(x = Year, y = cost_usd, color = cost_type, group = cost_type)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`Level 2 Cause`), scales = "free_y") +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = "Summary Statistics - Average Spending per beneficiary per disease category, stratified by Cost Type") +
  theme_minimal(base_size = 12)

p15_plotly <- ggplotly(p15, tooltip = c("Year", "cost_usd", "cost_type", "Level 2 Cause"))

saveWidget(p15_plotly, file.path(output_dir, "F15.SS_per_ben.html"), selfcontained = TRUE)



##########################################################################
# Figure 16: Using MODELED results to plot spending by disease over years
# Rows = years, columns = diseases (Mean, HIV, SUD, HIV + SUD lines), 
# all age groups, all races, all sexes combined
##########################################################################

# 1) Group to get weighted means across strata (age/race/sex), by cause & year
df_f16 <- df_master %>%
  filter(!is.na(year_id), !is.na(cause_name_lvl2)) %>%
  group_by(cause_name_lvl2, year_id) %>%
  summarise(
    mean_cost_mean     = weighted.mean(mean_cost,         w = total_row_count, na.rm = TRUE),
    mean_cost_hiv      = weighted.mean(mean_cost_hiv,     w = total_row_count, na.rm = TRUE),
    mean_cost_sud      = weighted.mean(mean_cost_sud,     w = total_row_count, na.rm = TRUE),
    mean_cost_hiv_sud  = weighted.mean(mean_cost_hiv_sud, w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# 2) Rename to match Figure 15 pattern and prepare for pivoting
df_f16 <- df_f16 %>%
  rename(`Level 2 Cause` = cause_name_lvl2,
         Year            = year_id) %>%
  # wide with readable column names like in Fig 15
  mutate(.keep = "all") %>%
  tidyr::pivot_longer(
    cols = c(mean_cost_mean, mean_cost_sud, mean_cost_hiv, mean_cost_hiv_sud),
    names_to = "which",
    values_to = "val"
  ) %>%
  mutate(which = recode(which,
                        "mean_cost_mean"    = "Mean Cost Model",
                        "mean_cost_sud"     = "SUD Cost Model",
                        "mean_cost_hiv"     = "HIV Cost Model",
                        "mean_cost_hiv_sud" = "HIV + SUD Cost Model")) %>%
  tidyr::pivot_wider(
    id_cols = c(`Level 2 Cause`, Year),
    names_from = which,
    values_from = val
  )

# 3) Convert to dollars (string) to mirror Fig 15, then parse back for plotting
for (col in colnames(df_f16)) {
  if (col %in% c("Level 2 Cause", "Year")) next
  df_f16[[col]] <- dollar(df_f16[[col]])
}

# 4) Pivot long for plot (exactly like Fig 15)
df_f16_long <- df_f16 %>%
  mutate(
    `Mean Cost Model`      = parse_number(`Mean Cost Model`),
    `SUD Cost Model`       = parse_number(`SUD Cost Model`),
    `HIV Cost Model`       = parse_number(`HIV Cost Model`),
    `HIV + SUD Cost Model` = parse_number(`HIV + SUD Cost Model`)
  ) %>%
  pivot_longer(
    cols = c(`Mean Cost Model`, `SUD Cost Model`, `HIV Cost Model`, `HIV + SUD Cost Model`),
    names_to = "cost_type",
    values_to = "cost_usd"
  ) %>%
  mutate(
    cost_type = recode(cost_type,
                       `Mean Cost Model`      = "Mean",
                       `SUD Cost Model`       = "SUD",
                       `HIV Cost Model`       = "HIV",
                       `HIV + SUD Cost Model` = "HIV + SUD")
  )

# 5) Plot (same aesthetics as Fig 15; no ribbons)
p16 <- ggplot(df_f16_long, aes(x = Year, y = cost_usd, color = cost_type, group = cost_type)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`Level 2 Cause`), scales = "free_y") +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = "Model Results - Average Spending per beneficiary per disease category, stratified by Cost Type") +
  theme_minimal(base_size = 12)

p16_plotly <- ggplotly(p16, tooltip = c("Year", "cost_usd", "cost_type", "Level 2 Cause"))

saveWidget(p16_plotly, file.path(output_dir, "F16.model_per_ben.html"), selfcontained = TRUE)

##########################################################################
# Figure 17: Using SS plot spending by disease over years to investigate 2016 drop (per encounter)
# Rows = years, columns = diseases (neither, hiv, sud, hiv+sud, so four lines total), all age groups, all races, all sexes combined
##########################################################################

# Load SS data
df_f17 <- read.csv(file = file.path(input_dir, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"))

# Group by summary to get mean_cost for all races all years all age groups
df_f17 <- df_f17 %>%
  group_by(acause_lvl2, year_id, has_hiv, has_sud) %>%
  summarise(
    mean_cost = weighted.mean(avg_cost_per_encounter, w = total_unique_bene, na.rm = TRUE)
  )

# Merge with mapping table for cause names
df_f17 <- left_join(x = df_f17, y = df_map, by = "acause_lvl2") %>%
  ungroup() %>%
  select(-c("acause_lvl2", "acause_lvl1", "cause_name_lvl1"))

# Pivot wider
df_f17 <- df_f17 %>%
  mutate(combo = paste0(has_hiv, has_sud)) %>%   # e.g. 00, 01, 10, 11
  pivot_wider(
    id_cols = c(cause_name_lvl2, year_id),                   # what stays as identifier
    names_from = combo,                          # new column names from combos
    values_from = mean_cost                      # values to spread
  ) %>%
  setnames(old = c("cause_name_lvl2", "year_id", "00", "01", "10", "11"),
           new = c("Level 2 Cause", "Year","Mean Cost Summary", "SUD Cost Summary", "HIV Cost Summary", "HIV + SUD Cost Summary"))

# Convert to dollars
for (col in colnames(df_f17)) {
  if (col == "Level 2 Cause" | col == "Year") {
    next
  } else {
    df_f17[[col]] <- dollar(df_f17[[col]])
  }
}

# Pivot long for plot
df_f17_long <- df_f17 %>%
  mutate(
    `Mean Cost Summary`      = parse_number(`Mean Cost Summary`),
    `SUD Cost Summary`       = parse_number(`SUD Cost Summary`),
    `HIV Cost Summary`       = parse_number(`HIV Cost Summary`),
    `HIV + SUD Cost Summary` = parse_number(`HIV + SUD Cost Summary`)
  ) %>%
  pivot_longer(
    cols = c(`Mean Cost Summary`, `SUD Cost Summary`, `HIV Cost Summary`, `HIV + SUD Cost Summary`),
    names_to = "cost_type",
    values_to = "cost_usd"
  ) %>%
  mutate(
    cost_type = recode(cost_type,
                       `Mean Cost Summary`      = "Mean",
                       `SUD Cost Summary`       = "SUD",
                       `HIV Cost Summary`       = "HIV",
                       `HIV + SUD Cost Summary` = "HIV + SUD"
    )
  )

# Make a ggplot and convert to plotly
p17 <- ggplot(df_f17_long, aes(x = Year, y = cost_usd, color = cost_type, group = cost_type)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`Level 2 Cause`), scales = "free_y") +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = "Summary Statistics - Average Spending per encounter per disease category, stratified by Cost Type") +
  theme_minimal(base_size = 12)

p17_plotly <- ggplotly(p17, tooltip = c("Year", "cost_usd", "cost_type", "Level 2 Cause"))

saveWidget(p17_plotly, file.path(output_dir, "F17.SS_per_encounter.html"), selfcontained = TRUE)

##########################################################################
# Figure 18, 19, 20, 21: Using SS plot spending by disease over years to investigate 2016 drop (per encounter)
# 4 plots:
# 18 - HIV, all toc except IP
# 19 - HIV, IP
# 20 - SUD, all toc except IP
# 21 - SUD, IP
##########################################################################

# Load SS data
df_f18 <- read.csv(file = file.path(input_dir, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"))

# Group by summary to get mean_cost for all races all years all age groups
df_f18 <- df_f18 %>%
  group_by(acause_lvl2, year_id, toc, has_hiv, has_sud) %>%
  summarise(
    mean_cost = weighted.mean(avg_cost_per_encounter, w = total_unique_bene, na.rm = TRUE),
    total_unique_bene = sum(total_unique_bene)
  )

# Merge with mapping table for cause names
df_f18 <- left_join(x = df_f18, y = df_map, by = "acause_lvl2") %>%
  ungroup() %>%
  select(-c("acause_lvl2", "acause_lvl1", "cause_name_lvl1"))

df_f18 <- df_f18 %>%
  mutate(
    combo = paste0(has_hiv, has_sud),
    cost_type = case_when(
      combo == "00" ~ "Mean",
      combo == "01" ~ "SUD",
      combo == "10" ~ "HIV",
      combo == "11" ~ "HIV + SUD"
    )
  ) %>%
  select(-c(combo, has_hiv, has_sud))

# Make a ggplot and convert to plotly

# 18 - HIV, all toc except IP
p18 <- ggplot(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc != "IP"), aes(x = year_id, y = mean_cost, color = toc)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`cause_name_lvl2`), scales = "free_y") +
  scale_x_continuous(breaks = unique(df_f18$year_id)) +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: All except IP, Cost Type: HIV")) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

p18_plotly <- ggplotly(p18, tooltip = c("year_id", "mean_cost", "toc", "Level 2 Cause"))



# 19 - HIV, IP
p19 <- ggplot(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc == "IP"), aes(x = year_id, y = mean_cost, color = toc, group = toc)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`cause_name_lvl2`), scales = "free_y") +
  scale_x_continuous(breaks = unique(df_f18$year_id)) +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: IP, Cost Type: HIV")) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

p19_plotly <- ggplotly(p19, tooltip = c("year_id", "mean_cost", "toc", "Level 2 Cause"))



# 20 - SUD, all toc except IP
p20 <- ggplot(df_f18 %>% filter(cost_type == "SUD") %>% filter(toc != "IP"), aes(x = year_id, y = mean_cost, color = toc, group = toc)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`cause_name_lvl2`), scales = "free_y") +
  scale_x_continuous(breaks = unique(df_f18$year_id)) +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: All Except IP, Cost Type: SUD")) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

p20_plotly <- ggplotly(p20, tooltip = c("year_id", "mean_cost", "toc", "Level 2 Cause"))



# 21 - SUD, IP
p21 <- ggplot(df_f18 %>% filter(cost_type == "SUD") %>% filter(toc == "IP"), aes(x = year_id, y = mean_cost, color = toc, group = toc)) +
  geom_line() +
  geom_point(size = 1) +
  facet_wrap(vars(`cause_name_lvl2`), scales = "free_y") +
  scale_x_continuous(breaks = unique(df_f18$year_id)) +
  scale_y_continuous(labels = scales::dollar) +
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: IP, Cost Type: SUD")) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

p21_plotly <- ggplotly(p21, tooltip = c("year_id", "mean_cost", "toc", "Level 2 Cause"))


# Save Plots
saveWidget(p18_plotly, file.path(output_dir, "F18.SS_per_enc_hiv_all_no_ip.html"), selfcontained = TRUE)
saveWidget(p19_plotly, file.path(output_dir, "F19.SS_per_enc_hiv_ip.html"), selfcontained = TRUE)
saveWidget(p20_plotly, file.path(output_dir, "F20.SS_per_enc_sud_all_no_ip.html"), selfcontained = TRUE)
saveWidget(p21_plotly, file.path(output_dir, "F21.SS_per_enc_sud_ip.html"), selfcontained = TRUE)


# # Testing (safe to delete)
# 
# # HIV, no IP
# p_test <- ggplot(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc != "IP") %>% filter(cause_name_lvl2 == "HIV/AIDS"),
#                  aes(x = year_id, y = mean_cost, color = toc)) +
#   geom_line() +
#   geom_point(aes(size = total_unique_bene), alpha = 0.7) +
#   scale_x_continuous(breaks = unique(df_f18$year_id)) +
#   scale_y_continuous(labels = scales::dollar) +
#   labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: All except IP, Cost Type: HIV")) +
#   theme_minimal(base_size = 12) +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))
# 
# p_test_plotly <- ggplotly(p_test, tooltip = c("year_id", "mean_cost", "toc", "total_unique_bene"))
# 
# p_test_plotly
# 
# # HIV, only IP
# View(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc == "IP") %>% filter(cause_name_lvl2 == "HIV/AIDS"))
# 
# p_test <- ggplot(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc == "IP") %>% filter(cause_name_lvl2 == "HIV/AIDS"),
#                  aes(x = year_id, y = mean_cost, color = toc)) +
#   geom_line() +
#   geom_point(aes(size = total_unique_bene), alpha = 0.7) +
#   scale_x_continuous(breaks = unique(df_f18$year_id)) +
#   scale_y_continuous(labels = scales::dollar) +
#   scale_size_continuous(
#     name = "Beneficiaries",
#     range = c(2, 12)   # min and max dot size in mm
#   ) +
#   labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: IP, Cost Type: HIV")) +
#   theme_minimal(base_size = 12) +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))
# 
# p_test_plotly <- ggplotly(p_test, tooltip = c("year_id", "mean_cost", "toc", "total_unique_bene"))
# 
# p_test_plotly
# 
# # Load SS data
# df_ss <- read.csv(file = file.path(input_dir, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"))
# 
# df_ss <- df_ss %>% filter(acause_lvl2 == "hiv")
# 
# 
# # Testing
# p_test <- ggplot(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc == "IP") %>% filter(cause_name_lvl2 == "HIV/AIDS"),
#                  aes(x = year_id, y = mean_cost, color = toc)) +
#   geom_line() +
#   geom_point(aes(size = total_unique_bene), alpha = 0.7) +
#   scale_x_continuous(breaks = unique(df_f18$year_id)) +
#   scale_y_continuous(labels = scales::dollar) +
#   scale_size_continuous(
#     name = "Beneficiaries",
#     range = c(2, 12)   # min and max dot size in mm
#   ) +
#   labs(x = "Year", y = "Cost (USD)", color = "Cost Type", title = paste0("Summary Statistics - Spending per encounter, Type of care: IP, Cost Type: HIV")) +
#   theme_minimal(base_size = 12) +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1))
# 
# p_test_plotly <- ggplotly(p_test, tooltip = c("year_id", "mean_cost", "toc", "total_unique_bene"))
# p_test_plotly
# saveWidget(p_test_plotly, file.path(output_dir, "HIV IP over time.html"), selfcontained = TRUE)
# 
# View(df_f18 %>% filter(cost_type == "HIV") %>% filter(toc == "IP") %>% filter(cause_name_lvl2 == "HIV/AIDS"))
# View(df_f18 %>% filter(toc == "IP") %>% filter(cause_name_lvl2 == "HIV/AIDS"))








# NEED TO MAKE THESE INTO OFFICIAL FIGURES
# Unique beneficiaries per year by type of care
df_test <- df_f18 %>%
  group_by(year_id, toc, cause_name_lvl2) %>%
  summarize(total_unique_bene = sum(total_unique_bene))

cause_filter <- "HIV/AIDS"

p_test <- ggplot(df_test %>% filter(cause_name_lvl2 == cause_filter), aes(x = year_id, y = total_unique_bene, color = toc)) +
  geom_line() +
  geom_point(aes(size = total_unique_bene), alpha = 0.7) +
  labs(title = paste0("Total Unique Beneficiares - ", cause_filter))

p_test_plotly <- ggplotly(p_test, tooltip = c("year_id", "toc", "total_unique_bene"))
saveWidget(p_test_plotly, file.path(output_dir, "unique_bene_hiv.html"), selfcontained = TRUE)

# Avg cost per encounter per year by type of care ########## Interesting plot!
# Load SS data
df_ss <- read.csv(file = file.path(input_dir, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"))

df_ss <- df_ss %>% filter(acause_lvl2 == "hiv")

df_test <- df_ss %>%
  group_by(year_id, toc, acause_lvl2, has_sud) %>%
  summarize(avg_encounters_per_bene = weighted.mean(avg_encounters_per_bene, total_unique_bene),
            avg_cost_per_encounter = weighted.mean(avg_cost_per_encounter, total_unique_bene),
            total_unique_bene = sum(total_unique_bene)
            )

# NO IP
p_test <- ggplot(df_test %>% filter(toc != "IP") %>% filter(has_sud == 0), aes(x = year_id, y = avg_encounters_per_bene, color = toc)) +
  geom_line() +
  geom_point(aes(size = avg_cost_per_encounter), alpha = 0.7) +
  scale_size_continuous(
    name = "Beneficiaries",
    range = c(2, 12)   # min and max dot size in mm
  ) +
  labs(title = paste0("Avg Encounters per Bene - Cause Name: HIV NO COMBO, size represents avg cost per encounter, No IP"))
#+facet_wrap(~toc)

p_test_plotly <- ggplotly(p_test, tooltip = c("year_id", "toc", "avg_encounters_per_bene", "avg_cost_per_encounter"))
saveWidget(p_test_plotly, file.path(output_dir, "avg_encounters_per_bene_hiv_no_ip.html"), selfcontained = TRUE)


# ONLY IP
p_test <- ggplot(df_test %>% filter(toc == "IP") %>% filter(has_sud == 0), aes(x = year_id, y = avg_encounters_per_bene, color = toc)) +
  geom_line() +
  geom_point(aes(size = avg_cost_per_encounter), alpha = 0.7) +
  scale_size_continuous(
    name = "Beneficiaries",
    range = c(2, 12)   # min and max dot size in mm
  ) +
  labs(title = paste0("Avg Encounters per Bene - Cause Name: HIV NO COMBO, size represents avg cost per encounter, Only IP"))
#+facet_wrap(~toc)

p_test_plotly <- ggplotly(p_test, tooltip = c("year_id", "toc", "avg_encounters_per_bene", "avg_cost_per_encounter"))

saveWidget(p_test_plotly, file.path(output_dir, "avg_encounters_per_bene_hiv_ip.html"), selfcontained = TRUE)

# TEST FIGURE 14-B PLOTTING MODEL DATA FOR HIV / HIV + SUD OVER TIME AGAINST SUMMARIZED DATA

## =========================
## HIV vs HIV+SUD — Model vs Summary + IP share + TOC shares (self-contained)
## =========================


## ------------------------------------------------------------
## Load inputs if not present in session
## (expects `input_dir` defined in your script; if not, set it)
## ------------------------------------------------------------
if (!exists("input_dir")) {
  # <- set this if you're running standalone
  # input_dir <- "/path/to/05.Aggregation_Summary/bested"  # example
  stop("`input_dir` not found. Please set `input_dir` to your CSV folder before running.")
}

# Model tables (05.*)
if (!exists("df_hiv_master")) {
  df_hiv_master <- read_csv(file.path(input_dir, "05.HIV_inflation_adjusted_aggregated.csv"),
                            show_col_types = FALSE)
}

# Summary statistics table (01.*)
if (!exists("df_ss")) {
  df_ss <- read_csv(file.path(input_dir, "01.Summary_Statistics_inflation_adjusted_aggregated.csv"),
                    show_col_types = FALSE)
}

## ------------------------------------------------------------
## MODEL DATA (aggregate across race/age using row counts as weights)
## ------------------------------------------------------------
df_hiv_model <- df_hiv_master %>%
  group_by(year_id) %>%
  summarise(
    mean_cost_hiv      = weighted.mean(mean_cost_hiv,      w = total_row_count, na.rm = TRUE),
    lower_ci_hiv       = weighted.mean(lower_ci_hiv,       w = total_row_count, na.rm = TRUE),
    upper_ci_hiv       = weighted.mean(upper_ci_hiv,       w = total_row_count, na.rm = TRUE),
    mean_cost_hiv_sud  = weighted.mean(mean_cost_hiv_sud,  w = total_row_count, na.rm = TRUE),
    lower_ci_hiv_sud   = weighted.mean(lower_ci_hiv_sud,   w = total_row_count, na.rm = TRUE),
    upper_ci_hiv_sud   = weighted.mean(upper_ci_hiv_sud,   w = total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

# Tidy model frame with mean + CI per group
df_plot <- bind_rows(
  df_hiv_model %>%
    transmute(year_id = as.integer(year_id),
              group   = "HIV",
              mean    = mean_cost_hiv,
              lower   = lower_ci_hiv,
              upper   = upper_ci_hiv),
  df_hiv_model %>%
    transmute(year_id = as.integer(year_id),
              group   = "HIV + SUD",
              mean    = mean_cost_hiv_sud,
              lower   = lower_ci_hiv_sud,
              upper   = upper_ci_hiv_sud)
)

## ------------------------------------------------------------
## SUMMARY STATS (SS) data in the same tidy shape (means only)
## ------------------------------------------------------------
df_hiv_ss <- df_ss %>%
  filter(acause_lvl2 == "hiv") %>%
  group_by(acause_lvl2, year_id, has_hiv, has_sud) %>%
  summarise(mean_cost = weighted.mean(avg_cost_per_bene, w = total_unique_bene, na.rm = TRUE),
            .groups = "drop")

df_hiv_ss <- df_hiv_ss %>%
  mutate(cost_type = case_when(
    has_hiv == 1 & has_sud == 0 ~ "HIV",
    has_hiv == 1 & has_sud == 1 ~ "HIV + SUD",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(cost_type)) %>%
  select(year_id, mean_cost, cost_type)

## ------------------------------------------------------------
## Nice combined plot: Model vs Summary (same color by condition,
## source distinguished by linetype/shape; model has CI ribbons)
## ------------------------------------------------------------
df_model <- df_plot %>%
  mutate(source = "Model",
         group  = factor(group, levels = c("HIV", "HIV + SUD")))

df_ss_line <- df_hiv_ss %>%
  mutate(source = "Summary",
         group  = factor(cost_type, levels = c("HIV", "HIV + SUD"))) %>%
  transmute(year_id = as.integer(year_id), group, source, mean = mean_cost)

cond_cols <- c("HIV" = "#2C7BB6", "HIV + SUD" = "#D7191C")
src_lty   <- c("Model" = "solid", "Summary" = "22")
src_shp   <- c("Model" = 16,      "Summary" = 17)

hiv_model_summary <- ggplot() +
  geom_ribbon(
    data = df_model,
    aes(x = year_id, ymin = lower, ymax = upper, fill = group),
    alpha = 0.15, color = NA
  ) +
  geom_line(
    data = df_model,
    aes(x = year_id, y = mean, color = group, linetype = source),
    linewidth = 1.1
  ) +
  geom_point(
    data = df_model,
    aes(x = year_id, y = mean, color = group, shape = source),
    size = 2.5
  ) +
  geom_line(
    data = df_ss_line,
    aes(x = year_id, y = mean, color = group, linetype = source),
    linewidth = 1.05
  ) +
  geom_point(
    data = df_ss_line,
    aes(x = year_id, y = mean, color = group, shape = source),
    size = 2.5
  ) +
  scale_color_manual(values = cond_cols, name = "Condition") +
  scale_fill_manual(values  = cond_cols, guide = "none") +
  scale_linetype_manual(values = src_lty, name = "Source") +
  scale_shape_manual(values   = src_shp, name = "Source") +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = sort(unique(df_model$year_id))) +
  labs(
    title    = "HIV vs HIV + SUD — Modeled vs Summary Over Time",
    subtitle = "Solid lines = Model (95% CI ribbons). Dashed = Summary statistics.",
    x = "Year", y = "Mean cost per beneficiary (2019 USD)"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    legend.position = "bottom",
    legend.box = "vertical",
    axis.text.x = element_text(angle = 45, hjust = 1),
    panel.grid.minor = element_blank()
  )

save_plot(hiv_model_summary, "hiv_model_summary_over_time")

## ------------------------------------------------------------
## Figure: Share of IP encounters among all HIV encounters over time
## encounters = avg_encounters_per_bene * total_unique_bene
## ------------------------------------------------------------
df_hiv_ip_prop <- df_ss %>%
  filter(acause_lvl2 == "hiv", has_hiv == 1) %>%
  mutate(encounters = avg_encounters_per_bene * total_unique_bene) %>%
  group_by(year_id, toc) %>%
  summarise(encounters = sum(encounters, na.rm = TRUE), .groups = "drop") %>%
  group_by(year_id) %>%
  mutate(total_encounters_all_toc = sum(encounters, na.rm = TRUE),
         prop_ip = if_else(toc == "IP" & total_encounters_all_toc > 0,
                           encounters / total_encounters_all_toc, NA_real_)) %>%
  ungroup() %>%
  filter(toc == "IP") %>%
  arrange(year_id)

p_hiv_ip_share <- ggplot(df_hiv_ip_prop,
                         aes(x = as.integer(year_id), y = prop_ip, group = 1)) +
  geom_line(color = "#2b8cbe", linewidth = 1.2) +
  geom_point(color = "#2b8cbe", size = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_x_continuous(breaks = sort(unique(as.integer(df_hiv_ip_prop$year_id)))) +
  labs(
    title = "Share of IP Encounters Among All HIV Encounters Over Time",
    subtitle = "SS data; encounters = avg_encounters_per_bene × total_unique_bene",
    x = "Year", y = "IP Share of HIV Encounters"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(p_hiv_ip_share, "FXX.HIV_IP_share_over_time")

## ------------------------------------------------------------
## Figure: Share of encounters by Type of Care among HIV encounters
## (facet each TOC; independent y scales)
## ------------------------------------------------------------
df_hiv_toc_share <- df_ss %>%
  filter(acause_lvl2 == "hiv", has_hiv == 1) %>%
  mutate(encounters = avg_encounters_per_bene * total_unique_bene) %>%
  group_by(year_id, toc) %>%
  summarise(encounters = sum(encounters, na.rm = TRUE), .groups = "drop_last") %>%
  group_by(year_id) %>%
  mutate(total = sum(encounters, na.rm = TRUE),
         prop_toc = if_else(total > 0, encounters / total, NA_real_)) %>%
  ungroup()

p_toc_facets <- ggplot(df_hiv_toc_share,
                       aes(x = as.integer(year_id), y = prop_toc, group = 1)) +
  geom_line(color = "#2b8cbe", linewidth = 1) +
  geom_point(color = "#2b8cbe", size = 2) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_x_continuous(breaks = sort(unique(as.integer(df_hiv_toc_share$year_id)))) +
  labs(
    title = "Share of Encounters by Type of Care among HIV Encounters",
    subtitle = "SS data; each facet has its own y-scale",
    x = "Year", y = "Share of Encounters"
  ) +
  facet_wrap(~ toc, scales = "free_y", ncol = 3) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Optional: interactive
p_toc_facets_plotly <- ggplotly(p_toc_facets, tooltip = c("x", "y"))

# Save static PNG
save_plot(p_toc_facets, "FXXd.HIV_encounter_share_by_TOC_facets")


## ------------------------------------------------------------
## Figure: Stacked area — share of encounters by TOC among HIV encounters
## ------------------------------------------------------------

df_hiv_toc_share <- df_ss %>%
  filter(acause_lvl2 == "hiv", has_hiv == 1) %>%
  mutate(encounters = avg_encounters_per_bene * total_unique_bene) %>%
  group_by(year_id, toc) %>%
  summarise(encounters = sum(encounters, na.rm = TRUE), .groups = "drop_last") %>%
  group_by(year_id) %>%
  mutate(total = sum(encounters, na.rm = TRUE),
         prop_toc = if_else(total > 0, encounters / total, NA_real_)) %>%
  ungroup()

p_hiv_toc_share_area <- ggplot(
  df_hiv_toc_share,
  aes(x = as.integer(year_id), y = prop_toc, fill = toc)
) +
  geom_area(alpha = 0.9, color = "white", size = 0.2) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, 1)) +
  scale_x_continuous(breaks = sort(unique(as.integer(df_hiv_toc_share$year_id)))) +
  labs(
    title = "Composition of HIV Encounters by Type of Care Over Time",
    subtitle = "SS data; shares sum to 100% per year",
    x = "Year", y = "Share of Encounters", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(p_hiv_toc_share_area, "FXXc.HIV_encounter_share_by_TOC_area")

# Optional interactive
# p_hiv_toc_share_area_plotly <- ggplotly(
#   p_hiv_toc_share_area,
#   tooltip = c("x", "y", "fill")
# )
# saveWidget(p_hiv_toc_share_area_plotly,
#            file.path(output_dir, "FXXc.HIV_encounter_share_by_TOC_area.html"),
#            selfcontained = TRUE)

