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
  date_of_input <- "20250914" # last run on 20250914
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
    title = "Average Cost per Beneficiary by Disease Category",
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
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type") +
  theme_minimal(base_size = 12)

p15_plotly <- ggplotly(p15, tooltip = c("Year", "cost_usd", "cost_type", "Level 2 Cause"))

saveWidget(p15_plotly, file.path(output_dir, "F15.SS_2016_per_bene_drop.html"), selfcontained = TRUE)



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
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type") +
  theme_minimal(base_size = 12)

p16_plotly <- ggplotly(p16, tooltip = c("Year", "cost_usd", "cost_type", "Level 2 Cause"))

saveWidget(p16_plotly, file.path(output_dir, "F16.MODELED_2016_drop.html"), selfcontained = TRUE)

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
  labs(x = "Year", y = "Cost (USD)", color = "Cost Type") +
  theme_minimal(base_size = 12)

p17_plotly <- ggplotly(p17, tooltip = c("Year", "cost_usd", "cost_type", "Level 2 Cause"))

saveWidget(p17_plotly, file.path(output_dir, "F17.SS_2016_per_encounter_drop.html"), selfcontained = TRUE)

