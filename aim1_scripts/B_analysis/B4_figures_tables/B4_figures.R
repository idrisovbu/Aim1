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
pacman::p_load(ggplot2, readr, tidyverse, viridis, scales, ggsci)
library(viridis)

# Set the current date for folder naming
date_today <- format(Sys.time(), "%Y%m%d")

# Detect IHME cluster by checking for /mnt/share/limited_use
if (dir.exists("/mnt/share/limited_use")) {
  # IHME/cluster environment
  base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/"
  input_dir <- file.path(base_dir, "05.Aggregation_Summary", "bested")
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

save_plot(F1, "F1")

##########################################################################
# 1.22 Figure 2 - Mean delta comparison by cause 
##########################################################################

# # data prep - Need to get this section working to add in the HIV + SUD combo
# df_F2 <- df_sub_cause %>%
#   select(acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2,
#          mean_delta_hiv_only_only, lower_ci_delta_hiv_only_only, upper_ci_delta_hiv_only_only,
#          mean_delta_sud_only, lower_ci_delta_sud_only, upper_ci_delta_sud_only,
#          mean_delta_hiv_only_sud, lower_ci_delta_hiv_only_sud, upper_ci_delta_hiv_only_sud) %>%
#   pivot_longer(
#     cols = c(mean_delta_hiv_only_only, mean_delta_sud_only, mean_delta_hiv_only_sud,
#              lower_ci_delta_hiv_only_only, lower_ci_delta_sud_only, lower_ci_delta_hiv_only_sud,
#              upper_ci_delta_hiv_only_only, upper_ci_delta_sud_only, upper_ci_delta_hiv_only_sud),
#     names_to = c("stat", "scenario"),
#     names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv|sud|hiv_sud)",
#     values_to = "value"
#   ) %>%
#   pivot_wider(
#     names_from = stat,
#     values_from = value
#   )

# data prep
df_F2 <- df_sub_cause %>%
  select(acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2,
         mean_delta_hiv_only_only, lower_ci_delta_hiv_only_only, upper_ci_delta_hiv_only_only,
         mean_delta_sud_only, lower_ci_delta_sud_only, upper_ci_delta_sud_only) %>%
  pivot_longer(
    cols = c(mean_delta_hiv_only_only, mean_delta_sud_only,
             lower_ci_delta_hiv_only_only, lower_ci_delta_sud_only,
             upper_ci_delta_hiv_only_only, upper_ci_delta_sud_only),
    names_to = c("stat", "scenario"),
    names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv|sud)",
    values_to = "value"
  ) %>%
  pivot_wider(
    names_from = stat,
    values_from = value
  )

df_F2$scenario <- factor(df_F2$scenario, levels = c("hiv", "sud"), labels = c("HIV", "Substance Use"))

# plot 
F2 <- ggplot(df_F2, aes(x = mean, y = reorder(cause_name_lvl2, mean), fill = scenario)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8), width = 0.7) +
  geom_errorbar(
    aes(xmin = lower_ci, xmax = upper_ci),
    width = 0.2,
    position = position_dodge(width = 0.8)
  ) +
  geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +
  scale_fill_jama() +
  scale_x_continuous(
    breaks = c(-2500, 0, 2500, 7500, 12500),
    labels = scales::dollar_format()
  ) +
  labs(
    title = "Mean Delta per Disease Category (HIV and SUD Only)",
    x = "All years, care types and races (Change in Cost compared to No HIV & no SUD, 2019 USD)",
    y = "Disease Category",
    fill = "Scenario"
  ) +
  guides(fill = guide_legend(reverse = TRUE)) +   # <---- This flips legend order!
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.y = element_text(size = 10),
    plot.title = element_text(hjust = 0.5)
  )

save_plot(F2, "F2")


##########################################################################
# 1.23 Figure 3 - Mean delta by comorbidity, facet by race
##########################################################################

#data prep
df_F3 <- df_sub_race %>%
  select(acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2, race_cd,
         mean_delta_hiv_only_only, lower_ci_delta_hiv_only_only, upper_ci_delta_hiv_only_only,
         mean_delta_sud_only, lower_ci_delta_sud_only, upper_ci_delta_sud_only) %>%
  pivot_longer(
    cols = c(mean_delta_hiv_only_only, mean_delta_sud_only,
             lower_ci_delta_hiv_only_only, lower_ci_delta_sud_only,
             upper_ci_delta_hiv_only_only, upper_ci_delta_sud_only),
    names_to = c("stat", "scenario"),
    names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv|sud)",
    values_to = "value"
  ) %>%
  pivot_wider(
    names_from = stat,
    values_from = value
  )

df_F3$scenario <- factor(df_F3$scenario, 
                                levels = c("hiv", "sud"), 
                                labels = c("HIV", "SUD"))
labs(
  title = "Mean Delta by Disease and Comorbidity (by Race)",
  subtitle = "All years, all ages",
  x = "Mean Delta (Change in Cost compared to No HIV & SUD, 2019 USD)",
  y = "Level 2 Disease Category",
  fill = "Race"
)

## plot
### need to do top 15 only 

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
  scale_x_continuous(
    breaks = c(-2500, 0, 2500, 7500, 12500),
    labels = scales::dollar_format()
  ) +
  labs(
    title = "Mean Delta by Disease and Comorbidity (by Race)",
    x = "Mean Delta (Change in Cost compared to No HIV & SUD, 2019 USD)",
    y = "Level 2 Disease Category",
    fill = "Race"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.y = element_text(size = 10),
    plot.title = element_text(hjust = 0.5)
  ) +
  facet_wrap(~scenario)

save_plot(F3, "F3")

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

save_plot(F4, "F4")

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
df_ribbon_msud <- df_master %>%
  group_by(year_id, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_sud, total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta_sud, total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta_sud, total_row_count, na.rm = TRUE),
    .groups = "drop"
  )

F6 <- ggplot(
  df_ribbon_msud,
  aes(x = as.integer(year_id), y = mean_delta, color = race_cd, group = race_cd, fill = race_cd)
) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.22, color = NA) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  scale_color_jama() +
  scale_fill_jama() +
  scale_x_continuous(breaks = unique(df_ribbon_msud$year_id)) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Cost Delta Over Time by Races (All Races, All Ages)",
    subtitle = "Weighted difference in cost (SUD+ vs SUD−); 95% CI; 2019 USD",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Race", fill = "Race"
  ) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(F6, "F6")

##########################################################################
# 1.27 Figure 7 - Cost Delta by Age and Race Over Time (SUD)
##########################################################################
F7 <- df_master %>%
  group_by(year_id, age_group_years_start, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_sud, total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta_sud, total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta_sud, total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(age_group_years_start = factor(age_group_years_start, 
                                        levels = sort(unique(age_group_years_start)))) %>%  # ensures numeric order
  ggplot(aes(x = factor(year_id), 
             y = mean_delta, 
             color = age_group_years_start, 
             group = age_group_years_start, 
             fill = age_group_years_start)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, 
                  group = age_group_years_start, 
                  fill = age_group_years_start), 
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
  group_by(year_id, race_cd, age_group_years_start) %>%
  summarise(
    delta = weighted.mean(mean_delta_hiv_only, total_row_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, total_row_count, na.rm = TRUE),
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

save_plot(F8, "F8")


##########################################################################
# 1.29 Figure 9 - Carpet Plot - Percent of HIV-Attributable Spending per Disease, by Race
##########################################################################
hiv_percent_race <- ggplot(
  data = df_sub_race %>%
    group_by(cause_name_lvl2) %>%
    mutate(percent_mean_cost_hiv = mean_cost_hiv / sum(mean_cost_hiv, na.rm = TRUE) * 100) %>%
    ungroup(),
  aes(
    x = reorder(cause_name_lvl2, cause_name_lvl2),
    y = percent_mean_cost_hiv,
    fill = factor(race_cd)  # or use  recoded variable for prettier legend
  )
) +
  geom_bar(stat = "identity", position = "stack") +
  labs(
    title = "Percent of HIV-Attributable Spending per Disease, by Race",
    x = "Level 2 Disease Category",
    y = "Percentage of Total HIV-Attributable Spending",
    fill = "Race"
  ) +
  scale_y_continuous(labels = percent_format(scale = 1)) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "plain", hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.x = element_text(angle = 45, hjust = 1)
  ) +
  scale_fill_jama()  # Use JAMA palette for race

# Output
save_plot(hiv_percent_race, "hiv_percent_race")

##########################################################################
# 1.3 Figure 10 - Carpet Plot - Percent of Spending per Beneficiary, HIV, by Age Group
##########################################################################
hiv_percent_age <- ggplot(
  data = df_sub_age %>%
    group_by(cause_name_lvl2) %>%
    mutate(percent_mean_cost_hiv = mean_cost_hiv / sum(mean_cost_hiv, na.rm = TRUE) * 100) %>%
    ungroup(),
  aes(
    x = reorder(cause_name_lvl2, cause_name_lvl2),
    y = percent_mean_cost_hiv,
    fill = factor(age_group_years_start)
  )
) +
  geom_bar(stat = "identity", position = "stack") +
  labs(
    title = "Percent of Spending per Beneficiary, HIV, by Age Group",
    x = "Level 2 Disease Category",
    y = "Percentage of Total Spending per Disease",
    fill = "Age Group (Years)"
  ) +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +  # Format y-axis as %
  scale_fill_jama() +  # <- JAMA palette here
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "plain", hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

# Output
save_plot(hiv_percent_age, "hiv_percent_age")

##########################################################################
# 1.31 Figure 11 - Average Cost per Beneficiary Over Time by Disease Category (with CI)
##########################################################################
plot_mean_cost_by_disease_over_time <- df_master %>%
  group_by(year_id, cause_name_lvl2) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = mean_cost, color = cause_name_lvl2, group = cause_name_lvl2, fill = cause_name_lvl2)) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.17, color = NA) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  scale_color_viridis_d(option = "turbo") +
  scale_fill_viridis_d(option = "turbo") +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = unique(df_master$year_id)) +
  labs(
    title = "Average Cost per Beneficiary Over Time by Disease Category",
    x = "Year", y = "Mean Cost (USD)", color = "Disease", fill = "Disease"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot_mean_cost_by_disease_over_time, "mean_cost_over_time_by_disease_ribbons_viridis")

