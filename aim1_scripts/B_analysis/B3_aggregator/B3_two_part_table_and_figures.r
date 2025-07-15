#B3descriptive_twoPart.R



rm(list = ls())
pacman::p_load(ggsci,dplyr, openxlsx, RMySQL,data.table, ini, DBI, tidyr, readr,writexl, purrr, ggplot2, gridExtra, scales, ggpubr, patchwork, RColorBrewer)



#######
# Base analysis directory
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"

# Use  date 
date_of_input <- "20250624"

# Construct path to Agregators folder
agregators_folder <- file.path(base_dir, date_of_input, "Agregators")

# Read input files from Agregators folder
two_part_by_toc <- fread(file.path(agregators_folder, "weighted_summary_two_part_table_master.csv"))

# Create output folder for tables and figures
output_folder <- file.path(base_dir, date_of_input, "tablesandfigures")
if (!dir.exists(output_folder)) {
  dir.create(output_folder, recursive = TRUE)
}

#####

#function to save plots
save_plot <- function(plot_obj, filename) {
  ggsave(filename = file.path(output_folder, filename),
         plot = plot_obj, width = 9, height = 6, dpi = 300,
         device = "jpeg")
}

jama_palette <- pal_jama("default")(7)


######Use master table to create subtables

# Columns to *always* include in the grouping
cause_cols <- c("acause_lvl1", "cause_name_lvl1", "acause_lvl2", "cause_name_lvl2")

# Helper function as before
weighted_mean_all <- function(df, group_cols, value_cols, weight_col) {
  df %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(across(all_of(value_cols), 
                     ~ weighted.mean(.x, get(weight_col), na.rm = TRUE),
                     .names = "{.col}"
    ),
    total_bin_count = sum(.data[[weight_col]], na.rm = TRUE),
    .groups = "drop")
}

value_cols <- c(
  "mean_cost","lower_ci","upper_ci",
  "mean_cost_hiv","lower_ci_hiv","upper_ci_hiv",
  "mean_cost_sud","lower_ci_sud","upper_ci_sud",
  "mean_cost_hiv_sud","lower_ci_hiv_sud","upper_ci_hiv_sud",
  "mean_delta_hiv","lower_ci_delta_hiv","upper_ci_delta_hiv",
  "mean_delta_sud","lower_ci_delta_sud","upper_ci_delta_sud",
  "mean_delta_hiv_sud","lower_ci_delta_hiv_sud","upper_ci_delta_hiv_sud"
)

# By cause (Level 2 + Level 1)
by_cause <- weighted_mean_all(two_part_by_toc, cause_cols, value_cols, "total_bin_count")
write_csv(by_cause, file.path(output_folder, "subtable_by_cause.csv"))

# By year (preserving both cause levels)
by_year <- weighted_mean_all(two_part_by_toc, c(cause_cols, "year_id"), value_cols, "total_bin_count")
write_csv(by_year, file.path(output_folder, "subtable_by_year.csv"))

# By type of care (preserving both cause levels)
by_toc <- weighted_mean_all(two_part_by_toc, c(cause_cols, "toc"), value_cols, "total_bin_count")
write_csv(by_toc, file.path(output_folder, "subtable_by_toc.csv"))

# By race (preserving both cause levels)
by_race <- weighted_mean_all(two_part_by_toc, c(cause_cols, "race_cd"), value_cols, "total_bin_count")
write_csv(by_race, file.path(output_folder, "subtable_by_race.csv"))

# By age group (preserving both cause levels)
by_age <- weighted_mean_all(two_part_by_toc, c(cause_cols, "age_group_years_start"), value_cols, "total_bin_count")
write_csv(by_age, file.path(output_folder, "subtable_by_age.csv"))

# Example: By cause and year (joint stratification, cause levels always present)
by_cause_year <- weighted_mean_all(two_part_by_toc, c(cause_cols, "year_id"), value_cols, "total_bin_count")
write_csv(by_cause_year, file.path(output_folder, "subtable_by_cause_year.csv"))

cat("All subtables (with cause levels preserved) have been saved to CSV in ", output_folder, "\n")

######
# BELOW IS AN OLD CODE THAT NEEDS TO BE REWRITTEN 
#
########




# Summary Table 1 - by Level 2 and Level 1 causes
summary_lvl2 <- two_part_all_toc %>%
  group_by(acause_lvl2, cause_name_lvl2) %>%
  summarise(
    mean_cost = mean(mean_cost, na.rm = TRUE),
    mean_cost_hiv = mean(mean_cost_hiv, na.rm = TRUE),
    mean_cost_sud = mean(mean_cost_sud, na.rm = TRUE),
    mean_cost_hiv_sud = mean(mean_cost_hiv_sud, na.rm = TRUE),
    total_bins = sum(total_bin_count, na.rm = TRUE),
    .groups = "drop"
  )

summary_lvl1 <- two_part_all_toc %>%
  group_by(acause_lvl1, cause_name_lvl1) %>%
  summarise(
    mean_cost = mean(mean_cost, na.rm = TRUE),
    mean_cost_hiv = mean(mean_cost_hiv, na.rm = TRUE),
    mean_cost_sud = mean(mean_cost_sud, na.rm = TRUE),
    mean_cost_hiv_sud = mean(mean_cost_hiv_sud, na.rm = TRUE),
    total_bins = sum(total_bin_count, na.rm = TRUE),
    .groups = "drop"
  )


# Summary by Year and Level 2 causes
summary_year_lvl2 <- two_part_all_toc %>%
  group_by(year_id, acause_lvl2, cause_name_lvl2) %>%
  summarise(
    mean_cost = mean(mean_cost, na.rm = TRUE),
    mean_cost_hiv = mean(mean_cost_hiv, na.rm = TRUE),
    mean_cost_sud = mean(mean_cost_sud, na.rm = TRUE),
    mean_cost_hiv_sud = mean(mean_cost_hiv_sud, na.rm = TRUE),
    total_bins = sum(total_bin_count, na.rm = TRUE),
    .groups = "drop"
  )

# Summary by Year and Level 1 causes
summary_year_lvl1 <- two_part_all_toc %>%
  group_by(year_id, acause_lvl1, cause_name_lvl1) %>%
  summarise(
    mean_cost = mean(mean_cost, na.rm = TRUE),
    mean_cost_hiv = mean(mean_cost_hiv, na.rm = TRUE),
    mean_cost_sud = mean(mean_cost_sud, na.rm = TRUE),
    mean_cost_hiv_sud = mean(mean_cost_hiv_sud, na.rm = TRUE),
    total_bins = sum(total_bin_count, na.rm = TRUE),
    .groups = "drop"
  )


# Save all four summary tables into one Excel file
write.xlsx(
  list(
    "Level 2 (All Years)"     = summary_lvl2,
    "Level 1 (All Years)"     = summary_lvl1,
    "Level 2 by Year"         = summary_year_lvl2,
    "Level 1 by Year"         = summary_year_lvl1
  ),
  file = file.path(output_folder, "table1_modeled_summary_costs.xlsx")
)

cat("All summary tables saved to 'table1_modeled_summary_costs.xlsx'\n")


#######
library(ggplot2)

# Plot: Mean cost by year and cause level 1
ggplot(summary_year_lvl1, aes(x = year_id, y = mean_cost, color = cause_name_lvl1, group = cause_name_lvl1)) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  labs(
    title = "Mean Cost by Year and Level 1 Cause",
    x = "Year", y = "Mean Cost (USD)",
    color = "Level 1 Cause"
  ) +
  theme_minimal(base_size = 14) +
  scale_x_continuous(breaks = seq(min(summary_year_lvl1$year_id), max(summary_year_lvl1$year_id), 1))


# ---- ###### ----
##two part model
# ---- ######## ----




# Helper function: summary for all 2x2 combinations (neither, hiv, sud, hiv+sud)
summarize_model_table <- function(df, group_var) {
  df %>%
    group_by({{ group_var }}) %>%
    summarise(
      # Neither HIV nor SUD
      median_cost_neither = median(mean_cost_neither, na.rm = TRUE),
      mean_cost_neither   = mean(mean_cost_neither, na.rm = TRUE),
      ci_range_neither    = paste0(round(median(lower_ci_neither, na.rm = TRUE)), "–", round(median(upper_ci_neither, na.rm = TRUE))),
      
      # HIV only
      median_cost_hiv_only = median(mean_cost_hiv_only, na.rm = TRUE),
      mean_cost_hiv_only   = mean(mean_cost_hiv_only, na.rm = TRUE),
      ci_range_hiv_only    = paste0(round(median(lower_ci_hiv_only, na.rm = TRUE)), "–", round(median(upper_ci_hiv_only, na.rm = TRUE))),
      
      # SUD only
      median_cost_sud_only = median(mean_cost_sud_only, na.rm = TRUE),
      mean_cost_sud_only   = mean(mean_cost_sud_only, na.rm = TRUE),
      ci_range_sud_only    = paste0(round(median(lower_ci_sud_only, na.rm = TRUE)), "–", round(median(upper_ci_sud_only, na.rm = TRUE))),
      
      # Both HIV and SUD
      median_cost_hiv_sud = median(mean_cost_hiv_sud, na.rm = TRUE),
      mean_cost_hiv_sud   = mean(mean_cost_hiv_sud, na.rm = TRUE),
      ci_range_hiv_sud    = paste0(round(median(lower_ci_hiv_sud, na.rm = TRUE)), "–", round(median(upper_ci_hiv_sud, na.rm = TRUE))),
      
      # Deltas
      median_delta_hiv_only   = median(mean_delta_hiv_only, na.rm = TRUE),
      mean_delta_hiv_only     = mean(mean_delta_hiv_only, na.rm = TRUE),
      ci_range_delta_hiv_only = paste0(round(median(lower_ci_delta_hiv_only, na.rm = TRUE)), "–", round(median(upper_ci_delta_hiv_only, na.rm = TRUE))),
      
      median_delta_sud_only   = median(mean_delta_sud_only, na.rm = TRUE),
      mean_delta_sud_only     = mean(mean_delta_sud_only, na.rm = TRUE),
      ci_range_delta_sud_only = paste0(round(median(lower_ci_delta_sud_only, na.rm = TRUE)), "–", round(median(upper_ci_delta_sud_only, na.rm = TRUE))),
      
      median_delta_hiv_sud    = median(mean_delta_hiv_sud, na.rm = TRUE),
      mean_delta_hiv_sud      = mean(mean_delta_hiv_sud, na.rm = TRUE),
      ci_range_delta_hiv_sud  = paste0(round(median(lower_ci_delta_hiv_sud, na.rm = TRUE)), "–", round(median(upper_ci_delta_hiv_sud, na.rm = TRUE))),
      
      total_bins = sum(total_bin_count, na.rm = TRUE),
      .groups = "drop"
    )
}


# Reference the model results
model_df <- two_part_df

# Run modeled summaries
model_table_b <- summarize_model_table(model_df, acause)
model_table_c <- summarize_model_table(model_df, year_id)
model_table_d <- summarize_model_table(model_df, race_cd)
model_table_e <- summarize_model_table(model_df, toc)
model_table_f <- summarize_model_table(model_df, age_group_years_start)

# Name each as a sheet
model_table_list <- list(
  "By Disease" = model_table_b,
  "By Year" = model_table_c,
  "By Race" = model_table_d,
  "By Type of Care" = model_table_e,
  "By Age Group" = model_table_f
)

# Save to Excel file
write_xlsx(model_table_list, path = file.path(output_folder, "table3_modeled_summaries.xlsx"))

cat("Modeled Table 3 summaries saved to Excel.\n")


####



# PLOT 1: HIV+ vs Neither
plot1_delta_hiv_only <- two_part_df %>%
  group_by(year_id, toc) %>%
  summarise(
    delta = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = delta, color = toc, group = toc)) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
  scale_color_manual(values = jama_palette) +
  scale_x_continuous(breaks = seq(min(two_part_df$year_id), max(two_part_df$year_id), 1)) +
  labs(
    title = "Incremental Cost: HIV+ vs Neither by Year and Type of Care",
    x = "Year", y = "Incremental Cost (USD)", color = "Type of Care"
  ) +
  theme_minimal(base_size = 13)
save_plot(plot1_delta_hiv_only, "model_delta_cost_by_year_toc_hiv_only.jpeg")

library(ggsci)      # For pal_jama
library(scales)     # For hue_pal

# Create a palette with as many colors as unique diseases (acause)
n_acause <- length(unique(two_part_df$acause))
# Option 1: Generate via base R colors
acause_palette <- scales::hue_pal()(n_acause)
# Option 2: Extend JAMA palette by repeating if you want consistent theme
# acause_palette <- rep(pal_jama("default")(7), length.out = n_acause)

names(acause_palette) <- sort(unique(two_part_df$acause))

# PLOT 2: HIV+ vs Neither
plot2_delta_hiv_only <- two_part_df %>%
  group_by(acause, race_cd) %>%
  summarise(
    delta = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = race_cd, y = delta, fill = acause)) +
  geom_col(position = position_dodge(width = 0.9), color = "black") +
  geom_errorbar(aes(ymin = lower, ymax = upper),
                position = position_dodge(width = 0.9), width = 0.3) +
  facet_wrap(~ acause) +
  scale_fill_manual(values = acause_palette) +  # <- now enough colors
  labs(
    title = "Modeled Incremental Cost (HIV+ vs Neither) by Race and Disease",
    x = "Race/Ethnicity", y = "Incremental Cost (USD)", fill = "Disease"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot2_delta_hiv_only, "model_delta_cost_by_race_and_disease_hiv_only.jpeg")


# PLOT 3: HIV+ vs Neither
plot3_delta_hiv_only <- two_part_df %>%
  group_by(age_group_years_start, race_cd) %>%
  summarise(
    delta = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(age_group_years_start), y = delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_color_manual(values = jama_palette) +
  labs(
    title = "Incremental Cost (HIV+ vs Neither) by Age Group and Race",
    x = "Age Group", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13)
save_plot(plot3_delta_hiv_only, "model_delta_cost_by_age_and_race_hiv_only.jpeg")


# --- Dynamic palette for races
n_races <- length(unique(two_part_df$race_cd))
race_palette <- scales::hue_pal()(n_races)
names(race_palette) <- sort(unique(two_part_df$race_cd))

plot4_modeled <- two_part_df %>%
  group_by(year_id, race_cd, toc) %>%
  summarise(
    delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(year_id), y = delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  facet_wrap(~ toc) +
  scale_color_manual(values = race_palette) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Modeled Incremental Cost (HIV+ vs HIV−) by Year, Race, and Type of Care",
    subtitle = "Weighted difference in cost; faceted by type of care",
    x = "Year", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot4_modeled, "model_delta_cost_by_year_race_facet_toc.jpeg")


#####
plot4_delta_hiv_only <- two_part_df %>%
  group_by(year_id, race_cd, toc) %>%
  summarise(
    delta = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(year_id), y = delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  facet_wrap(~ toc) +
  scale_color_manual(values = jama_palette) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Modeled Incremental Cost (HIV+ vs Neither) by Year, Race, and Type of Care",
    subtitle = "Weighted difference in cost; faceted by type of care",
    x = "Year", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot4_delta_hiv_only, "model_delta_hiv_only_by_year_race_facet_toc.jpeg")




####
plot5_delta_hiv_only <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id) %>%
  summarise(
    mean  = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = mean)) +
  geom_point(size = 3, color = jama_palette[1]) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.4, color = jama_palette[1]) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = seq(min(two_part_df$year_id), max(two_part_df$year_id), 1)) +
  labs(
    title = "Modeled Incremental Cost Over Time (All Races)",
    subtitle = "HIV+ vs Neither",
    x = "Year", y = "Incremental Cost (USD)"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot5_delta_hiv_only, "model_delta_hiv_only_over_time_all_races.jpeg")

plot6_delta_hiv_only_by_toc <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id, toc) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = mean_delta, color = toc, group = toc)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color = NA) +
  scale_color_manual(values = jama_palette) +
  scale_fill_manual(values = jama_palette) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = seq(min(two_part_df$year_id), max(two_part_df$year_id), 1)) +
  labs(
    title = "Modeled Average Cost Delta by Type of Care Over Time",
    subtitle = "HIV+ vs Neither, all races; weighted means with 95% CI ribbons",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Type of Care", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot6_delta_hiv_only_by_toc, "model_delta_hiv_only_by_year_toc_all_races.jpeg")


# Plot 7: Modeled Average Cost Delta by Type of Care and Race Over Time

plot7_delta_hiv_only_by_toc_and_race_facet <- two_part_df %>%
  group_by(year_id, toc, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_delta_hiv_only, total_bin_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta_hiv_only, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(year_id), y = mean_delta, color = toc, group = toc)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color = NA) +
  scale_color_manual(values = jama_palette) +
  scale_fill_manual(values = jama_palette) +
  scale_y_continuous(labels = scales::dollar_format()) +
  facet_wrap(~ race_cd) +
  labs(
    title = "Modeled Cost Delta by Type of Care and Race Over Time",
    subtitle = "Weighted difference in cost (HIV+ vs Neither); 95% CI ribbons",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Type of Care", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot7_delta_hiv_only_by_toc_and_race_facet, "model_delta_hiv_only_by_year_toc_facet_race.jpeg")


# Define colors for HIV status (all races)
hiv_colors <- c("Neither" = jama_palette[1], "HIV+" = jama_palette[2])

plot8_model_cost_by_toc_hiv <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  select(toc, mean_cost_neither, mean_cost_hiv_only) %>%
  pivot_longer(cols = starts_with("mean_cost_"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "cost") %>%
  mutate(hiv_status = recode(hiv_status,
                             "neither" = "Neither",
                             "hiv_only" = "HIV+")) %>%
  group_by(toc, hiv_status) %>%
  summarise(mean_cost = mean(cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = toc, y = mean_cost, fill = hiv_status)) +
  geom_col(position = "dodge", color = "black") +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Modeled Average Cost by Type of Care and HIV Status",
    x = "Type of Care", y = "Mean Cost (USD)", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot8_model_cost_by_toc_hiv, "model_cost_by_toc_and_hiv_status.jpeg")


plot9_model_cost_by_age_hiv <- two_part_df %>%
  filter(race_cd == "all_race", !is.na(age_group_years_start)) %>%
  select(age_group_years_start, mean_cost_neither, mean_cost_hiv_only) %>%
  pivot_longer(cols = starts_with("mean_cost_"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status,
                             "neither" = "Neither",
                             "hiv_only" = "HIV+")) %>%
  group_by(age_group_years_start, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(factor(age_group_years_start), mean_cost, fill = hiv_status)) +
  geom_col(position = position_dodge(), color = "black") +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Modeled Average Cost by Age Group and HIV Status (All Races)",
    x = "Age Group", y = "Mean Cost (USD)", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot9_model_cost_by_age_hiv, "model_cost_by_age_and_hiv_status.jpeg")





# #  Modeled Mean Cost Over Time by Disease and HIV Status
# plot10_model_cost_by_disease_year_hiv <- two_part_df %>%
#   pivot_longer(cols = starts_with("mean_cost"),
#                names_to = "hiv_status",
#                names_prefix = "mean_cost_",
#                values_to = "mean_cost") %>%
#   mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+")) %>%
#   group_by(acause, year_id, hiv_status) %>%
#   summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
#   ggplot(aes(x = as.integer(year_id), y = mean_cost, color = acause, linetype = hiv_status)) +
#   geom_line(size = 1.1) +
#   geom_point(size = 2) +
#   scale_y_continuous(labels = dollar_format()) +
#   scale_x_continuous(breaks = seq(min(two_part_df$year_id), max(two_part_df$year_id), 1)) +
#   labs(
#     title = "Modeled Mean Cost Over Time by Disease and HIV Status",
#     x = "Year", y = "Mean Cost (USD)",
#     color = "Disease Category", linetype = "HIV Status"
#   ) +
#   theme_minimal(base_size = 13)
# 
# save_plot(plot10_model_cost_by_disease_year_hiv, "model_cost_by_disease_year_and_hiv.jpeg")



# Load JAMA palette
library(ggsci)
jama_palette <- pal_jama("default")(7)

# Define JAMA colors for HIV and diseases
hiv_colors <- c("HIV-" = jama_palette[1], "HIV+" = jama_palette[2])

# Create consistent disease color palette
acause_vals <- unique(two_part_df$acause)
disease_colors <- setNames(jama_palette[1:length(acause_vals)], acause_vals)

# 📊 Plot 18: Modeled Mean Cost by Disease and Type of Care, Faceted by Race
plot18_model_cost_by_disease_toc_race <- two_part_df %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+")) %>%
  group_by(acause, toc, hiv_status, race_cd) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = toc, y = mean_cost, fill = acause)) +
  geom_col(position = "dodge", color = "black") +
  facet_wrap(~ race_cd) +
  scale_fill_manual(values = disease_colors) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Mean Cost by Disease and Type of Care (Faceted by Race)",
    x = "Type of Care", y = "Mean Cost (USD)", fill = "Disease Category"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot18_model_cost_by_disease_toc_race, "model_cost_by_disease_toc_race.jpeg")

# Plot 19: Histogram of Modeled Cost Delta (Inpatient Only)
plot19_model_ip_hist_cost_delta <- two_part_df %>%
  filter(toc == "IP") %>%
  ggplot(aes(x = mean_cost_delta)) +
  geom_histogram(binwidth = 1000, fill = jama_palette[1], color = "white") +
  scale_x_continuous(labels = dollar_format()) +
  labs(
    title = "Inpatient Mean Cost Delta (HIV+ minus HIV−)",
    x = "Cost Difference (USD)", y = "Number of Strata"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot19_model_ip_hist_cost_delta, "model_ip_hist_cost_delta.jpeg")

# Plot 20: Modeled Inpatient Cost by Age Group and HIV Status
plot20_model_ip_cost_by_age_hiv <- two_part_df %>%
  filter(toc == "IP", race_cd == "all_race") %>%
  select(age_group_years_start, mean_cost_hiv_0, mean_cost_hiv_1,
         lower_ci_cost_hiv_0, upper_ci_cost_hiv_0,
         lower_ci_cost_hiv_1, upper_ci_cost_hiv_1) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status", values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status,
                             "mean_cost_hiv_0" = "HIV-",
                             "mean_cost_hiv_1" = "HIV+")) %>%
  pivot_longer(cols = starts_with("lower_ci_cost"),
               names_to = "ci_lower_status", values_to = "lower_ci") %>%
  pivot_longer(cols = starts_with("upper_ci_cost"),
               names_to = "ci_upper_status", values_to = "upper_ci") %>%
  filter((ci_lower_status == "lower_ci_cost_hiv_0" & hiv_status == "HIV-") |
           (ci_lower_status == "lower_ci_cost_hiv_1" & hiv_status == "HIV+")) %>%
  filter((ci_upper_status == "upper_ci_cost_hiv_0" & hiv_status == "HIV-") |
           (ci_upper_status == "upper_ci_cost_hiv_1" & hiv_status == "HIV+")) %>%
  group_by(age_group_years_start, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE),
            lower_ci = mean(lower_ci, na.rm = TRUE),
            upper_ci = mean(upper_ci, na.rm = TRUE),
            .groups = "drop") %>%
  ggplot(aes(x = factor(age_group_years_start), y = mean_cost, color = hiv_status, group = hiv_status)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = hiv_status), alpha = 0.2, color = NA) +
  scale_color_manual(values = hiv_colors) +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Inpatient Cost by Age Group and HIV Status",
    x = "Age Group", y = "Mean Cost (USD)",
    color = "HIV Status", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot20_model_ip_cost_by_age_hiv, "model_ip_cost_by_age_hiv.jpeg")


#####
# Load JAMA color palette and set up color schemes
library(ggsci)
jama_palette <- pal_jama("default")(7)
hiv_colors <- c("HIV-" = jama_palette[1], "HIV+" = jama_palette[2])

# 📊 Plot 21: Inpatient Cost by Age Group and HIV Status (All Races)
plot21_ip_cost_by_age_hiv_status <- two_part_df %>%
  filter(toc == "IP", race_cd == "all_race", !is.na(age_group_years_start)) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status", values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status,
                             "mean_cost_hiv_0" = "HIV-",
                             "mean_cost_hiv_1" = "HIV+")) %>%
  group_by(age_group_years_start, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(factor(age_group_years_start), mean_cost, fill = hiv_status)) +
  geom_col(position = position_dodge(), color = "black") +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Inpatient Cost by Age Group and HIV Status (All Races)",
    x = "Age Group", y = "Mean Cost (USD)", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot21_ip_cost_by_age_hiv_status, "model_ip_cost_by_age_hiv_status.jpeg")

# 📊 Plot 22: Inpatient Cost Delta by Race Over Time
plot22_ip_delta_cost_by_race_time <- two_part_df %>%
  filter(toc == "IP", race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  group_by(year_id, race_cd) %>%
  summarise(mean_delta = mean(mean_cost_delta, na.rm = TRUE),
            lower = mean(lower_ci_delta, na.rm = TRUE),
            upper = mean(upper_ci_delta, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = as.integer(year_id), y = mean_delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = seq(2008, 2019, 1)) +
  labs(
    title = "Inpatient Cost Delta by Race Over Time",
    x = "Year", y = "Cost Difference (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot22_ip_delta_cost_by_race_time, "model_ip_delta_cost_by_race_time.jpeg")

# Plot 23: Faceted by Disease - IP Delta Cost by Race & Year
plot23_ip_delta_by_race_time_faceted <- two_part_df %>%
  filter(toc == "IP", race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  mutate(year_id = as.integer(year_id)) %>%
  group_by(year_id, race_cd, acause) %>%
  summarise(mean_delta = mean(mean_cost_delta, na.rm = TRUE),
            lower = mean(lower_ci_delta, na.rm = TRUE),
            upper = mean(upper_ci_delta, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = year_id, y = mean_delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_y_continuous(labels = dollar_format()) +
  facet_wrap(~acause) +
  labs(
    title = "Inpatient Cost Delta by Race Over Time, Faceted by Disease Category",
    x = "Year", y = "Cost Difference (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot23_ip_delta_by_race_time_faceted, "model_ip_delta_cost_by_race_time_faceted.jpeg")

# Plot 24: Same as Above but With Year Tick Fix
plot24_ip_delta_by_race_time_tickfix <- two_part_df %>%
  filter(toc == "IP", race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  mutate(year_id = as.integer(year_id)) %>%
  group_by(year_id, race_cd, acause) %>%
  summarise(mean_delta = mean(mean_cost_delta, na.rm = TRUE),
            lower = mean(lower_ci_delta, na.rm = TRUE),
            upper = mean(upper_ci_delta, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = year_id, y = mean_delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_x_continuous(breaks = seq(2008, 2019, 2)) +
  scale_y_continuous(labels = dollar_format()) +
  facet_wrap(~acause) +
  labs(
    title = "Inpatient Cost Delta by Race Over Time, Faceted by Disease Category",
    x = "Year", y = "Cost Difference (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(size = 9))

save_plot(plot24_ip_delta_by_race_time_tickfix, "model_ip_delta_cost_by_race_time_facet_acause_cleaned.jpeg")

################
## regs check 
##
#################

# # Define output file path
# output_file <- file.path(input_folder_regres, "combined_regression_estimates.csv")
# 
# # Save to CSV
# fwrite(regression_all_df, file = output_file)


ggplot(regression_all_df, aes(x = p_value_logit)) +
  geom_histogram(bins = 50) +
  labs(title = "P-value distribution (logit)", x = "p-value", y = "Count")

ggplot(regression_all_df, aes(x = p_value_gamma)) +
  geom_histogram(bins = 50) +
  labs(title = "P-value distribution (gamma)", x = "p-value", y = "Count")


# Summary of mean p-values by variable
regression_all_df[, .(
  mean_p_logit = mean(p_value_logit, na.rm = TRUE),
  mean_p_gamma = mean(p_value_gamma, na.rm = TRUE),
  n_sig_logit = sum(p_value_logit < 0.05, na.rm = TRUE),
  n_sig_gamma = sum(p_value_gamma < 0.05, na.rm = TRUE)
), by = variable]



regression_summary <- regression_all_df[, .(
  n_obs = .N,
  mean_p_logit = mean(p_value_logit, na.rm = TRUE),
  median_p_logit = median(p_value_logit, na.rm = TRUE),
  min_p_logit = min(p_value_logit, na.rm = TRUE),
  max_p_logit = max(p_value_logit, na.rm = TRUE),
  sd_p_logit = sd(p_value_logit, na.rm = TRUE),
  n_sig_logit = sum(p_value_logit < 0.05, na.rm = TRUE),
  
  mean_p_gamma = mean(p_value_gamma, na.rm = TRUE),
  median_p_gamma = median(p_value_gamma, na.rm = TRUE),
  min_p_gamma = min(p_value_gamma, na.rm = TRUE),
  max_p_gamma = max(p_value_gamma, na.rm = TRUE),
  sd_p_gamma = sd(p_value_gamma, na.rm = TRUE),
  n_sig_gamma = sum(p_value_gamma < 0.05, na.rm = TRUE)
), by = variable][order(mean_p_gamma)]



regression_summary <- regression_all_df[, .(
  # Count
  n_obs = .N,
  
  # Logit model: p-values
  mean_p_logit = mean(p_value_logit, na.rm = TRUE),
  median_p_logit = median(p_value_logit, na.rm = TRUE),
  min_p_logit = min(p_value_logit, na.rm = TRUE),
  max_p_logit = max(p_value_logit, na.rm = TRUE),
  sd_p_logit = sd(p_value_logit, na.rm = TRUE),
  n_sig_logit = sum(p_value_logit < 0.05, na.rm = TRUE),
  
  # Gamma model: p-values
  mean_p_gamma = mean(p_value_gamma, na.rm = TRUE),
  median_p_gamma = median(p_value_gamma, na.rm = TRUE),
  min_p_gamma = min(p_value_gamma, na.rm = TRUE),
  max_p_gamma = max(p_value_gamma, na.rm = TRUE),
  sd_p_gamma = sd(p_value_gamma, na.rm = TRUE),
  n_sig_gamma = sum(p_value_gamma < 0.05, na.rm = TRUE),
  
  # Logit model: estimates
  mean_est_logit = mean(Estimate_logit, na.rm = TRUE),
  sd_est_logit = sd(Estimate_logit, na.rm = TRUE),
  
  # Gamma model: estimates
  mean_est_gamma = mean(Estimate_gamma, na.rm = TRUE),
  sd_est_gamma = sd(Estimate_gamma, na.rm = TRUE)
), by = variable][order(mean_p_gamma)]


