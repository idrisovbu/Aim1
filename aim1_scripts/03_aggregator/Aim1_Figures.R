rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, readr,writexl, purrr, ggplot2, gridExtra, scales, ggpubr, patchwork, RColorBrewer)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

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


# Define base input directory
base_input_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"

# Set the date folders where input files are located
date_of_input_sum <- "20250404"
date_of_input_2part <- "20250403"
date_of_input_meta <- "20250404"  # <-- metadata 

# Construct full paths to input folders
input_folder_sum <- file.path(base_input_dir, date_of_input_sum, "Agregator(summary_stats)")
input_folder_2part <- file.path(base_input_dir, date_of_input_2part, "Agregator(two-part_summary)")
input_folder_meta <- file.path(base_input_dir, date_of_input_meta, "Agregator(summary_stats)")

# Load CSVs from respective folders
summary_df <- fread(file.path(input_folder_sum, "f2t_rx_weighted_summary_table.csv"))
two_part_df <- fread(file.path(input_folder_2part, "weighted_summary_two_part_table.csv"))
metadata_df <- fread(file.path(input_folder_meta, "aggregated_metadata_table.csv"))

# Set output directory using today's date
today <- format(Sys.Date(), "%Y%m%d")
#figures_folder <- file.path(base_input_dir, today, "Figures_tables")

figures_folder <- '/Users/idrisov/Desktop/Agregator csv files/figures'

# Create output directory if it doesn't exist
dir.create(figures_folder, recursive = TRUE, showWarnings = FALSE)


#function to save plots
save_plot <- function(plot_obj, filename) {
  ggsave(filename = file.path(figures_folder, filename),
         plot = plot_obj, width = 9, height = 6, dpi = 300,
         device = "jpeg")
}

##----------------------------------------------------------------
##   Create descriptive summary tables (Table 1 style) and plots
##   for the finalized dataset used in the JAMA paper.
##----------------------------------------------------------------


# Define output location for Table 1
table1_outfile <- file.path(figures_folder, "table1_summary_metadata.csv")

metadata_df <- aggregated_metadata_table

# 1. Overall summary
overall_summary <- metadata_df %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene, na.rm = TRUE),
    hiv_positive_beneficiaries = sum(hiv_unique_bene, na.rm = TRUE),
    hiv_prevalence = hiv_positive_beneficiaries / total_beneficiaries
  ) %>%
  mutate(hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1)) %>%
  select(-hiv_prevalence) %>%
  mutate(category = "Overall", subgroup = "All")

# 2. Summary by age group
age_summary <- metadata_df %>%
  group_by(age_group) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene),
    hiv_positive_beneficiaries = sum(hiv_unique_bene),
    hiv_prevalence = hiv_positive_beneficiaries / total_beneficiaries
  ) %>%
  mutate(
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    category = "Age Group",
    subgroup = as.character(age_group)
  ) %>%
  select(-hiv_prevalence)

# 3. Summary by type of care
toc_summary <- metadata_df %>%
  group_by(toc) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene),
    hiv_positive_beneficiaries = sum(hiv_unique_bene),
    hiv_prevalence = hiv_positive_beneficiaries / total_beneficiaries
  ) %>%
  mutate(
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    category = "Type of Care",
    subgroup = toc
  ) %>%
  select(-hiv_prevalence)

# 4. Summary by year
year_summary <- metadata_df %>%
  group_by(year_id) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene),
    hiv_positive_beneficiaries = sum(hiv_unique_bene),
    hiv_prevalence = hiv_positive_beneficiaries / total_beneficiaries
  ) %>%
  mutate(
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    category = "Year",
    subgroup = as.character(year_id)
  ) %>%
  select(-hiv_prevalence)


# Combine all into final Table 1
table1_df <- bind_rows(
  overall_summary,
  age_summary,
  toc_summary,
  year_summary
) %>%
  select(category, subgroup, total_beneficiaries, hiv_positive_beneficiaries, hiv_prevalence_percent)

# Save Table 1 as CSV
write_csv(table1_df, table1_outfile)
cat("✅ Table 1 saved to:", table1_outfile, "\n")

# Note: Total beneficiary counts represent stratified records by age group, type of care, year,
# and coding system. Individuals may appear in multiple subgroups. HIV prevalence is calculated 
# as the proportion of beneficiaries with HIV-coded claims within each stratum.
##########################################


# Define output path
table1a_outfile <- file.path(figures_folder, "table1a_summary_statistics.csv")

# Helper functions
summarize_table <- function(df, group_var) {
  df %>%
    group_by({{ group_var }}) %>%
    summarise(
      median_avg_cost = median(avg_cost_per_bene, na.rm = TRUE),
      iqr_avg_cost = IQR(avg_cost_per_bene, na.rm = TRUE),
      median_max_cost = median(max_cost_per_bene, na.rm = TRUE),
      iqr_max_cost = IQR(max_cost_per_bene, na.rm = TRUE),
      median_quant99 = median(quant99, na.rm = TRUE),
      iqr_quant99 = IQR(quant99, na.rm = TRUE),
      median_total_cost = median(total_cost, na.rm = TRUE),
      iqr_total_cost = IQR(total_cost, na.rm = TRUE),
      median_avg_encounters = round(median(avg_encounters_per_bene, na.rm = TRUE), 2),
      iqr_avg_encounters = round(IQR(avg_encounters_per_bene, na.rm = TRUE), 2),
      median_total_encounters = median(total_encounters, na.rm = TRUE),
      iqr_total_encounters = IQR(total_encounters, na.rm = TRUE),
      median_total_bene = median(total_unique_bene, na.rm = TRUE),
      iqr_total_bene = IQR(total_unique_bene, na.rm = TRUE),
      mean_any_cost = percent(mean(mean_any_cost, na.rm = TRUE), accuracy = 0.1),
      .groups = "drop"
    )
}

# Stratified summaries
# table_1b <- summarize_table(summary_df, hiv_flag)
# table_1c <- summarize_table(summary_df, acause)
# table_1d <- summarize_table(summary_df, year_id)
# table_1e <- summarize_table(summary_df, race_cd)
# table_1f <- summarize_table(summary_df, toc)
# table_1g <- summarize_table(summary_df, age_group)
# 
# # Save all tables
# write_csv(table_1b, file.path(figures_folder, "table1b_by_hiv.csv"))
# write_csv(table_1c, file.path(figures_folder, "table1c_by_disease.csv"))
# write_csv(table_1d, file.path(figures_folder, "table1d_by_year.csv"))
# write_csv(table_1e, file.path(figures_folder, "table1e_by_race.csv"))
# write_csv(table_1f, file.path(figures_folder, "table1f_by_toc.csv"))
# write_csv(table_1g, file.path(figures_folder, "table1g_by_age.csv"))
# 
# cat("✅ Tables 1b–1g saved to:", figures_folder, "\n")


summary_df <- f2t_rx_weighted_summary_table

# Run stratified summaries
table_1b <- summarize_table(summary_df, hiv_flag)
table_1c <- summarize_table(summary_df, acause)
table_1d <- summarize_table(summary_df, year_id)
table_1e <- summarize_table(summary_df, race_cd)
table_1f <- summarize_table(summary_df, toc)
table_1g <- summarize_table(summary_df, age_group)

# Name each as a sheet
table_list <- list(
  "By HIV Status" = table_1b,
  "By Disease" = table_1c,
  "By Year" = table_1d,
  "By Race" = table_1e,
  "By Type of Care" = table_1f,
  "By Age Group" = table_1g
)

# Save all sheets to one Excel file
write_xlsx(table_list, path = file.path(figures_folder, "table1_stratified_summaries.xlsx"))

cat("✅ All stratified Table 1 summaries saved to one Excel file with separate tabs.\n")


##----------------------------------------------------------------
##   long form for the table
##----------------------------------------------------------------


summarize_longform <- function(df, group_var, group_name) {
  df %>%
    group_by({{ group_var }}) %>%
    summarise(
      avg_cost_per_bene_median = median(avg_cost_per_bene, na.rm = TRUE),
      avg_cost_per_bene_iqr = IQR(avg_cost_per_bene, na.rm = TRUE),
      max_cost_per_bene_median = median(max_cost_per_bene, na.rm = TRUE),
      total_cost_median = median(total_cost, na.rm = TRUE),
      total_encounters_median = median(total_encounters, na.rm = TRUE),
      total_unique_bene_median = median(total_unique_bene, na.rm = TRUE),
      mean_any_cost = mean(mean_any_cost, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    pivot_longer(
      cols = -{{ group_var }},
      names_to = c("variable", "stat"),
      names_sep = "_(?=[^_]+$)",
      values_to = "value"
    ) %>%
    mutate(
      group = as.character({{ group_var }}),
      grouping = group_name
    ) %>%
    select(grouping, group, variable, stat, value)
}


# Run summaries
lf_hiv <- summarize_longform(summary_df, hiv_flag, "HIV Status")
lf_disease <- summarize_longform(summary_df, acause, "Disease")
lf_year <- summarize_longform(summary_df, year_id, "Year")
lf_toc <- summarize_longform(summary_df, toc, "Type of Care")
lf_race <- summarize_longform(summary_df, race_cd, "Race")
lf_age <- summarize_longform(summary_df, age_group, "Age Group")

# Combine
summary_longform <- bind_rows(lf_hiv, lf_disease, lf_year, lf_toc, lf_race, lf_age)

# Save
write_csv(summary_longform, file.path(figures_folder, "table1_longform_summary.csv"))
cat("✅ Long-form summary saved.\n")



##----------------------------------------------------------------
##   Create descriptive plots
##----------------------------------------------------------------



p1 <- summary_longform %>%
  filter(grouping == "HIV Status", variable == "avg_cost_per_bene", stat == "median") %>%
  ggplot(aes(x = group, y = value)) +
  geom_col(fill = "steelblue") +
  scale_y_continuous(labels = dollar_format()) +
  labs(title = "Median Cost per Beneficiary by HIV Status",
       x = "HIV Status (0 = HIV−, 1 = HIV+)",
       y = "Cost (USD)") +
  theme_minimal()

save_plot(p1, "cost_by_hiv_status.jpeg")


#
p2 <- summary_longform %>%
  filter(grouping == "Year", variable == "avg_cost_per_bene", stat == "median") %>%
  ggplot(aes(x = as.integer(group), y = value)) +
  geom_line(color = "darkred", linewidth = 1.2) +
  geom_point(color = "darkred", size = 2) +
  scale_y_continuous(labels = dollar_format()) +
  labs(title = "Median Cost per Beneficiary by Year",
       x = "Year", y = "Cost (USD)") +
  theme_minimal()

save_plot(p2, "cost_by_year.jpeg")












###############

# Average Cost per Beneficiary by Disease Category
p7 <- summary_df %>%
  group_by(acause) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = reorder(acause, avg_cost_per_bene), y = avg_cost_per_bene, fill = acause)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = scales::dollar(avg_cost_per_bene)),
            hjust = 1.1, color = "white", size = 4) +
  coord_flip() +
  labs(title = "Average Cost per Beneficiary by Disease Category",
       x = "Disease Category", y = "Average Cost (2019 USD)") +
  theme_minimal() +
  theme(legend.position = "none")

save_plot(p7, "Avg_Cost_per_Bene_by_Disease_labeled.jpeg")



# Plot 2: Total Costs Over Time
plot2 <- summary_df %>%
  group_by(year_id) %>%
  summarise(total_cost = sum(total_cost, na.rm = TRUE)) %>%
  ggplot(aes(x = year_id, y = total_cost)) +
  geom_line(size = 1.5, color = "blue") + geom_point(size = 3, color = "blue") +
  labs(title = "Total Costs Over Time (2019 USD)", x = "Year", y = "Total Costs") +
  theme_minimal()

save_plot(plot2, "Total_Costs_Over_Time.png")

# Plot 3: Average Encounters per Beneficiary by Age Group
# year and bene is not consitatnt

plot3_facet <- summary_df %>%
  ggplot(aes(x = factor(age_group), y = log(avg_encounters_per_bene), fill = factor(age_group))) +
  geom_boxplot() +
  facet_wrap(~ toc) +
  labs(title = "Log Average Encounters per Beneficiary by Age Group and Type of Care",
       x = "Age Group", y = "Log(Avg Encounters)") +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(plot3_facet, "Log_Avg_Encounters_by_Age_Group_and_TOC.png")

plot3_by_acause <- summary_df %>%
  ggplot(aes(x = factor(age_group), y = log(avg_encounters_per_bene), fill = factor(age_group))) +
  geom_boxplot() +
  facet_wrap(~ acause, scales = "free_y") +
  labs(title = "Log Average Encounters per Beneficiary by Age Group and Disease Category",
       x = "Age Group", y = "Log(Avg Encounters)") +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(plot3_by_acause, "Log_Avg_Encounters_by_Age_Group_and_Acause.png")



#this is ok
plot4 <- summary_df %>%
  ggplot(aes(x = acause, y = avg_cost_per_bene, fill = race_cd)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  labs(title = "Average Cost per Beneficiary by Disease and Race",
       x = "Disease Category", y = "Average Cost (2019 USD)", fill = "Race") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot4, "Avg_Cost_per_Bene_by_Disease_and_Race.png")


#### histograms


# Histogram of average encounters per beneficiary
hist3 <- summary_df %>%
  ggplot(aes(x = avg_encounters_per_bene)) +
  geom_histogram(bins = 50, fill = "tomato", color = "white") +
  labs(title = "Distribution of Average Encounters per Beneficiary",
       x = "Avg Encounters", y = "Count") +
  theme_minimal()

save_plot(hist3, "Hist_Avg_Encounters_Per_Bene.jpeg")


###########

scatter1_color <- summary_df %>%
  ggplot(aes(x = avg_encounters_per_bene,
             y = avg_cost_per_bene,
             color = toc)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_y_log10(labels = scales::dollar_format()) +
  scale_x_log10() +
  labs(title = "Encounters vs Average Cost (log-log scale)",
       subtitle = "By Type of Care",
       x = "Avg Encounters", y = "Avg Cost",
       color = "Type of Care") +
  theme_minimal() +
  theme(legend.position = "right")

save_plot(scatter1_color, "Scatter_Encounters_vs_Cost_By_TOC.jpeg")


##########




# 3. Avg Cost by Age Group GOOD
# Boxplot faceted by Race
p3_race <- ggplot(summary_df, aes(x = factor(age_group), y = avg_cost_per_bene)) +
  geom_boxplot(fill = "lightblue") +
  scale_y_log10(labels = dollar_format()) +
  facet_wrap(~race_cd) +
  labs(title = "Avg Cost per Beneficiary by Age Group (Faceted by Race)",
       x = "Age Group", y = "Avg Cost (log scale)") +
  theme_minimal()
save_plot(p3_race, "3a_Cost_by_Age_Group_by_Race.jpeg")

# Boxplot faceted by Type of Care
# Bar plot of average cost per beneficiary by Type of Care (TOC)
p3b <- summary_df %>%
  group_by(toc) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = reorder(toc, avg_cost_per_bene), y = avg_cost_per_bene, fill = toc)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = scales::dollar(avg_cost_per_bene)), vjust = -0.3, size = 4) +
  scale_y_log10(labels = dollar_format()) +
  labs(title = "Average Cost per Beneficiary by Type of Care",
       x = "Type of Care", y = "Average Cost (log scale, 2019 USD)") +
  theme_minimal() +
  theme(legend.position = "none")

save_plot(p3b, "3b_Cost_by_TOC.jpeg")





# 6. Median Cost by Year & HIV Status
#good but why the diffrnce? 

###
p6_data <- summary_df %>%
  group_by(year_id, hiv_flag) %>%
  summarise(med_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  mutate(year_id = as.integer(year_id))  # 👈 Force integer for clean axis

p6 <- ggplot(p6_data, aes(x = year_id, y = med_cost, color = factor(hiv_flag))) +
  geom_line(linewidth = 1.2) + 
  geom_point(size = 2) +
  scale_x_continuous(breaks = seq(min(p6_data$year_id), max(p6_data$year_id), 1)) +  # clean year ticks
  scale_y_continuous(labels = dollar_format()) +
  labs(title = "Median Avg Cost by Year and HIV Status",
       x = "Year", y = "Median Avg Cost", color = "HIV Status") +
  theme_minimal()

save_plot(p6, "6_Cost_by_Year_HIV.jpeg")

#####


# Re-aggregate data
bubble_df <- summary_df %>%
  group_by(year_id, toc) %>%
  summarise(
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    avg_cost = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(year_id = factor(year_id))  # Ensure clean year labels

# Plot without facets
plot_bubble_flat <- ggplot(bubble_df, aes(x = year_id, y = avg_cost, size = total_bene, fill = toc)) +
  geom_point(shape = 21, color = "black", alpha = 0.85) +
  scale_y_continuous(labels = dollar_format()) +
  scale_size_continuous(range = c(3, 15), name = "Total Beneficiaries", labels = comma_format()) +
  scale_fill_brewer(palette = "Set1", name = "Type of Care") +
  labs(
    title = "Average Cost per Beneficiary by Year",
    subtitle = "Bubble size reflects sample size; color reflects type of care",
    x = "Year", y = "Average Cost (2019 USD)"
  ) +
  theme_minimal(base_size = 13)

# Save
save_plot(plot_bubble_flat, "Bubble_Cost_by_Year_ColorByTOC.jpeg")









###

# 7. Heatmap of Avg Cost by Year and Race
p7_data <- summary_df %>%
  group_by(year_id, race_cd) %>%
  summarise(med_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop")
p7 <- ggplot(p7_data, aes(x = factor(year_id), y = race_cd, fill = med_cost)) +
  geom_tile(color = "white") +
  scale_fill_viridis_c(labels = dollar_format()) +
  labs(title = "Heatmap: Median Avg Cost by Year and Race",
       x = "Year", y = "Race", fill = "Cost (USD)") +
  theme_minimal()
save_plot(p7, "7_Heatmap_Cost_Year_Race.jpeg")



###########

#two part model 

#########


# Color Schemes
hiv_colors <- c("HIV-Negative" = "#009E73", "HIV-Positive" = "#0E5EAE")
toc_colors <- RColorBrewer::brewer.pal(7, "Set1")


# Plot 1: Incremental Cost by Type of Care Over Years
plot1 <- two_part_df %>%
  group_by(year_id, toc) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
            lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
            upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
            .groups = "drop") %>%
  ggplot(aes(year_id, delta, color = toc, group = toc)) +
  geom_line(size = 1.2) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
  labs(title = "Incremental Cost for HIV+ vs HIV− by Type of Care",
       x = "Year", y = "Incremental Cost (USD)", color = "Type of Care") +
  scale_color_manual(values = toc_colors) +
  theme_minimal()
save_plot(plot1, "plot1_delta_by_toc_year.jpg")

# Plot 2: Incremental Spending by Race and Disease
#this is good
plot2 <- two_part_df %>%
  group_by(acause, race_cd) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
            lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
            upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
            .groups = "drop") %>%
  ggplot(aes(race_cd, delta, fill = acause)) +
  geom_col(position = position_dodge(width=0.9)) +
  geom_errorbar(aes(ymin = lower, ymax = upper), position = position_dodge(width=0.9), width=0.2) +
  facet_wrap(~ acause) +
  labs(title = "Incremental Spending by Race and Disease Category",
       x = "Race/Ethnicity", y = "Incremental Cost (USD)", fill = "Disease") +
  theme_bw()
save_plot(plot2, "plot2_delta_by_race_and_disease.jpg")

# Plot 3: Incremental Spending Delta by Age and Race
plot3 <- two_part_df %>%
  group_by(age_group, race_cd) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
            lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
            upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
            .groups = "drop") %>%
  ggplot(aes(age_group, delta, color = race_cd, group=race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width=0.3) +
  labs(title = "Spending Delta by Age and Race",
       x = "Age Group", y = "Incremental Cost (USD)", color = "Race") +
  theme_classic()
save_plot(plot3, "plot3_delta_by_age_and_race.jpg")

# Plot 3a: Incremental Spending Delta by Year and Race
# plot3a <- two_part_df %>%
#   group_by(year_id, race_cd) %>%
#   summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
#             lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
#             upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
#             .groups = "drop") %>%
#   ggplot(aes(year_id, delta, color = race_cd, group=race_cd)) +
#   geom_line(size = 1.1) +
#   geom_point() +
#   geom_errorbar(aes(ymin = lower, ymax = upper), width=0.3) +
#   labs(title = "Spending Delta by Year and Race",
#        x = "Year", y = "Incremental Cost (USD)", color = "Race") +
#   theme_classic()
# save_plot(plot3a, "plot3a_delta_by_year_and_race.jpg")



#####

plot3a_faceted <- two_part_df %>%
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
  facet_wrap(~toc) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Incremental Spending by Year, Race, and Type of Care",
    subtitle = "Weighted average cost difference (HIV+ vs HIV-); faceted by type of care",
    x = "Year", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot3a_faceted, "plot3a_delta_by_year_and_race_faceted.jpg")

#####



plot5 <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id) %>%
  summarise(
    mean = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE)
  ) %>%
  ggplot(aes(year_id, mean)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.4) +
  labs(title = "Incremental Cost (HIV+ vs HIV−) Over Time (All Races)",
       x = "Year", y = "Incremental Cost (USD)") +
  theme_minimal()
save_plot(plot5, "plot5_incremental_cost_over_time.jpg")

#good but only for IP
# plot6 <- two_part_df %>%
#   filter(race_cd %in% c("BLCK", "WHT", "HISP")) %>%
#   group_by(year_id, race_cd) %>%
#   summarise(
#     mean = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
#     lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
#     upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE)
#   ) %>%
#   ggplot(aes(year_id, mean, color = race_cd)) +
#   geom_line(size = 1) + geom_point(size = 2) +
#   geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
#   labs(title = "Incremental Cost by Race Over Time",
#        x = "Year", y = "Incremental Cost (USD)", color = "Race") +
#   theme_minimal()
# save_plot(plot6, "plot6_incremental_cost_race_time.jpg")

####good try make for races as above
plot7 <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id, toc) %>%
  summarise(mean_delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
            lower_ci = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
            upper_ci = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE)) %>%
  ggplot(aes(year_id, mean_delta, color = toc, group=toc)) +
  geom_line() +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color=NA) +
  labs(title = "Average Cost Delta by Type of Care Over Time",
       x = "Year", y = "Mean Cost Delta (USD)") +
  theme_minimal()
save_plot(plot7, "plot7_avg_delta_toc_time.jpg")


#####

plot7_faceted <- two_part_df %>%
  group_by(year_id, toc, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(year_id), y = mean_delta, color = toc, group = toc)) +
  geom_line(size = 1) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color = NA) +
  facet_wrap(~race_cd) +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Average Cost Delta by Type of Care and Race Over Time",
    subtitle = "Weighted difference in mean cost (HIV+ vs HIV-); error bands show weighted 95% CI",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Type of Care", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot7_faceted, "plot7_avg_delta_by_toc_and_race.jpg")



#####


# Define color scheme
hiv_colors <- c("HIV-" = "#009E73", "HIV+" = "#0E5EAE")
toc_colors <- RColorBrewer::brewer.pal(7, "Set1")

##########################################
# Plot 9: Average Spending by Type of Care and HIV Status
##########################################

#this is good 
plot9 <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  pivot_longer(cols = starts_with("mean_cost_hiv"),
               names_to = "hiv_status",
               values_to = "cost") %>%
  mutate(hiv_status = recode(hiv_status, "mean_cost_hiv_0" = "HIV-", "mean_cost_hiv_1" = "HIV+")) %>%
  group_by(toc, hiv_status) %>%
  summarise(mean_cost = mean(cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = toc, y = mean_cost, fill = hiv_status)) +
  geom_col(position = "dodge", color="black") +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = scales::dollar) +
  labs(title = "Average Spending by Type of Care and HIV Status",
       x = "Type of Care", y = "Mean Cost (USD)") +
  theme_minimal()
save_plot(plot9, "plot9_spending_by_toc_hiv_status.jpg")

##########################################
# Plot 10: Delta Cost by Race Over Time (Means)
##########################################
# plot10 <- two_part_df %>%
#   filter(race_cd %in% c("WHT", "BLCK", "HISP")) %>%
#   group_by(year_id, race_cd) %>%
#   summarise(mean_delta = mean(mean_cost_delta, na.rm = TRUE),
#             lower = mean(lower_ci_delta, na.rm = TRUE),
#             upper = mean(upper_ci_delta, na.rm = TRUE), .groups = "drop") %>%
#   ggplot(aes(year_id, mean_delta, color = race_cd, group = race_cd)) +
#   geom_line(size = 1.1) +
#   geom_point() +
#   geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
#   labs(title = "Delta Cost by Race Over Time",
#        x = "Year", y = "Cost Difference (USD)", color = "Race") +
#   theme_minimal()
# save_plot(plot10, "plot10_delta_cost_by_race_time_means.jpg")

##########################################
# Plot 11: Mean Cost by Age Group and HIV Status (All Races)
##########################################
#good by add label for delta
plot11 <- two_part_df %>%
  filter(race_cd == "all_race", !is.na(age_group)) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "mean_cost_hiv_0" = "HIV-", "mean_cost_hiv_1" = "HIV+")) %>%
  group_by(age_group, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(factor(age_group), mean_cost, fill = hiv_status)) +
  geom_bar(stat = "identity", position = position_dodge(), color="black") +
  scale_fill_manual(values = hiv_colors) +
  labs(title = "Average Cost by Age Group and HIV Status (All Races)",
       x = "Age Group", y = "Mean Cost (USD)") +
  theme_minimal(base_size = 13)
save_plot(plot11, "plot11_cost_by_age_hiv_status.jpg")



##########################################
# Plot 12: Mean Cost Over Time by Disease and HIV Status
##########################################
plot12 <- two_part_df %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+")) %>%
  group_by(acause, year_id, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(year_id, mean_cost, color = acause, linetype = hiv_status)) +
  geom_line(size = 1.1) + geom_point() +
  labs(title = "Mean Cost Over Time by Disease and HIV Status",
       y = "Mean Cost (USD)", x = "Year") +
  theme_minimal()
save_plot(plot12, "plot12_cost_by_disease_and_hiv_time.jpg")

##########################################
# Plot 13: Mean Cost by Disease and Type of Care
##########################################

plot13 <- two_part_df %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+")) %>%
  group_by(acause, toc, hiv_status, race_cd) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(toc, mean_cost, fill = acause)) +
  geom_bar(stat = "identity", position = "dodge") +  # removed color="black"
  facet_wrap(~ race_cd) +
  labs(title = "Mean Cost by Disease and Type of Care, Faceted by Race",
       y = "Mean Cost (USD)", x = "Type of Care") +
  theme_minimal()

save_plot(plot13, "plot13_cost_by_disease_toc_facet_race.jpg")


  
###### IP only

# Filter to Inpatient (IP) only
df_ip <- two_part_df %>% filter(toc == "IP")

# Define color palette
my_pal <- c("#E41A1C", "#377EB8", "#4DAF4A", "#984EA3", "#FF7F00")

# Plot: Mean Cost Delta Histogram
p_ip_delta_hist <- df_ip %>%
  ggplot(aes(x = mean_cost_delta)) +
  geom_histogram(binwidth = 1000, fill = "darkblue", color = "white") +
  scale_x_continuous(labels = dollar_format()) +
  labs(title = "IP Distribution of Mean Cost Delta (HIV+ minus HIV-)",
       x = "Cost Difference", y = "Number of Strata") +
  theme_minimal()

save_plot(p_ip_delta_hist, "IP_Histogram_Cost_Delta.jpeg")



# 🔵 Define colors for HIV statuses
hiv_colors <- c("HIV-" = "#1f77b4", "HIV+" = "#d62728")

# 📊 Line plot of mean cost by age group, stratified by HIV status, with ribbons
plot_hiv_age_lines <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  select(age_group, toc, year_id,
         mean_cost_hiv_0, mean_cost_hiv_1,
         lower_ci_cost_hiv_0, upper_ci_cost_hiv_0,
         lower_ci_cost_hiv_1, upper_ci_cost_hiv_1) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status,
                             "mean_cost_hiv_0" = "HIV-",
                             "mean_cost_hiv_1" = "HIV+")) %>%
  pivot_longer(cols = starts_with("lower_ci_cost"),
               names_to = "ci_lower_status",
               values_to = "lower_ci") %>%
  pivot_longer(cols = starts_with("upper_ci_cost"),
               names_to = "ci_upper_status",
               values_to = "upper_ci") %>%
  filter((ci_lower_status == "lower_ci_cost_hiv_0" & hiv_status == "HIV-") |
           (ci_lower_status == "lower_ci_cost_hiv_1" & hiv_status == "HIV+")) %>%
  filter((ci_upper_status == "upper_ci_cost_hiv_0" & hiv_status == "HIV-") |
           (ci_upper_status == "upper_ci_cost_hiv_1" & hiv_status == "HIV+")) %>%
  group_by(age_group, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE),
            lower_ci = mean(lower_ci, na.rm = TRUE),
            upper_ci = mean(upper_ci, na.rm = TRUE),
            .groups = "drop") %>%
  ggplot(aes(x = factor(age_group), y = mean_cost, color = hiv_status, group = hiv_status)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = hiv_status), alpha = 0.2, color = NA) +
  scale_color_manual(values = hiv_colors) +
  scale_fill_manual(values = hiv_colors) +
  labs(title = "Mean Cost by Age Group and HIV Status IP",
       x = "Age Group", y = "Mean Cost (USD)", color = "HIV Status", fill = "HIV Status") +
  theme_minimal(base_size = 13)

save_plot(plot_hiv_age_lines, "IP_plot_hiv_cost_by_age_lines.jpg")


########

# Custom colors for HIV status
hiv_colors <- c("HIV+" = "#E41A1C", "HIV-" = "#377EB8")

#  Plot A: Avg Cost by Age Group and HIV Status (IP only, all races)
plot_ip_age <- two_part_df %>%
  filter(toc == "IP", race_cd == "all_race", !is.na(age_group)) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status", values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status,
                             "mean_cost_hiv_0" = "HIV-",
                             "mean_cost_hiv_1" = "HIV+")) %>%
  group_by(age_group, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(factor(age_group), mean_cost, fill = hiv_status)) +
  geom_bar(stat = "identity", position = position_dodge(), color = "black") +
  scale_fill_manual(values = hiv_colors) +
  labs(title = "Inpatient Cost by Age Group and HIV Status (All Races)",
       x = "Age Group", y = "Mean Cost (USD)", fill = "HIV Status") +
  theme_minimal(base_size = 13)

save_plot(plot_ip_age, "plot_ip_cost_by_age_hiv_status.jpg")


# Plot B: Delta Cost by Race Over Time (IP only)
plot_ip_race_time <- two_part_df %>%
  filter(toc == "IP", race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  group_by(year_id, race_cd) %>%
  summarise(mean_delta = mean(mean_cost_delta, na.rm = TRUE),
            lower = mean(lower_ci_delta, na.rm = TRUE),
            upper = mean(upper_ci_delta, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = year_id, y = mean_delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  labs(title = "Inpatient Cost Delta by Race Over Time",
       x = "Year", y = "Cost Difference (USD)", color = "Race") +
  theme_minimal(base_size = 13)

save_plot(plot_ip_race_time, "plot_ip_delta_cost_by_race_time.jpg")


library(scales)

plot_ip_race_time_faceted <- two_part_df %>%
  filter(toc == "IP", race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  mutate(year_id = as.integer(year_id)) %>%  # Fix decimal issue
  group_by(year_id, race_cd, acause) %>%
  summarise(
    mean_delta = mean(mean_cost_delta, na.rm = TRUE),
    lower = mean(lower_ci_delta, na.rm = TRUE),
    upper = mean(upper_ci_delta, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year_id, y = mean_delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Inpatient Cost Delta by Race Over Time, Faceted by Disease Category",
    x = "Year", y = "Cost Difference (USD)", color = "Race"
  ) +
  facet_wrap(~acause) +
  theme_minimal(base_size = 13)

save_plot(plot_ip_race_time_faceted, "plot_ip_delta_cost_by_race_time_faceted.jpg")


######
plot_ip_race_time_fixed <- two_part_df %>%
  filter(toc == "IP", race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  mutate(year_id = as.integer(year_id)) %>%
  group_by(year_id, race_cd, acause) %>%
  summarise(
    mean_delta = mean(mean_cost_delta, na.rm = TRUE),
    lower = mean(lower_ci_delta, na.rm = TRUE),
    upper = mean(upper_ci_delta, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year_id, y = mean_delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_x_continuous(breaks = seq(2008, 2019, 2)) +  #  Every other year
  facet_wrap(~acause) +
  labs(
    title = "Inpatient Cost Delta by Race Over Time, Faceted by Disease Category",
    x = "Year", y = "Cost Difference (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(size = 9))  # 🔠 Smaller font

save_plot(plot_ip_race_time_fixed, "plot_ip_delta_cost_by_race_time_facet_acause_cleaned.jpg")




##########


