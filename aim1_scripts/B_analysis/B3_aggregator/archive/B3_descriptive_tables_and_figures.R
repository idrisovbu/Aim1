#B3descriptive_tables_and_figures.R

# 
# This script compiles, summarizes, and visualizes Medicare claims analysis results across both descriptive and two-part modeled estimates for beneficiaries with and without HIV. It generates:
#   
#   Table 1: HIV prevalence summaries by age, type of care, and year (based on metadata)
# 
# Table 2: Stratified descriptive statistics (mean, median, IQR) for cost and utilization
# 
# Table 3: Modeled incremental cost summaries (from the two-part model)
# 
# Figures: Multiple visualizations including trends over time, differences by HIV status, race, age, and disease category
# 
# Outputs include:
#   
#   Formatted Excel tables for dissemination
# 
# High-resolution JAMA-styled plots (log-transformed where appropriate)
# 
# Model diagnostic summaries and p-value distributions


rm(list = ls())
pacman::p_load(ggsci,dplyr, openxlsx, RMySQL,data.table, ini, DBI, tidyr, readr,writexl, purrr, ggplot2, gridExtra, scales, ggpubr, patchwork, RColorBrewer)
#library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
# if("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
# library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
# suppressMessages(lbd.loader::load.containing.package())


#######
# Base analysis directory
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"

# Use today's date dynamically
date_of_input <- "20250624"

# Construct path to Agregators folder
agregators_folder <- file.path(base_dir, date_of_input, "Agregators")

# Read input files from Agregators folder
metadata_df <- fread(file.path(agregators_folder, "agregated_meta_table.csv"))
summary_df <- fread(file.path(agregators_folder, "weighted_summary_table.csv"))
two_part_df <- fread(file.path(agregators_folder, "weighted_summary_two_part_table.csv"))

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


# 1. Overall summary


summary_by_year <- metadata_df %>%
  group_by(year_id) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene, na.rm = TRUE),
    hiv_positive = sum(hiv_unique_bene, na.rm = TRUE),
    sud_positive = sum(sud_unique_bene, na.rm = TRUE),
    hcv_positive = sum(hepc_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    hiv_prevalence = hiv_positive / total_beneficiaries,
    sud_prevalence = sud_positive / total_beneficiaries,
    hcv_prevalence = hcv_positive / total_beneficiaries,
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    sud_prevalence_percent = percent(sud_prevalence, accuracy = 0.1),
    hcv_prevalence_percent = percent(hcv_prevalence, accuracy = 0.1)
  ) %>%
  select(
    year_id,
    total_beneficiaries,
    hiv_positive, hiv_prevalence_percent,
    sud_positive, sud_prevalence_percent,
    hcv_positive, hcv_prevalence_percent
  )


# Prevalence summary by year and type of care
summary_by_year_toc <- metadata_df %>%
  group_by(year_id, toc) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene, na.rm = TRUE),
    hiv_positive = sum(hiv_unique_bene, na.rm = TRUE),
    sud_positive = sum(sud_unique_bene, na.rm = TRUE),
    hcv_positive = sum(hepc_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    hiv_prevalence = hiv_positive / total_beneficiaries,
    sud_prevalence = sud_positive / total_beneficiaries,
    hcv_prevalence = hcv_positive / total_beneficiaries,
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    sud_prevalence_percent = percent(sud_prevalence, accuracy = 0.1),
    hcv_prevalence_percent = percent(hcv_prevalence, accuracy = 0.1)
  ) %>%
  select(
    year_id, toc,
    total_beneficiaries,
    hiv_positive, hiv_prevalence_percent,
    sud_positive, sud_prevalence_percent,
    hcv_positive, hcv_prevalence_percent
  )


# 2. Summary by age group
age_summary <- metadata_df %>%
  group_by(age_group_years_start) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene),
    hiv_positive_beneficiaries = sum(hiv_unique_bene),
    hiv_prevalence = hiv_positive_beneficiaries / total_beneficiaries
  ) %>%
  mutate(
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    category = "Age Group",
    subgroup = as.character(age_group_years_start)
  ) %>%
  select(-hiv_prevalence)

# 3. Summary by type of care
toc_summary <- metadata_df %>%
  group_by(toc) %>%
  summarise(
    total_beneficiaries = sum(total_unique_bene, na.rm = TRUE),
    hiv_positive = sum(hiv_unique_bene, na.rm = TRUE),
    sud_positive = sum(sud_unique_bene, na.rm = TRUE),
    hcv_positive = sum(hepc_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    hiv_prevalence = hiv_positive / total_beneficiaries,
    sud_prevalence = sud_positive / total_beneficiaries,
    hcv_prevalence = hcv_positive / total_beneficiaries,
    hiv_prevalence_percent = percent(hiv_prevalence, accuracy = 0.1),
    sud_prevalence_percent = percent(sud_prevalence, accuracy = 0.1),
    hcv_prevalence_percent = percent(hcv_prevalence, accuracy = 0.1),
    category = "Type of Care",
    subgroup = toc
  ) %>%
  select(category, subgroup,
         total_beneficiaries,
         hiv_positive, hiv_prevalence_percent,
         sud_positive, sud_prevalence_percent,
         hcv_positive, hcv_prevalence_percent)



####
# Save each summary as separate CSV files
fwrite(summary_by_year, file.path(output_folder, "table1a_summary_by_year.csv"))
fwrite(summary_by_year_toc, file.path(output_folder, "table1b_summary_by_year_toc.csv"))
fwrite(age_summary, file.path(output_folder, "table1c_summary_by_age.csv"))
fwrite(toc_summary, file.path(output_folder, "table1d_summary_by_toc.csv"))

cat("✅ Table 1 CSV files saved to:", output_folder, "\n")


#####



# Note: Total beneficiary counts represent stratified records by age group, type of care, year,
# and coding system. Individuals may appear in multiple subgroups. HIV prevalence is calculated 
# as the proportion of beneficiaries with HIV-coded claims within each stratum.
##########################################

# colnames(summary_df)
# summary(summary_df)
# Define output path
#table1a_outfile <- file.path(figures_folder, "table1a_summary_statistics.csv")

# Updated summarizer to align with actual column names
summarize_table <- function(df, group_var) {
  df %>%
    group_by({{ group_var }}) %>%
    summarise(
      median_avg_cost = median(avg_cost_per_bene, na.rm = TRUE),
      mean_avg_cost = mean(avg_cost_per_bene, na.rm = TRUE),
      iqr_avg_cost = IQR(avg_cost_per_bene, na.rm = TRUE),
      
      median_max_cost = median(max_cost_per_bene, na.rm = TRUE),
      mean_max_cost = mean(max_cost_per_bene, na.rm = TRUE),
      iqr_max_cost = IQR(max_cost_per_bene, na.rm = TRUE),
      
      median_quant99 = median(quantile_99_cost_per_bene, na.rm = TRUE),
      mean_quant99 = mean(quantile_99_cost_per_bene, na.rm = TRUE),
      iqr_quant99 = IQR(quantile_99_cost_per_bene, na.rm = TRUE),
      
      median_total_cost = median(sum_cost_per_group, na.rm = TRUE),
      mean_total_cost = mean(sum_cost_per_group, na.rm = TRUE),
      iqr_total_cost = IQR(sum_cost_per_group, na.rm = TRUE),
      
      median_avg_encounters = median(avg_encounters_per_bene, na.rm = TRUE),
      mean_avg_encounters = mean(avg_encounters_per_bene, na.rm = TRUE),
      iqr_avg_encounters = IQR(avg_encounters_per_bene, na.rm = TRUE),
      
      median_total_encounters = median(sum_encounters_per_group, na.rm = TRUE),
      iqr_total_encounters = IQR(sum_encounters_per_group, na.rm = TRUE),
      
      median_total_bene = median(total_unique_bene, na.rm = TRUE),
      iqr_total_bene = IQR(total_unique_bene, na.rm = TRUE),
      .groups = "drop"
    )
}

# Run stratified summaries
table_2a <- summarize_table(summary_df, has_hiv)
table_2b <- summarize_table(summary_df, has_sud)
table_2c <- summarize_table(summary_df, has_hepc)
table_2d <- summarize_table(summary_df, acause)
table_2e <- summarize_table(summary_df, year_id)
table_2f <- summarize_table(summary_df, race_cd)
table_2g <- summarize_table(summary_df, toc)
table_2h <- summarize_table(summary_df, age_group_years_start)

# Save each summary to CSV in the tablesandfigures folder
fwrite(table_2a, file.path(output_folder, "table2a_by_hiv_status.csv"))
fwrite(table_2b, file.path(output_folder, "table2b_by_sud_status.csv"))
fwrite(table_2c, file.path(output_folder, "table2c_by_hepc_status.csv"))
fwrite(table_2d, file.path(output_folder, "table2d_by_disease.csv"))
fwrite(table_2e, file.path(output_folder, "table2e_by_year.csv"))
fwrite(table_2f, file.path(output_folder, "table2f_by_race.csv"))
fwrite(table_2g, file.path(output_folder, "table2g_by_type_of_care.csv"))
fwrite(table_2h, file.path(output_folder, "table2h_by_age_group.csv"))

cat("Table 2 CSV summaries saved to:", output_folder, "\n")


# # Helper functions
# 
# summarize_table <- function(df, group_var) {
#   df %>%
#     group_by({{ group_var }}) %>%
#     summarise(
#       median_avg_cost = median(avg_cost_per_bene, na.rm = TRUE),
#       mean_avg_cost = mean(avg_cost_per_bene, na.rm = TRUE),
#       iqr_avg_cost = IQR(avg_cost_per_bene, na.rm = TRUE),
#       
#       median_max_cost = median(max_cost_per_bene, na.rm = TRUE),
#       mean_max_cost = mean(max_cost_per_bene, na.rm = TRUE),
#       iqr_max_cost = IQR(max_cost_per_bene, na.rm = TRUE),
#       
#       median_quant99 = median(quant99, na.rm = TRUE),
#       mean_quant99 = mean(quant99, na.rm = TRUE),
#       iqr_quant99 = IQR(quant99, na.rm = TRUE),
#       
#       median_total_cost = median(total_cost, na.rm = TRUE),
#       mean_total_cost = mean(total_cost, na.rm = TRUE),
#       iqr_total_cost = IQR(total_cost, na.rm = TRUE),
#       
#       median_avg_encounters = round(median(avg_encounters_per_bene, na.rm = TRUE), 2),
#       mean_avg_encounters = round(mean(avg_encounters_per_bene, na.rm = TRUE), 2),
#       iqr_avg_encounters = round(IQR(avg_encounters_per_bene, na.rm = TRUE), 2),
#       
#       median_total_encounters = median(total_encounters, na.rm = TRUE),
#       iqr_total_encounters = IQR(total_encounters, na.rm = TRUE),
#       
#       median_total_bene = median(total_unique_bene, na.rm = TRUE),
#       iqr_total_bene = IQR(total_unique_bene, na.rm = TRUE),
#       
#       mean_any_cost = percent(mean(mean_any_cost, na.rm = TRUE), accuracy = 0.1),
#       .groups = "drop"
#     )
# }
# 
# 
# 
# # Run stratified summaries
# table_1b <- summarize_table(summary_df, has_hiv)
# table_1c <- summarize_table(summary_df, acause)
# table_1d <- summarize_table(summary_df, year_id)
# table_1e <- summarize_table(summary_df, race_cd)
# table_1f <- summarize_table(summary_df, toc)
# table_1g <- summarize_table(summary_df, age_group_years_start)
# 
# # Name each as a sheet
# table_list <- list(
#   "By HIV Status" = table_1b,
#   "By Disease" = table_1c,
#   "By Year" = table_1d,
#   "By Race" = table_1e,
#   "By Type of Care" = table_1f,
#   "By Age Group" = table_1g
# )
# 
# # Save all sheets to one Excel file
# write_xlsx(table_list, path = file.path(figures_folder, "table2_stratified_summaries.xlsx"))
# 
# cat("âœ… All stratified Table 1 summaries saved to one Excel file with separate tabs.\n")
# 



####


jama_palette <- pal_jama("default")(7)


# ---- p1: Cost by HIV status, faceted by Type of Care ----
p1 <- summary_df %>%
  group_by(has_hiv, toc) %>%
  summarise(median_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  mutate(hiv_status = ifelse(has_hiv == 1, "HIV+", "HIV-")) %>%
  ggplot(aes(x = hiv_status, y = median_cost, fill = hiv_status)) +
  geom_col(color = "black") +
  facet_wrap(~ toc) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_fill_manual(values = jama_palette[1:2]) +
  labs(
    title = "Median Cost per Beneficiary by HIV Status and Type of Care",
    x = "HIV Status", y = "Cost (USD)", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(p1, "desc_cost_by_hiv_status_facet_toc.jpeg")


# ---- p2: Cost by Year, faceted by Type of Care ----
p2 <- summary_df %>%
  group_by(year_id, toc) %>%
  summarise(median_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = year_id, y = median_cost)) +
  geom_line(color = jama_palette[3], linewidth = 1.2) +
  geom_point(color = jama_palette[3], size = 2) +
  facet_wrap(~ toc) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_x_continuous(breaks = seq(2008, 2019, 1)) +
  labs(
    title = "Median Cost per Beneficiary by Year and Type of Care",
    x = "Year", y = "Cost (USD)"
  ) +
  theme_minimal(base_size = 13)

save_plot(p2, "desc_cost_by_year_facet_toc.jpeg")



p3 <- summary_df %>%
  ggplot(aes(x = avg_cost_per_bene)) +
  geom_histogram(binwidth = 1000, fill = jama_palette[4], color = "white") +
  scale_x_continuous(labels = dollar_format(), limits = c(0, 50000)) +
  #facet_wrap(~ toc) +
  labs(title = "Distribution of Average Cost per Beneficiary",
       x = "Avg Cost (USD)", y = "Count") +
  theme_minimal(base_size = 13)

save_plot(p3, "desc_hist_avg_cost_per_bene.jpeg")

# mean(summary_df$avg_cost_per_bene)
# median(summary_df$avg_cost_per_bene)

# Count distinct diseases to set palette size
n_acause <- summary_df %>% distinct(acause) %>% nrow()

p4 <- summary_df %>%
  group_by(acause) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = reorder(acause, avg_cost_per_bene), y = avg_cost_per_bene, fill = acause)) +
  geom_bar(stat = "identity") +
  geom_text(aes(label = scales::dollar(avg_cost_per_bene)),
            hjust = 1.1, color = "white", size = 4) +
  scale_fill_manual(values = rep(jama_palette, length.out = n_acause)) +
  coord_flip() +
  labs(
    title = "Average Cost per Beneficiary by Disease Category",
    x = "Disease Category", y = "Average Cost (2019 USD)"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(p4, "desc_cost_by_disease_labeled_jama.jpeg")



p5 <- summary_df %>%
  group_by(toc, race_cd) %>%
  summarise(median_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = reorder(toc, median_cost), y = median_cost, fill = toc)) +
  geom_col(color = "black") +
  coord_flip() +
  facet_wrap(~ race_cd) +
  scale_y_continuous(labels = scales::dollar_format()) +
  scale_fill_manual(values = jama_palette) +
  labs(
    title = "Median Cost per Beneficiary by Type of Care and Disease Category",
    x = "Type of Care", y = "Median Cost (USD)"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(p5, "desc_cost_by_toc_facet_disease.jpeg")



p6a <- summary_df %>%
  ggplot(aes(x = factor(age_group_years_start), y = log(avg_encounters_per_bene), fill = factor(age_group_years_start))) +
  geom_boxplot(color = "black") +
  facet_wrap(~ acause, scales = "free_y") +
  scale_fill_manual(values = jama_palette) +
  labs(
    title = "Log Average Encounters per Beneficiary by Age Group and Disease Category",
    x = "Age Group", y = "Log(Average Encounters)", fill = "Age Group"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(p6a, "desc_log_encounters_by_agegroup_acause.jpeg")


p7<- summary_df %>%
  ggplot(aes(x = factor(age_group_years_start), y = log(avg_encounters_per_bene), fill = factor(age_group_years_start))) +
  geom_boxplot(color = "black") +
  facet_wrap(~ toc) +
  scale_fill_manual(values = jama_palette) +
  labs(
    title = "Log Average Encounters per Beneficiary by Age Group and Type of Care",
    x = "Age Group", y = "Log(Avg Encounters)", fill = "Age Group"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(p7, "desc_log_encounters_by_agegroup_toc.jpeg")




####
p8 <- summary_df %>%
  ggplot(aes(x = avg_encounters_per_bene,
             y = avg_cost_per_bene,
             color = toc)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_y_log10(labels = dollar_format()) +
  scale_x_log10() +
  scale_color_manual(values = jama_palette) +
  labs(
    title = "Encounters vs Average Cost (log-log scale)",
    subtitle = "Colored by Type of Care",
    x = "Avg Encounters per Beneficiary",
    y = "Avg Cost (USD, log scale)",
    color = "Type of Care"
  ) +
  theme_minimal(base_size = 13)

###
p8b <- summary_df %>%
  ggplot(aes(x = avg_encounters_per_bene,
             y = avg_cost_per_bene,
             color = toc)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_y_log10(labels = dollar_format()) +
  scale_x_log10() +
  scale_color_manual(values = jama_palette) +
  facet_wrap(~ has_hiv, labeller = labeller(has_hiv = c("0" = "HIV-", "1" = "HIV+"))) +
  labs(
    title = "Encounters vs Average Cost (log-log scale), by HIV Status",
    subtitle = "Colored by Type of Care",
    x = "Avg Encounters per Beneficiary",
    y = "Avg Cost (USD, log scale)",
    color = "Type of Care"
  ) +
  theme_minimal(base_size = 13)


save_plot(p8b, "desc_scatter_encounters_vs_cost_by_toc.jpeg")


p9 <- ggplot(summary_df, aes(x = factor(age_group_years_start), y = avg_cost_per_bene, fill = factor(age_group_years_start))) +
  geom_boxplot(color = "black") +
  scale_y_log10(labels = dollar_format()) +
  facet_wrap(~race_cd) +
  scale_fill_manual(values = jama_palette) +
  labs(
    title = "Average Cost per Beneficiary by Age Group (Faceted by Race)",
    x = "Age Group", y = "Avg Cost (USD, log scale)", fill = "Age"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(p9, "desc_cost_by_agegroup_facet_race.jpeg")




p10 <- summary_df %>%
  group_by(toc) %>%
  summarise(
    avg_cost_per_bene = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = reorder(toc, avg_cost_per_bene), y = avg_cost_per_bene, fill = toc)) +
  geom_bar(stat = "identity", color = "black") +
  geom_text(aes(label = dollar(avg_cost_per_bene)), vjust = -0.3, size = 4) +
  scale_y_log10(labels = dollar_format()) +
  scale_fill_manual(values = jama_palette) +
  labs(
    title = "Average Cost per Beneficiary by Type of Care",
    x = "Type of Care", y = "Average Cost (USD, log scale)", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13) +
  theme(legend.position = "none")

save_plot(p10, "desc_cost_by_toc_log_labeled.jpeg")




p11 <- summary_df %>%
  group_by(year_id, has_hiv) %>%
  summarise(med_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    year_id = as.integer(year_id),
    hiv_status = ifelse(has_hiv == 1, "HIV+", "HIV-")
  ) %>%
  ggplot(aes(x = year_id, y = med_cost, color = hiv_status, group = hiv_status)) +
  geom_line(linewidth = 1.2) +
  geom_point(size = 2) +
  scale_x_continuous(breaks = seq(2008, 2019, 1)) +
  scale_y_continuous(labels = dollar_format()) +
  scale_color_manual(values = jama_palette[1:2]) +
  labs(
    title = "Median Cost per Beneficiary by Year and HIV Status",
    x = "Year", y = "Median Cost (USD)", color = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(p11, "desc_cost_by_year_and_hiv_status.jpeg")


p12 <- summary_df %>%
  group_by(year_id, toc) %>%
  summarise(
    total_bene = sum(total_unique_bene, na.rm = TRUE),
    avg_cost = weighted.mean(avg_cost_per_bene, total_unique_bene, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(year_id = as.integer(year_id)) %>%
  ggplot(aes(x = year_id, y = avg_cost, size = total_bene, fill = toc)) +
  geom_point(shape = 21, color = "black", alpha = 0.85) +
  scale_y_continuous(labels = dollar_format()) +
  scale_size_continuous(range = c(3, 15), name = "Total Beneficiaries", labels = comma_format()) +
  scale_fill_manual(values = jama_palette, name = "Type of Care") +
  scale_x_continuous(breaks = seq(2008, 2019, 1)) +
  labs(
    title = "Average Cost per Beneficiary by Year and Type of Care",
    subtitle = "Bubble size reflects sample size",
    x = "Year", y = "Average Cost (USD)"
  ) +
  theme_minimal(base_size = 13)

save_plot(p12, "desc_cost_by_year_bubble_toc.jpeg")




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


