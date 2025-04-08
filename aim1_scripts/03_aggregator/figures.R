

rm(list = ls())
pacman::p_load(ggsci,dplyr, openxlsx, RMySQL,data.table, ini, DBI, tidyr, readr,writexl, purrr, ggplot2, gridExtra, scales, ggpubr, patchwork, RColorBrewer)
#library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
# if("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
# library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
# suppressMessages(lbd.loader::load.containing.package())


#######

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
figures_folder <- file.path(base_input_dir, today, "Figures_tables")

# Create output directory if it doesn't exist
dir.create(figures_folder, recursive = TRUE, showWarnings = FALSE)


#function to save plots
save_plot <- function(plot_obj, filename) {
  ggsave(filename = file.path(figures_folder, filename),
         plot = plot_obj, width = 9, height = 6, dpi = 300,
         device = "jpeg")
}


jama_palette <- pal_jama("default")(7)


# ---- p1: Cost by HIV status, faceted by Type of Care ----
p1 <- summary_df %>%
  group_by(hiv_flag, toc) %>%
  summarise(median_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  mutate(hiv_status = ifelse(hiv_flag == 1, "HIV+", "HIV-")) %>%
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
  ggplot(aes(x = factor(age_group), y = log(avg_encounters_per_bene), fill = factor(age_group))) +
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
  ggplot(aes(x = factor(age_group), y = log(avg_encounters_per_bene), fill = factor(age_group))) +
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

save_plot(p8, "desc_scatter_encounters_vs_cost_by_toc.jpeg")


p9 <- ggplot(summary_df, aes(x = factor(age_group), y = avg_cost_per_bene, fill = factor(age_group))) +
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
  group_by(year_id, hiv_flag) %>%
  summarise(med_cost = median(avg_cost_per_bene, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    year_id = as.integer(year_id),
    hiv_status = ifelse(hiv_flag == 1, "HIV+", "HIV-")
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

two_part_df <- weighted_summary_two_part_table

plot1_modeled <- two_part_df %>%
  group_by(year_id, toc) %>%
  summarise(
    delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = delta, color = toc, group = toc)) +
  geom_line(size = 1.2) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
  scale_color_manual(values = jama_palette) +
  scale_x_continuous(breaks = seq(2008, 2019, 1)) +
  labs(
    title = "Incremental Cost (HIV+ vs HIV−) by Year and Type of Care",
    x = "Year", y = "Incremental Cost (USD)", color = "Type of Care"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot1_modeled, "model_delta_cost_by_year_toc.jpeg")

plot2_modeled <- two_part_df %>%
  group_by(acause, race_cd) %>%
  summarise(
    delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = race_cd, y = delta, fill = acause)) +
  geom_col(position = position_dodge(width = 0.9), color = "black") +
  geom_errorbar(aes(ymin = lower, ymax = upper),
                position = position_dodge(width = 0.9), width = 0.3) +
  facet_wrap(~ acause) +
  scale_fill_manual(values = jama_palette) +
  labs(
    title = "Modeled Incremental Cost (HIV+ vs HIV−) by Race and Disease",
    x = "Race/Ethnicity", y = "Incremental Cost (USD)", fill = "Disease"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot2_modeled, "model_delta_cost_by_race_and_disease.jpeg")

plot3_modeled <- two_part_df %>%
  group_by(age_group, race_cd) %>%
  summarise(
    delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(age_group), y = delta, color = race_cd, group = race_cd)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  scale_color_manual(values = jama_palette) +
  labs(
    title = "Modeled Incremental Cost (HIV+ vs HIV−) by Age Group and Race",
    x = "Age Group", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot3_modeled, "model_delta_cost_by_age_and_race.jpeg")



# Plot 4: Modeled Incremental Cost by Year, Race, and Type of Care (faceted)
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
  scale_color_manual(values = jama_palette) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Incremental Cost (HIV+ vs HIV−) by Year, Race, and Type of Care",
    subtitle = "Weighted difference in cost; faceted by type of care",
    x = "Year", y = "Incremental Cost (USD)", color = "Race"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot4_modeled, "model_delta_cost_by_year_race_facet_toc.jpeg")


# Plot 5: Modeled Average Incremental Cost Over Time (All Races)
plot5_modeled <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id) %>%
  summarise(
    mean = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = mean)) +
  geom_point(size = 3, color = jama_palette[1]) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.4, color = jama_palette[1]) +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = seq(min(two_part_df$year_id), max(two_part_df$year_id), 1)) +
  labs(
    title = "Modeled Incremental Cost Over Time (All Races)",
    subtitle = "HIV+ vs HIV−",
    x = "Year", y = "Incremental Cost (USD)"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot5_modeled, "model_delta_cost_over_time_all_races.jpeg")



# Plot 6: Modeled Average Cost Delta by Type of Care Over Time (All Races)
plot6_modeled_delta_by_toc <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id, toc) %>%
  summarise(
    mean_delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = as.integer(year_id), y = mean_delta, color = toc, group = toc)) +
  geom_line(size = 1.1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color = NA) +
  scale_color_manual(values = jama_palette) +
  scale_fill_manual(values = jama_palette) +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = seq(min(two_part_df$year_id), max(two_part_df$year_id), 1)) +
  labs(
    title = "Modeled Average Cost Delta by Type of Care Over Time",
    subtitle = "HIV+ vs HIV−, all races; weighted means with 95% CI ribbons",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Type of Care", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot6_modeled_delta_by_toc, "model_delta_cost_by_year_toc_all_races.jpeg")


# Plot 7: Modeled Average Cost Delta by Type of Care and Race Over Time
plot7_modeled_delta_by_toc_and_race_facet <- two_part_df %>%
  group_by(year_id, toc, race_cd) %>%
  summarise(
    mean_delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = factor(year_id), y = mean_delta, color = toc, group = toc)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color = NA) +
  scale_color_manual(values = jama_palette) +
  scale_fill_manual(values = jama_palette) +
  scale_y_continuous(labels = dollar_format()) +
  facet_wrap(~ race_cd) +
  labs(
    title = "Modeled Cost Delta by Type of Care and Race Over Time",
    subtitle = "Weighted difference in cost (HIV+ vs HIV−); 95% CI ribbons",
    x = "Year", y = "Mean Cost Delta (USD)",
    color = "Type of Care", fill = "Type of Care"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot7_modeled_delta_by_toc_and_race_facet, "model_delta_cost_by_year_toc_facet_race.jpeg")


###
# Define JAMA-style colors for HIV status
hiv_colors <- c("HIV-" = jama_palette[1], "HIV+" = jama_palette[2])
#jama_palette <- pal_jama("default")(7)

#  Modeled Average Cost by Type of Care and HIV Status
plot8_model_cost_by_toc_hiv <- two_part_df %>%
  filter(race_cd == "all_race") %>%
  pivot_longer(cols = starts_with("mean_cost_hiv"),
               names_to = "hiv_status",
               values_to = "cost") %>%
  mutate(hiv_status = recode(hiv_status, "mean_cost_hiv_0" = "HIV-", "mean_cost_hiv_1" = "HIV+")) %>%
  group_by(toc, hiv_status) %>%
  summarise(mean_cost = mean(cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = toc, y = mean_cost, fill = hiv_status)) +
  geom_col(position = "dodge", color = "black") +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Average Cost by Type of Care and HIV Status",
    x = "Type of Care", y = "Mean Cost (USD)", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot8_model_cost_by_toc_hiv, "model_cost_by_toc_and_hiv.jpeg")



#  Modeled Cost by Age Group and HIV Status
plot9_model_cost_by_age_hiv <- two_part_df %>%
  filter(race_cd == "all_race", !is.na(age_group)) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "mean_cost_hiv_0" = "HIV-", "mean_cost_hiv_1" = "HIV+")) %>%
  group_by(age_group, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(factor(age_group), mean_cost, fill = hiv_status)) +
  geom_bar(stat = "identity", position = position_dodge(), color = "black") +
  scale_fill_manual(values = hiv_colors) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Average Cost by Age Group and HIV Status (All Races)",
    x = "Age Group", y = "Mean Cost (USD)", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot9_model_cost_by_age_hiv, "model_cost_by_age_and_hiv.jpeg")



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



# Use JAMA palette for diseases
disease_colors<- c("HIV-" = jama_palette[1], "HIV+" = jama_palette[2])
# jama_palette <- pal_jama("default")(7)
# disease_colors <- jama_palette(length(unique(two_part_df$acause)))

# 📊 Plot 11: Modeled Mean Cost by Disease and Type of Care, Faceted by Race
plot11_model_cost_by_disease_toc_race <- two_part_df %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+")) %>%
  group_by(acause, toc, hiv_status, race_cd) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = toc, y = mean_cost, fill = acause)) +
  geom_bar(stat = "identity", position = "dodge", color = "black") +
  facet_wrap(~ race_cd) +
  scale_fill_manual(values = disease_colors) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Mean Cost by Disease and Type of Care (Faceted by Race)",
    x = "Type of Care", y = "Mean Cost (USD)", fill = "Disease Category"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot11_model_cost_by_disease_toc_race, "model_cost_by_disease_toc_race.jpeg")


# 📊 Plot 12: Histogram of Modeled Cost Delta (Inpatient Only)
plot12_ip_hist_cost_delta <- two_part_df %>%
  filter(toc == "IP") %>%
  ggplot(aes(x = mean_cost_delta)) +
  geom_histogram(binwidth = 1000, fill = jama_palette[1], color = "white") +
  scale_x_continuous(labels = dollar_format()) +
  labs(
    title = "Inpatient Mean Cost Delta (HIV+ minus HIV−)",
    x = "Cost Difference (USD)", y = "Number of Strata"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot12_ip_hist_cost_delta, "model_ip_hist_cost_delta.jpeg")


# 📊 Plot 13: Modeled Cost by Age Group and HIV Status (Inpatient Only)
plot13_ip_cost_by_age_hiv <- two_part_df %>%
  filter(toc == "IP", race_cd == "all_race") %>%
  select(age_group, mean_cost_hiv_0, mean_cost_hiv_1,
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
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Modeled Inpatient Cost by Age Group and HIV Status",
    x = "Age Group", y = "Mean Cost (USD)",
    color = "HIV Status", fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)

save_plot(plot13_ip_cost_by_age_hiv, "model_ip_cost_by_age_hiv.jpeg")




