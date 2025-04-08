##----------------------------------------------------------------
##' Title: 03_aggregator_two_part_model.R
##'
##' Purpose:
##' This script aggregates and summarizes results from the two-part regression model 
##' for Medicare beneficiaries with HIV. It consolidates cost estimates across diseases, 
##' HIV status, and demographic groups.
##'
##' The script performs the following steps:
##'  1. Reads in all available CSV results from the two-part regression model.
##'  2. Computes mean incremental cost per disease and HIV status.
##'  3. Summarizes cost variations across race and demographic groups.
##'  4. Merges summary tables into a final structured output.
##'  5. Saves results to a dynamically created output directory.
##'
##' Inputs:
##'  - CSV files from the two-part model (`02.Two-Part_Results/`).
##'  - Includes metadata: year, age group, and race.
##'
##' Outputs:
##'  - Aggregated CSV (`two_part_model_summary.csv`) with cost comparisons.
##'  - All outputs are saved inside a date-stamped folder under `/output_aim1/YYYYMMDD/03.Table_2(two-part_summary)/`.
##'
##' Author: Bulat Idrisov
##' Date: 2025-03-06
##' Version: 1.0
##'
##----------------------------------------------------------------
# Environment setup
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,readr,purrr)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
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

##########################################
# Read in all csv files
##########################################
# Define base input directory
base_input_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1"

date_of_input_rx <- "20250403"
date_of_input_f2t <-"20250325"

# Get today's date in YYYYMMDD format
#today <- format(Sys.Date(), "%Y%m%d")
#defined manually
#today <- "20250325"

# Define input folder where CSV files are located
input_folder_f2t <- file.path(base_input_dir, date_of_input_f2t, "03.Two-Part_Results") #F2T
input_folder_rx <- file.path(base_input_dir, date_of_input_rx,"03.Rx_Two-Part_Results") #rx


# Define output directory
#Get today's date in YYYYMMDD format
today <- format(Sys.Date(), "%Y%m%d")

# Define dynamically created output directory
output_folder <- file.path(base_input_dir, today, "Agregator(two-part_summary)")

# Ensure output directory exists
dir.create(output_folder, recursive = TRUE, showWarnings = FALSE)

# List all CSV files in the input directory
#files_list <- list.files(input_folder, full.names = TRUE, pattern = "*.csv")
files_list_f2t <- list.files(input_folder_f2t, pattern = "\\.csv$", full.names = TRUE)
files_list_rx <- list.files(input_folder_rx, pattern = "\\.csv$", full.names = TRUE)


# Read all CSVs and combine them into one data frame
#df_input <- files_list_f2t %>%
 # map_dfr(read_csv)  # Reads and binds all files while keeping metadata columns

##############################
##############################
##############################
# Read aF2T files
df_f2t <- map_dfr(files_list_f2t, ~read_csv(.x, show_col_types = FALSE))

# Read rx files
df_rx <- map_dfr(files_list_rx, ~read_csv(.x, show_col_types = FALSE))

# Combine into one master table
df_input <- bind_rows(df_f2t, df_rx)


# **Ensure metadata columns exist**
if (!all(c("run_id", "toc", "year_id", "code_system", "age_group") %in% colnames(df_input))) {
  stop("One or more metadata columns are missing from the input files!")
}

figures_folder <- file.path(output_folder, "figures")

# Ensure figures directory exists
dir.create(figures_folder, recursive = TRUE, showWarnings = FALSE)

# Plotting Function
save_plot <- function(plot_obj, filename) {
  ggsave(filename = file.path(figures_folder, filename),
         plot = plot_obj, width = 9, height = 6, dpi = 300,
         device = "jpeg")
}

# Replace zeros with NA to avoid bias in averaging
df_input[df_input == 0] <- NA

##########################################
#  Summarize results: Compute mean incremental cost per disease and HIV status
##########################################


# Only keep years found in df_input
inflation_raw <- data.frame(
  year_id = 2008:2024,
  inflation_rate = c(0.001, 0.027, 0.015, 0.03, 0.017, 0.015, 0.008, 0.007, 0.021, 0.021,
                     0.019, 0.023, 0.014, 0.07, 0.065, 0.034, 0.029)
)

# Filter to match your df_input years
years_present <- sort(unique(df_input$year_id))
inflation_data <- inflation_raw %>%
  filter(year_id %in% years_present) %>%
  arrange(year_id) %>%
  mutate(
    inflation_factor = cumprod(1 + inflation_rate),
    deflator = inflation_factor / inflation_factor[year_id == 2019]  # normalize to 2019 dollars
  ) %>%
  select(year_id, deflator)

# Merge and adjust costs
df_input_adj <- df_input %>%
  left_join(inflation_data, by = "year_id")

cost_columns <- c("mean_cost_hiv_0", "lower_ci_cost_hiv_0", "upper_ci_cost_hiv_0",
                  "mean_cost_hiv_1", "lower_ci_cost_hiv_1", "upper_ci_cost_hiv_1",
                  "mean_cost_delta", "lower_ci_delta", "upper_ci_delta")

df_input_adj <- df_input_adj %>%
  mutate(across(all_of(cost_columns), ~ . / deflator))

# Create master table with inflation-adjusted, weighted averages
master_table <- df_input_adj %>%
  group_by(acause, year_id, toc, race_cd, age_group) %>%
  summarise(
    mean_cost_hiv_0     = weighted.mean(mean_cost_hiv_0, avg_bin_count, na.rm = TRUE),
    lower_ci_cost_hiv_0 = weighted.mean(lower_ci_cost_hiv_0, avg_bin_count, na.rm = TRUE),
    upper_ci_cost_hiv_0 = weighted.mean(upper_ci_cost_hiv_0, avg_bin_count, na.rm = TRUE),
    
    mean_cost_hiv_1     = weighted.mean(mean_cost_hiv_1, avg_bin_count, na.rm = TRUE),
    lower_ci_cost_hiv_1 = weighted.mean(lower_ci_cost_hiv_1, avg_bin_count, na.rm = TRUE),
    upper_ci_cost_hiv_1 = weighted.mean(upper_ci_cost_hiv_1, avg_bin_count, na.rm = TRUE),
    
    mean_cost_delta     = weighted.mean(mean_cost_delta, avg_bin_count, na.rm = TRUE),
    lower_ci_delta      = weighted.mean(lower_ci_delta, avg_bin_count, na.rm = TRUE),
    upper_ci_delta      = weighted.mean(upper_ci_delta, avg_bin_count, na.rm = TRUE),
    
    total_bin_count     = sum(avg_bin_count, na.rm = TRUE),
    .groups = "drop"
  )



# Save the aggregated results
output_file <- file.path(output_folder, "weighted_summary_two_part_table.csv")
write_csv(master_table, output_file)

cat("aster table saved as 'master_table_2019usd.csv' with all costs in 2019 dollars", output_file, "\n")




##########################################
# Plot 1: Average Delta by Type of Care and Year
##########################################
plot1 <- master_table %>%
  group_by(year_id, toc) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = year_id, y = delta, color = toc)) +
  geom_line(size = 1.2) +
  geom_point() +
  labs(title = "Incremental Cost for HIV+ vs HIV− by Type of Care",
       x = "Year", y = "Weighted Incremental Cost (USD)", color = "Type of Care") +
  theme_minimal()
save_plot(plot1, "plot1_delta_by_toc_year.png")
#add CI
#same as start wars chart 

##########################################
# Plot 2: Delta by Race and Disease
##########################################
plot2 <- master_table %>%
  group_by(acause, race_cd) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = race_cd, y = delta, fill = acause)) +
  geom_col(position = position_dodge()) +
  facet_wrap(~ acause) +
  labs(title = "Incremental Spending by Race and Disease Category",
       x = "Race/Ethnicity", y = "Incremental Cost (USD)", fill = "Disease") +
  theme_bw()
save_plot(plot2, "plot2_delta_by_race_and_disease.png")
### add CI 

##########################################
# Plot 3: Delta by Age and Race
##########################################
plot3 <- master_table %>%
  group_by(year_id, race_cd) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = age_group, y = delta, color = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  labs(title = "Spending Delta by Age and Race",
       x = "Age Group", y = "Incremental Cost (USD)", color = "Race") +
  theme_classic()
save_plot(plot3, "plot3_delta_by_age_and_race.png")
### add CI 
### make same but year instead of age group and also CI


plot3a <- master_table %>%
  group_by(year_id, race_cd) %>%
  summarise(delta = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = year_id, y = delta, color = race_cd)) +
  geom_line(size = 1.1) +
  geom_point() +
  labs(title = "Spending Delta by Year and Race",
       x = "Year", y = "Incremental Cost (USD)", color = "Race") +
  theme_classic()
save_plot(plot3a, "plot3a_delta_by_year_and_race.png")




##########################################
# Plot 5: Mean Cost by HIV Status Over Time (All Races)
##########################################
plot5 <- master_table %>%
  filter(race_cd == "all_race", year_id > 2008) %>%
  group_by(year_id) %>%
  summarise(
    hiv_neg = weighted.mean(mean_cost_hiv_0, total_bin_count, na.rm = TRUE),
    hiv_pos = weighted.mean(mean_cost_hiv_1, total_bin_count, na.rm = TRUE),
    ci_neg_l = weighted.mean(lower_ci_cost_hiv_0, total_bin_count, na.rm = TRUE),
    ci_neg_u = weighted.mean(upper_ci_cost_hiv_0, total_bin_count, na.rm = TRUE),
    ci_pos_l = weighted.mean(lower_ci_cost_hiv_1, total_bin_count, na.rm = TRUE),
    ci_pos_u = weighted.mean(upper_ci_cost_hiv_1, total_bin_count, na.rm = TRUE)
  ) %>%
  pivot_longer(cols = c(hiv_neg, hiv_pos), names_to = "hiv_status", values_to = "mean_cost") %>%
  mutate(
    lower_ci = if_else(hiv_status == "hiv_neg", ci_neg_l, ci_pos_l),
    upper_ci = if_else(hiv_status == "hiv_neg", ci_neg_u, ci_pos_u),
    hiv_status = recode(hiv_status, "hiv_neg" = "HIV-", "hiv_pos" = "HIV+")
  ) %>%
  ggplot(aes(x = year_id, y = mean_cost, color = hiv_status, fill = hiv_status)) +
  geom_line() +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci), alpha = 0.2, color = NA) +
  labs(title = "Mean Cost and CI by HIV Status Over Time (All Races)",
       x = "Year", y = "Mean Cost (2019 USD)") +
  theme_minimal()
save_plot(plot5, "plot5_cost_by_hiv_over_time.png")



##########################################
# Plot 6: Incremental Cost (HIV+ vs HIV−) Over Time (All Races)
##########################################
plot6 <- master_table %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id) %>%
  summarise(
    mean = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year_id, y = mean)) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.4) +
  labs(
    title = "Incremental Cost (HIV+ vs HIV−) Over Time (All Races)",
    x = "Year",
    y = "Incremental Cost (USD)"
  ) +
  theme_minimal()
save_plot(plot6, "plot6_cost_delta_all_race.png")

##########################################
# Plot 7: Incremental Cost by Race Over Time
##########################################
plot7 <- master_table %>%
  filter(race_cd %in% c("BLCK", "WHT", "HISP")) %>%
  group_by(year_id, race_cd) %>%
  summarise(
    mean = weighted.mean(mean_cost_delta, total_bin_count, na.rm = TRUE),
    lower = weighted.mean(lower_ci_delta, total_bin_count, na.rm = TRUE),
    upper = weighted.mean(upper_ci_delta, total_bin_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year_id, y = mean, color = race_cd)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  labs(
    title = "Incremental Cost by Race Over Time",
    x = "Year",
    y = "Incremental Cost (USD)",
    color = "Race"
  ) +
  theme_minimal()
save_plot(plot7, "plot7_cost_by_race_time.png")



##########################################
# Plot 8: Average Cost Delta Over Time by Type of Care (All Races)
##########################################
plot8 <- master_table %>%
  filter(race_cd == "all_race") %>%
  group_by(year_id, toc) %>%
  summarise(
    mean_delta = mean(mean_cost_delta, na.rm = TRUE),
    lower_ci = mean(lower_ci_delta, na.rm = TRUE),
    upper_ci = mean(upper_ci_delta, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year_id, y = mean_delta, color = toc)) +
  geom_line() +
  geom_ribbon(aes(ymin = lower_ci, ymax = upper_ci, fill = toc), alpha = 0.2, color = NA) +
  labs(
    title = "Average Cost Delta (HIV+ vs HIV-) Over Time by Type of Care",
    x = "Year", y = "Mean Cost Delta (2019 USD)", color = "Type of Care", fill = "Type of Care"
  ) +
  theme_minimal()
save_plot(plot8, "plot8_cost_delta_by_toc_over_time.png")

##########################################
# Plot 9: Average Spending by Type of Care and HIV Status
##########################################
df_long <- master_table %>%
  filter(race_cd == "all_race") %>%
  select(toc, mean_cost_hiv_0, mean_cost_hiv_1) %>%
  pivot_longer(cols = starts_with("mean_cost_hiv"),
               names_to = "hiv_status",
               values_to = "cost") %>%
  mutate(hiv_status = recode(hiv_status, "mean_cost_hiv_0" = "HIV-", "mean_cost_hiv_1" = "HIV+"))

plot9 <- df_long %>%
  group_by(toc, hiv_status) %>%
  summarise(mean_cost = mean(cost, na.rm = TRUE), .groups = "drop") %>%
  ggplot(aes(x = toc, y = mean_cost, fill = hiv_status)) +
  geom_col(position = "dodge") +
  scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
  labs(
    title = "Average Spending by Type of Care and HIV Status",
    x = "Type of Care", y = "Mean Cost (USD, 2019-adjusted)", fill = "HIV Status"
  ) +
  theme_minimal()
save_plot(plot9, "plot9_spending_by_toc_hiv_status.png")

##########################################
# Plot 10: Delta Cost by Race Over Time (Means)
##########################################
plot10 <- master_table %>%
  filter(race_cd %in% c("WHT", "BLCK", "HISP")) %>%
  group_by(year_id, race_cd) %>%
  summarise(
    mean_delta = mean(mean_cost_delta, na.rm = TRUE),
    lower = mean(lower_ci_delta, na.rm = TRUE),
    upper = mean(upper_ci_delta, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = year_id, y = mean_delta, color = race_cd)) +
  geom_line() +
  geom_point() +
  geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.3) +
  labs(
    title = "Delta Cost by Race Over Time",
    x = "Year", y = "Cost Difference (USD)", color = "Race"
  ) +
  theme_minimal()
save_plot(plot10, "plot10_delta_cost_by_race_time_means.png")


##########################################
# Plot 11: Mean Cost by Age Group and HIV Status (All Races)
##########################################
df_age <- master_table %>%
  filter(race_cd == "all_race", !is.na(age_group))

df_age_long <- df_age %>%
  select(age_group, mean_cost_hiv_0, mean_cost_hiv_1) %>%
  pivot_longer(
    cols = starts_with("mean_cost"),
    names_to = "hiv_status",
    values_to = "mean_cost"
  ) %>%
  mutate(
    hiv_status = recode(hiv_status,
                        "mean_cost_hiv_0" = "HIV-",
                        "mean_cost_hiv_1" = "HIV+")
  )

df_age_summary <- df_age_long %>%
  group_by(age_group, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop")

plot11 <- ggplot(df_age_summary, aes(x = factor(age_group), y = mean_cost, fill = hiv_status)) +
  geom_bar(stat = "identity", position = position_dodge()) +
  labs(
    title = "Average Cost by Age Group and HIV Status (All Races)",
    x = "Age Group",
    y = "Mean Cost (2019 USD)",
    fill = "HIV Status"
  ) +
  theme_minimal(base_size = 13)
save_plot(plot11, "plot11_cost_by_age_hiv_status.png")

##########################################
# Plot 12: Mean Cost Over Time by Disease and HIV Status
##########################################
df_long <- master_table %>%
  select(acause, year_id, mean_cost_hiv_0, mean_cost_hiv_1) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+"))

df_summary <- df_long %>%
  group_by(acause, year_id, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop")

plot12 <- ggplot(df_summary, aes(x = year_id, y = mean_cost, color = acause, linetype = hiv_status)) +
  geom_line(size = 1.1) +
  geom_point() +
  labs(title = "Mean Cost Over Time by Disease and HIV Status",
       y = "Mean Cost (2019 USD)", x = "Year") +
  theme_minimal()
save_plot(plot12, "plot12_cost_by_disease_and_hiv_time.png")

##########################################
# Plot 13: Mean Cost by Disease and Type of Care
##########################################
df_long <- master_table %>%
  select(acause, toc, mean_cost_hiv_0, mean_cost_hiv_1) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+"))

df_summary <- df_long %>%
  group_by(acause, toc, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop")

plot13 <- ggplot(df_summary, aes(x = toc, y = mean_cost, fill = acause)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Mean Cost by Disease and Type of Care",
       y = "Mean Cost (2019 USD)", x = "Type of Care") +
  theme_minimal()
save_plot(plot13, "plot13_cost_by_disease_and_toc.png")

##########################################
# Plot 14: Mean Cost by Disease and Race
##########################################
df_long <- master_table %>%
  select(acause, race_cd, mean_cost_hiv_0, mean_cost_hiv_1) %>%
  pivot_longer(cols = starts_with("mean_cost"),
               names_to = "hiv_status",
               names_prefix = "mean_cost_",
               values_to = "mean_cost") %>%
  mutate(hiv_status = recode(hiv_status, "hiv_0" = "HIV-", "hiv_1" = "HIV+"))

df_summary <- df_long %>%
  group_by(acause, race_cd, hiv_status) %>%
  summarise(mean_cost = mean(mean_cost, na.rm = TRUE), .groups = "drop")

plot14 <- ggplot(df_summary, aes(x = race_cd, y = mean_cost, fill = acause)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(title = "Mean Cost by Disease and Race",
       y = "Mean Cost (2019 USD)", x = "Race") +
  theme_minimal()
save_plot(plot14, "plot14_cost_by_disease_and_race.png")