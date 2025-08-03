######################################
# Title: B4_figures.R
# Description: Generates plots from the master table .csv file & from subtable csv files.
# Inputs: 05.Aggregation_Summary/bested/*.csv
# Outputs: 06.Figures/<date>/*.png
######################################

rm(list = ls())
pacman::p_load(ggplot2, readr, tidyverse, viridis, scales, ggsci)

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


####################################
# Functions for saving the plots 
####################################

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

####################################
# 04 two-parts estimates figures analysis
####################################

####################################
# 04 loading data and prep for vis
####################################

df_master <- read.csv(file = file.path(input_dir, "04.Two_Part_Estimates_inflation_adjusted_aggregated.csv"))
# This is the master analytic table for Aim 1 (most granular: cause, year, type of care, race, age group).

df_sub_age        <- read_csv(file.path(input_dir, "04.Two_Part_Estimates_subtable_by_age.csv"))
# Each row summarizes cost and incremental cost ("delta") by cause (lvl1/lvl2), stratified by age group, across all races, years, type of care, etc.

df_sub_cause      <- read_csv(file.path(input_dir, "04.Two_Part_Estimates_subtable_by_cause.csv"))
# Aggregated by disease cause (lvl1/lvl2) only, pooling across years, ages, race, and type of care. Shows overall average costs and deltas for each disease category.

df_sub_cause_year <- read_csv(file.path(input_dir, "04.Two_Part_Estimates_subtable_by_cause_year.csv"))
# Summarizes cost/deltas by cause and by year, so you can see temporal trends for each disease category.

df_sub_race       <- read_csv(file.path(input_dir, "04.Two_Part_Estimates_subtable_by_race.csv"))
# Summarizes cost and delta by race (and by cause lvl1/lvl2), pooling over years, age, type of care, etc.

df_sub_toc        <- read_csv(file.path(input_dir, "04.Two_Part_Estimates_subtable_by_toc.csv"))
# Summarizes by type of care (e.g., inpatient, outpatient, Rx), and disease.

df_sub_year       <- read_csv(file.path(input_dir, "04.Two_Part_Estimates_subtable_by_year.csv"))
# Aggregated by year and disease cause, pooling across age, race, and type of care.


# Remove upper / lower columns
df_sub_cause_nouplow <- df_sub_cause %>%
  select(-matches("upper|lower"))

##### Comparing Deltas across each level 2 category compared to mean

# Convert table to long
df_sub_cause_nouplow_long <- df_sub_cause_nouplow %>%
  pivot_longer(
    cols = starts_with("mean_delta"),
    names_to = "scenario",
    values_to = "delta"
  )

# Refactor the scenario column
df_sub_cause_nouplow_long$scenario <- factor(
  df_sub_cause_nouplow_long$scenario,
  levels = c("mean_delta_hiv", "mean_delta_sud", "mean_delta_hiv_sud"),
  labels = c("mean_delta_hiv", "mean_delta_sud", "mean_delta_hiv_sud")
)


####################################
# 04 figures
####################################

summary(df_master)
unique(df_master$acause_lvl2)
unique(df_master$toc)

df_long <- df_sub_cause %>%
  select(acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2,
         mean_delta_hiv, lower_ci_delta_hiv, upper_ci_delta_hiv,
         mean_delta_sud, lower_ci_delta_sud, upper_ci_delta_sud) %>%
  pivot_longer(
    cols = c(mean_delta_hiv, mean_delta_sud,
             lower_ci_delta_hiv, lower_ci_delta_sud,
             upper_ci_delta_hiv, upper_ci_delta_sud),
    names_to = c("stat", "scenario"),
    names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv|sud)",
    values_to = "value"
  ) %>%
  pivot_wider(
    names_from = stat,
    values_from = value
  )

delta_plot <- ggplot(df_long, aes(x = mean, y = reorder(cause_name_lvl2, mean), fill = scenario)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8), width = 0.7) +
  geom_errorbar(
    aes(xmin = lower_ci, xmax = upper_ci),
    width = 0.2,
    position = position_dodge(width = 0.8)
  ) +
  geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +
  scale_fill_jama(
    breaks = c("hiv", "sud"),
    labels = c("HIV", "SUD")
  ) +
  scale_x_continuous(
    breaks = c(-2500, 0, 2500, 7500, 12500),
    labels = scales::dollar_format()
  ) +
  labs(
    title = "Mean Delta per Level 2 Disease Category (HIV and SUD Only)",
    x = "Mean Delta (Change in Cost compared to No HIV & SUD, 2019 USD)",
    y = "Level 2 Disease Category",
    fill = "Scenario"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.y = element_text(size = 10),
    plot.title = element_text(hjust = 0.5)
  )

save_plot(delta_plot, "mean_delta_level2_comparison_HIV_SUD_only")



###______

# 1. Filter out RX type of care
df_master_filtered <- df_master %>%
  filter(toc != "RX")

# 2. Exclude specific causes by acause_lvl2 or cause_name_lvl2/cause_name_lvl1
# You can do this by disease group (acause_lvl2) or by descriptive name (cause_name_lvl2)
causes_to_exclude <- c(
  "_subs",         # Substance use disorders (likely coded as "_subs")
  "_mental",       # Mental disorders
  "hiv",           # HIV/AIDS (if you want to remove, or comment this out to keep HIV)
  "_well",         # Well care
  "mater_neonat"   # Maternal and neonatal
)

df_master_filtered <- df_master_filtered %>%
  filter(!acause_lvl2 %in% causes_to_exclude)


# Example: Continue with your plotting code, e.g.:
df_long <- df_master_filtered %>%
  select(acause_lvl1, cause_name_lvl1, acause_lvl2, cause_name_lvl2,
         mean_delta_hiv, lower_ci_delta_hiv, upper_ci_delta_hiv,
         mean_delta_sud, lower_ci_delta_sud, upper_ci_delta_sud) %>%
  pivot_longer(
    cols = c(mean_delta_hiv, mean_delta_sud,
             lower_ci_delta_hiv, lower_ci_delta_sud,
             upper_ci_delta_hiv, upper_ci_delta_sud),
    names_to = c("stat", "scenario"),
    names_pattern = "(mean|lower_ci|upper_ci)_delta_(hiv|sud)",
    values_to = "value"
  ) %>%
  pivot_wider(
    names_from = stat,
    values_from = value
  )

# Plot as before using df_long
delta_plot <- ggplot(df_long, aes(x = mean, y = reorder(cause_name_lvl2, mean), fill = scenario)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8), width = 0.7) +
  geom_errorbar(
    aes(xmin = lower_ci, xmax = upper_ci),
    width = 0.2,
    position = position_dodge(width = 0.8)
  ) +
  geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +
  scale_fill_jama(
    breaks = c("hiv", "sud"),
    labels = c("HIV", "SUD")
  ) +
  labs(
    title = "Mean Delta per Level 2 Disease Category (Filtered: No RX, No SUD/Mental/HIV/Well/Maternal)",
    x = "Mean Delta (Change in Cost compared to No HIV & SUD, 2019 USD)",
    y = "Level 2 Disease Category",
    fill = "Scenario"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.y = element_text(size = 10),
    plot.title = element_text(hjust = 0.5)
  )

save_plot(delta_plot, "mean_delta_level2_comparison_filtered")


