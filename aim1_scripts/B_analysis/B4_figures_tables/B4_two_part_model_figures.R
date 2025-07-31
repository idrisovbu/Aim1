######################################
# Title: B4_two_part_model_figures.R
# Description: Generates plots from the master table .csv file (weighted_summary_two_part_table_master.csv) & from subtable csv files
#
# Inputs: weighted_summary_two_part_table_master.csv, subtable_by_age/cause/cause_lvl2/cause_year/race/toc/year.csv
# Outputs: plot PNG files
######################################

# Clear environment
rm(list = ls())

# Load packages
pacman::p_load(ggplot2, readr, tidyverse, viridis, scales,ggsci)


####################################
# Set directories if on own PC
####################################
#date_today <- format(Sys.time(), "%Y%m%d")
#input_dir <- "C:/Users/aches/Desktop/Stuff/Bulat PhD/Data/" # Set as necessary
#output_dir <- paste0("C:/Users/aches/Desktop/Stuff/Bulat PhD/Plots/", date_today, "/") # Set as necessary

####################################
# Set directories working on IHME enviroment
####################################

####################################
# Input directories

input_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/bested/aggregation_subtable_results"

input_master_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/05.Aggregation_Summary/bested/aggregation_results"


####################################
# Output directories


date_today <- format(Sys.time(), "%Y%m%d")

# Define the base directory (the parent directory)
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis/"  # Added leading slash

figures_dir <- file.path(base_dir, "06.Figures")
if (!dir.exists(figures_dir)) {
  dir.create(figures_dir)
}

# Define the output directory inside 06.Figures with today's date
output_dir <- file.path(figures_dir, date_today)

# Create the output directory if it doesn't exist
if (!dir.exists(output_dir)) {
  dir.create(output_dir)
}


####################################
# Load data
####################################

df_master <- read.csv(file = file.path(input_master_dir, "weighted_summary_two_part_table_master.csv"))
# This is the **master analytic table for Aim 1**.
# Each row is a unique combination: cause (lvl2/lvl1), year, type of care, race, age group.
# Columns include: mean cost, deltas for HIV/SUD/HIV+SUD, CIs, and 'total_bin_count' (number of beneficiaries in each stratum).
# Use for any stratified analysis or when you want the highest granularity possible for Aim 1 figures or tables.

df_sub_age <- read.csv(file = file.path(input_dir, "subtable_by_age.csv"))
# Each row summarizes cost and incremental cost ("delta") by cause (lvl1/lvl2), **stratified by age group**, across all races, years, type of care, etc. 
# Used to analyze age patterns in disease-attributable spending and deltas for Aim 1.

df_sub_cause <- read.csv(file = file.path(input_dir, "subtable_by_cause.csv"))
# This table is **aggregated by disease cause** (lvl1/lvl2) only, pooling across years, ages, race, and type of care.
# Shows overall average costs and deltas for each disease category, key for ranking diseases by incremental costs.

df_sub_cause_lvl2 <- read.csv(file = file.path(input_dir, "subtable_by_cause_lvl2.csv"))
# (You may not actually produce this unless you create it separately) — Would be by lvl2 cause (most granular disease grouping), aggregating over all other variables.

df_sub_cause_year <- read.csv(file = file.path(input_dir, "subtable_by_cause_year.csv"))
# Summarizes cost/deltas **by cause and by year**, so you can see **temporal trends for each disease category**.

df_sub_race <- read.csv(file = file.path(input_dir, "subtable_by_race.csv"))
# Summarizes cost and delta **by race** (and by cause lvl1/lvl2), pooling over years, age, type of care, etc.
# Used for analyzing disparities or trends by race.

df_sub_toc <- read.csv(file = file.path(input_dir, "subtable_by_toc.csv"))
# Summarizes by **type of care** (e.g., inpatient, outpatient, Rx), and disease.
# Used for comparing how disease-attributable costs differ by healthcare setting.

df_sub_year <- read.csv(file = file.path(input_dir, "subtable_by_year.csv"))
# Aggregated by year and disease cause, pooling across age, race, and type of care.
# Used to look at broad **yearly trends** in disease-attributable costs.



####################################
# Functions
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
# Create visualizations
####################################

###################
# df_sub_cause
###################

summary(df_sub_cause)

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

# Plot
# delta <- ggplot(df_sub_cause_nouplow_long, aes(x = delta, y = reorder(cause_name_lvl2, delta), fill = scenario)) +
#   geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
#   geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +  # Center line at 0
#   scale_fill_manual(
#     values = c("mean_delta_hiv" = "red", "mean_delta_sud" = "blue", "mean_delta_hiv_sud" = "green"),
#     breaks = c("mean_delta_hiv_sud", "mean_delta_hiv", "mean_delta_sud"),  # raw names
#     labels = c("HIV + SUD", "HIV", "SUD")  # display names
#   ) +
#   scale_x_continuous(
#     breaks = c(-2500, 0, 2500, 7500, 12500),  # Custom breaks
#     labels = dollar_format()                  # Format as dollars
#   ) +
#   labs(
#     title = "Mean Delta per Level 2 Disease Category by Scenario",
#     x = "Mean Delta (Change in Cost compared to No HIV & SUD, 2019 USD)",
#     y = "Level 2 Disease Category",
#     fill = "Scenario"
#   ) +
#   theme_minimal(base_size = 14) +
#   theme(
#     axis.title.x = element_text(margin = margin(t = 10)),
#     axis.title.y = element_text(margin = margin(r = 10)),
#     axis.text.y = element_text(size = 10),
#     plot.title = element_text(hjust = 0.5)
#   )

delta <- ggplot(df_sub_cause_nouplow_long, aes(x = delta, y = reorder(cause_name_lvl2, delta), fill = scenario)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.8)) +
  geom_vline(xintercept = 0, color = "gray50", linetype = "dashed") +
  scale_fill_jama(
    breaks = c("mean_delta_hiv_sud", "mean_delta_hiv", "mean_delta_sud"),
    labels = c("HIV + SUD", "HIV", "SUD")
  ) +
  scale_x_continuous(
    breaks = c(-2500, 0, 2500, 7500, 12500),
    labels = scales::dollar_format()
  ) +
  labs(
    title = "Mean Delta per Level 2 Disease Category by Scenario",
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


# Output
save_plot(delta, "mean_delta_level2_comparison")

#######
# Same as above but without HIV+SUD but with 


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






##### Adjusted Weighted Mean Cost per Level 2 Disease Category
# plot just shows mean cost across all level 2 disease categories

ggplot(df_sub_cause, aes(x = mean_cost_hiv, y = reorder(cause_name_lvl2, mean_cost_hiv), fill = mean_cost_hiv)) +
  geom_bar(stat = "identity", width = 0.5) +
  
  # Dollar labels to the right of the bar, black text, no outline
  geom_text(
    aes(label = dollar(mean_cost_hiv, accuracy = 0.01)),
    hjust = -0.1,      # Just outside the bar on the right
    color = "black",
    size = 3.5
  ) +
  
  # Green to red gradient
  scale_fill_gradientn(
    colors = c("green", "red"),
    values = scales::rescale(c(0, 6000, 17500)),
    limits = c(0, 17500)
  ) +
  
  labs(
    title = "Adjusted Weight Mean Cost per Level 2 Disease Category",
    x = "Adjusted Weight Mean Cost per Beneficiary with HIV (2019 USD)",
    y = "Level 2 Disease Category"
  ) +
  
  theme_minimal(base_size = 14) +
  theme(
    panel.background = element_blank(),
    plot.background = element_blank(),
    panel.grid.major.x = element_line(color = "gray90"),  # Keep vertical grid lines
    panel.grid.major.y = element_blank(),                 # Remove horizontal grid lines
    panel.grid.minor = element_blank(),
    axis.ticks = element_line(color = "gray70"),
    legend.position = "none",
    plot.title = element_text(hjust = 0.5, size = 16),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10))
  ) +
  
  scale_x_continuous(
    limits = c(0, 20000),   # Extend max x-axis to 20000
    expand = c(0, 0),       # No extra expansion (limits control space)
    breaks = c(0, 2500, 5000, 7500, 10000, 12500, 15000, 17500),
    labels = dollar_format()
  ) +
  
  coord_cartesian(clip = "off")   # Allow text outside plot area

# Output
save_plot(all_mean, "all_mean_level2_comparison")

###################
# df_sub_year
###################

# Looking at HIV delta

hiv_delta <- ggplot(df_sub_year, aes(x = year_id, y = mean_cost_hiv, color = cause_name_lvl2, group = cause_name_lvl2)) +
  geom_line(size = 1.2) +  # Trend lines
  geom_point(size = 2) +   # Points at each year
  labs(
    title = "Delta Change over Time comparing Non-HIV Non-USD to HIV costs for Level 2 Disease",
    x = "Year",
    y = "Delta (2019 USD)",
    color = "Level 2 Disease Category"
  ) +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = 2008:2019, limits = c(2008, 2019)) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "plain", hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    panel.grid.major = element_line(color = "gray90"),
    panel.grid.minor = element_blank()
  ) +
  guides(color = guide_legend(ncol = 1))  # Force single column legend

# Output
save_plot(hiv_delta, "hiv_delta_over_time")


# Looking at HIV+SUD delta

ggplot(df_sub_year, aes(x = year_id, y = mean_cost_hiv_sud, color = cause_name_lvl2, group = cause_name_lvl2)) +
  geom_line(size = 1.2) +  # Trend lines
  geom_point(size = 2) +   # Points at each year
  labs(
    title = "Delta Change over Time comparing Non-HIV to HIV costs for Level 2 Disease",
    x = "Year",
    y = "Delta (2019 USD)",
    color = "Level 2 Disease Category"
  ) +
  scale_y_continuous(labels = dollar_format()) +
  scale_x_continuous(breaks = 2008:2019, limits = c(2008, 2019)) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "plain", hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    panel.grid.major = element_line(color = "gray90"),
    panel.grid.minor = element_blank()
  ) +
  guides(color = guide_legend(ncol = 1))  # Force single column legend


###################
# df_sub_age
###################

# Looking at specific age groups to see spending on hiv

ggplot(
  data = filter(df_sub_age, age_group_years_start == 85),
  aes(x = reorder(cause_name_lvl2, cause_name_lvl2), y = mean_cost_hiv)
) +
  geom_bar(stat = "identity", fill = "steelblue") +
  labs(
    title = "Spending per Beneficiary, HIV, Ages 65-69",
    x = "Level 2 Disease Category",
    y = "Spending per Disease Category per Beneficiary (2019 USD)"
  ) +
  scale_y_continuous(
    limits = c(0, 20000),
    breaks = c(5000, 10000, 15000, 20000),
    labels = dollar_format()  # Adds $ formatting
  ) +
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "plain", hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.x = element_text(angle = 45, hjust = 1)
  )

# Looking at all age groups to see spending on hiv, percentage wise

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
  scale_y_continuous(labels = percent_format(scale = 1)) +  # Format y-axis as %
  theme_minimal(base_size = 14) +
  theme(
    plot.title = element_text(face = "plain", hjust = 0.5),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    axis.text.x = element_text(angle = 45, hjust = 1)
  ) +
  scale_fill_brewer(palette = "Set3")  # Nice colors for age groups

# Output
save_plot(hiv_percent_age, "hiv_percent_age")




hiv_percent_race <- ggplot(
  data = df_sub_race %>%
    group_by(cause_name_lvl2) %>%
    mutate(percent_mean_cost_hiv = mean_cost_hiv / sum(mean_cost_hiv, na.rm = TRUE) * 100) %>%
    ungroup(),
  aes(
    x = reorder(cause_name_lvl2, cause_name_lvl2),
    y = percent_mean_cost_hiv,
    fill = factor(race_cd)  # or use your recoded variable for prettier legend
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

