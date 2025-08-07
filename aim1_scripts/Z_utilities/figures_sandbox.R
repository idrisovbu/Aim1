library(viridis)

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


#########

plot_mean_cost_by_race_facet_toc <- df_master %>%
  group_by(race_cd, toc) %>%
  summarise(
    mean_cost = weighted.mean(mean_cost, total_row_count, na.rm = TRUE),
    lower_ci = weighted.mean(lower_ci, total_row_count, na.rm = TRUE),
    upper_ci = weighted.mean(upper_ci, total_row_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  ggplot(aes(x = reorder(race_cd, mean_cost), y = mean_cost, fill = race_cd)) +
  geom_col(position = "dodge", color = "black") +
  geom_errorbar(aes(ymin = lower_ci, ymax = upper_ci), width = 0.22, position = position_dodge(width = 0.9)) +
  scale_fill_viridis_d(option = "plasma") +
  scale_y_continuous(labels = scales::dollar_format()) +
  labs(
    title = "Average Cost per Beneficiary by Race and Type of Care",
    x = "Race", y = "Mean Cost (USD)", fill = "Race"
  ) +
  facet_wrap(~ toc) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot_mean_cost_by_race_facet_toc, "mean_cost_by_race_facet_toc_CI")




#####


# Prepare long data for HIV+ and MSUD
df_long_hiv_msud <- df_master %>%
  select(cause_name_lvl2, mean_cost_hiv, lower_ci_hiv, upper_ci_hiv, mean_cost_sud, lower_ci_sud, upper_ci_sud) %>%
  pivot_longer(
    cols = c(mean_cost_hiv, mean_cost_sud, lower_ci_hiv, lower_ci_sud, upper_ci_hiv, upper_ci_sud),
    names_to = c("stat", "group"),
    names_pattern = "(mean_cost|lower_ci|upper_ci)_(hiv|sud)",
    values_to = "value"
  ) %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  mutate(group = recode(group, hiv = "HIV+", sud = "MSUD"))

plot_cost_distribution_by_disease_group <- ggplot(df_long_hiv_msud, aes(x = cause_name_lvl2, y = mean_cost, fill = group)) +
  geom_boxplot(outlier.shape = NA) +
  geom_errorbar(aes(ymin = lower_ci, ymax = upper_ci), width = 0.23, position = position_dodge(width = 0.75)) +
  scale_fill_viridis_d(option = "mako") +
  scale_y_continuous(labels = scales::dollar_format()) +
  facet_wrap(~ group) +
  labs(
    title = "Distribution of Modeled Cost per Beneficiary by Disease and Group",
    x = "Disease Category", y = "Mean Cost (USD)", fill = "Group"
  ) +
  theme_minimal(base_size = 13) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

save_plot(plot_cost_distribution_by_disease_group, "cost_distribution_by_disease_group_CI")


