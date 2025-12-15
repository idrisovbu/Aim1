# Scratch can delete when done

df <- copy(df_input_ss)

View(
  df %>% 
    group_by(acause_lvl2, toc) %>%
    summarize(
      avg_cost_per_bene = weighted.mean(avg_cost_per_bene, n_benes_per_group),
      .groups = "drop"
    ) %>%
    arrange(toc, desc(avg_cost_per_bene)) %>%
    group_by(toc) %>%
    mutate(rank = row_number()) %>%
    ungroup()
)




