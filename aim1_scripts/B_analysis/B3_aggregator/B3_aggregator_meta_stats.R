##----------------------------------------------------------------
##' Title: B3_aggregator_meta_stats.R
##'
##' Purpose:
##
##----------------------------------------------------------------
# Environment setup
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx,readr,purrr)
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))

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

##----------------------------------------------------------------
## 0. Create directory folders 
##----------------------------------------------------------------
# Ensure the output directory exists
ensure_dir_exists <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}

# Defined manually
date_of_input <- "bested" # bested from 20250631
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"

# Define input directory 
input_meta_stats <- file.path(base_dir, "03.Meta_Statistics", date_of_input) 

# Define output directory
date_of_output <- format(Sys.time(), "%Y%m%d")

output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output, "aggregation_meta_stats_results")
subtable_output_folder <- file.path(base_dir, "05.Aggregation_Summary", date_of_output, "aggregation_meta_subtable_results")

ensure_dir_exists(output_folder)
ensure_dir_exists(subtable_output_folder)


##----------------------------------------------------------------
## 1. Read in data
##----------------------------------------------------------------

# Get the list of all CSV files from the input directory
files_list <- list.files(input_meta_stats, pattern = "\\.csv$", full.names = TRUE) 

# Read files
df_input <- map_dfr(files_list, ~read_csv(.x, show_col_types = FALSE))

# Save the aggregated results
output_file <- file.path(output_folder, "meta_statistics.csv")
write_csv(df_input, output_file)
cat("table saved", output_file, "\n")


#######
# examine toc per year combo interactively
######

# The code below summarizes the analytic sample by year and type of care.
# It lists the unique types of care available each year, the number of unique beneficiaries per year,
# and produces a table showing beneficiary counts for each type of care and year.
#such as this table https://docs.google.com/document/d/1o6e8yvv1kW4mf7Be5F667b9mMOmA-O4G9TF5XKGFH_k/edit?usp=sharing

df_input %>%
  count(year_id, toc, name = "toc_count") %>%
  count(year_id, name = "num_unique_toc") %>%
  arrange(year_id)

df_input %>%
  group_by(year_id) %>%
  summarise(toc_list = paste(sort(unique(toc)), collapse = ", "))


# Summarize total unique beneficiaries per year
total_bene_by_year <- df_input %>%
  group_by(year_id) %>%
  summarise(total_unique_bene = sum(total_unique_bene, na.rm = TRUE)) %>%
  arrange(year_id)

print(total_bene_by_year)



# Summarize total unique beneficiaries per year and toc
total_bene_by_year_toc <- df_input %>%
  group_by(year_id, toc) %>%
  summarise(total_unique_bene = sum(total_unique_bene, na.rm = TRUE)) %>%
  arrange(year_id, toc)

print(total_bene_by_year_toc)

# 1. Save total unique beneficiaries by year
output_file_by_year <- file.path(output_folder, "total_bene_by_year.csv")
write_csv(total_bene_by_year, output_file_by_year)

# 2. Save total unique beneficiaries by year and toc
output_file_by_year_toc <- file.path(output_folder, "total_bene_by_year_toc.csv")
write_csv(total_bene_by_year_toc, output_file_by_year_toc)


