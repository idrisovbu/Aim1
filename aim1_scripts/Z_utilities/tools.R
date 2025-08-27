##----------------------------------------------------------------
## Title: tools.R
## Notes: This script has functions to check / test certain things for Aim 1
##----------------------------------------------------------------

rm(list = ls())

pacman::p_load(dplyr, data.table, tidyr, arrow, stringr, ini)  # add stringr!
library(lbd.loader, lib.loc = sprintf("/share/geospatial/code/geospatial-libraries/lbd.loader-%s", R.version$major))
if ("dex.dbr" %in% (.packages())) detach("package:dex.dbr", unload = TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(devtools::load_all(path = "/ihme/homes/idrisov/repo/dex_us_county/"))

# Paths
if (Sys.info()["sysname"] == 'Linux'){
  j <- "/home/j/"; h <- paste0("/ihme/homes/", Sys.info()[7], "/"); l <- '/ihme/limited_use/'
} else if (Sys.info()["sysname"] == 'Darwin'){
  j <- "/Volumes/snfs"; h <- paste0("/Volumes/", Sys.info()[7], "/"); l <- '/Volumes/limited_use'
} else {
  j <- "J:/"; h <- "H:/"; l <- 'L:/'
}

##----------------------------------------------------------------
## Set directories
##----------------------------------------------------------------
run_date <- "bested"
A_output <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation", run_date, "aggregated_by_year")
B_root <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis/")
B2_SS <- file.path(B_root, "01.Summary_Statistics/", run_date, "/")
B2_RE <- file.path(B_root, "02.Regression_Estimates/", run_date, "/")
B2_MS <- file.path(B_root, "03.Meta_Statistics/", run_date, "/")
B2_TPE <- file.path(B_root, "04.Two_Part_Estimates/", run_date, "/by_cause/results/")

##----------------------------------------------------------------
## Check B by-cause script outputs for completeness
##----------------------------------------------------------------
input_files <- list.files(B2_TPE, pattern = "\\csv$", full.names = TRUE, recursive = TRUE)

pat <- "^bootstrap_marginal_results_([^_]+)_([^_]+)_year(\\d{4})_age(\\d+)\\.csv$"

df <- data.frame(file_path = input_files) %>% 
  mutate(file = basename(file_path)) %>%
  extract(
    col = file,
    into = c("cause_name", "file_type", "year_id", "age_group_years_start"),
    regex = pat,
    convert = TRUE
  )

df_files <- data.frame(directory = input_files) %>%
  mutate(
    file_type = ifelse(grepl("F2T", basename(directory)), "F2T", "RX"),
    year_id   = as.numeric(str_extract(basename(directory), "(?<=data_)\\d{4}")),
    age_group_years_start = as.numeric(str_extract(basename(directory), "(?<=_age)\\d+"))
  )
# Define causes to run (safe to hardcode; worker will skip if absent)
causes_to_run <- c(
  "hiv", "_subs"  # <- only include HIV and Substance use disorder
)




