##----------------------------------------------------------------
## Title: B1_launcher_cause_model.R  (by-cause array)
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
## 1) Build parameters table: (file x cause) rows
##----------------------------------------------------------------
run_date <- "bested"
fp_input_data <- file.path(l, "LU_CMS/DEX/hivsud/aim1/A_data_preparation", run_date, "aggregated_by_year")

input_files <- list.files(fp_input_data, pattern = "\\.parquet$", full.names = TRUE)

df_files <- data.frame(directory = input_files) %>%
  mutate(
    file_type = ifelse(grepl("F2T", basename(directory)), "F2T", "RX"),
    year_id   = as.numeric(str_extract(basename(directory), "(?<=data_)\\d{4}")),
    age_group_years_start = as.numeric(str_extract(basename(directory), "(?<=_age)\\d+"))
  ) %>%
  filter(!is.na(year_id)) %>%
  filter(year_id %in% c(2000, 2010, 2014, 2015, 2016, 2019))

# Define causes to run (safe to hardcode; worker will skip if absent)
causes_to_run <- c(
  "_enteric_all","_infect","_intent","_mental","_neo","_neuro","_ntd","_otherncd",
  "_rf","_ri","_sense","cvd","diab_ckd","digest","_unintent","_well","resp",
  "nutrition","inj_trans","msk","skin","mater_neonat","std"  # <- exclude hiv, _subs
)


df_causes <- data.frame(cause_name = causes_to_run, stringsAsFactors = FALSE)

# Cross join files x causes
df_params <- tidyr::crossing(df_files, df_causes)

# Write params CSV
param_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/resources_aim1/")
dir.create(param_dir, recursive = TRUE, showWarnings = FALSE)
fp_parameters <- file.path(param_dir, "B1_two_part_model_parameters_aim1_BY_CAUSE.csv")
fwrite(df_params, fp_parameters)

##----------------------------------------------------------------
## 2) Submit array: one task per (file, cause)
##----------------------------------------------------------------
user        <- Sys.info()[["user"]]
script_path <- file.path(h, "repo/Aim1/aim1_scripts/B_analysis/B2_worker/B2_worker_cause_model.R")
# log_dir     <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis/logs")
# dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

# Create log directory with date subfolder
log_date <- format(Sys.Date(), "%Y%m%d")   # e.g., "20250818"
log_dir  <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis/logs", log_date)
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)


bootstrap_iterations_F2T <- 25
bootstrap_iterations_RX  <- 25 

#### was running 1 hour with these specs. 

jid <- SUBMIT_ARRAY_JOB(
  name       = "B1_cause_model",
  script     = script_path,
  args       = c(fp_parameters, bootstrap_iterations_F2T, bootstrap_iterations_RX),
  error_dir  = log_dir,
  output_dir = log_dir,
  queue      = "all.q",
  n_jobs     = nrow(df_params),
  memory     = "150G",         # often enough per cause; adjust if needed
  threads    = 1,
  time       = "01:00:00",    # adjust per dataset size/boots
  user_email = paste0(user, "@uw.edu"),
  archive    = FALSE,
  test       = T
)

cat("Submitted", nrow(df_params), "array tasks.\n")
