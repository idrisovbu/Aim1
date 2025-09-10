##----------------------------------------------------------------
## Title: B3_launcher_regression_aggregator.R
## Purpose: Create parameters CSV (one row per cause dir) and submit array job
## Author: Bulat Idrisov
##----------------------------------------------------------------

## 0) Clear & libs
rm(list = ls())

pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx)
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

##----------------------------------
## 1. Create parameters CSV file
##----------------------------------

base_dir <- file.path(l, "LU_CMS/DEX/hivsud/aim1/B_analysis")
date_of_regression <- "20250826"   # <-- set as needed
regcoef_root <- file.path(base_dir, "04.Two_Part_Estimates", date_of_regression, "by_cause", "regression_coefficients")


## 2) Build parameters CSV (one row per cause/year/age leaf)
# Tree expected:
#   regcoef_root/
#     ├─ <cause_stub_1>/
#     │    ├─ <year_id_1>/
#     │    │    ├─ <age_group_years_start_1>/*.csv
#     │    │    └─ <age_group_years_start_2>/*.csv
#     │    └─ <year_id_2>/...
#     └─ <cause_stub_2>/...


# 2.1 Collect first-level cause folders
cause_dirs <- list.dirs(regcoef_root, recursive = FALSE, full.names = TRUE)
stopifnot(length(cause_dirs) > 0)

# 2.2 Helper to list immediate numeric subdirs safely
list_numeric_subdirs <- function(dir_path) {
  if (!dir.exists(dir_path)) return(integer(0))
  subs <- list.dirs(dir_path, recursive = FALSE, full.names = TRUE)
  if (!length(subs)) return(integer(0))
  # take basename, keep those that look like integers
  b <- basename(subs)
  is_num <- grepl("^\\d+$", b)
  as.integer(b[is_num])
}

# 2.3 Walk cause → years → ages and build leaf rows
rows <- vector("list", length(cause_dirs))
for (i in seq_along(cause_dirs)) {
  cause_dir <- cause_dirs[i]
  cause_stub <- basename(cause_dir)
  
  years <- list_numeric_subdirs(cause_dir)
  if (!length(years)) next
  
  leafs <- vector("list", length(years))
  for (j in seq_along(years)) {
    y <- years[j]
    year_dir <- file.path(cause_dir, as.character(y))
    
    ages <- list_numeric_subdirs(year_dir)
    if (!length(ages)) {
      # if there is no age level and CSVs live directly under year_dir, treat age as NA
      csvs_here <- list.files(year_dir, pattern = "\\.csv$", full.names = TRUE, recursive = FALSE)
      if (length(csvs_here)) {
        leafs[[j]] <- data.table(
          cause_stub = cause_stub,
          year_id = y,
          age_group_years_start = NA_integer_,
          leaf_dir = year_dir,
          n_csv = length(csvs_here)
        )
      } else {
        leafs[[j]] <- data.table()  # empty
      }
      next
    }
    
    # ages exist: create one leaf per age directory
    # keep only ages that actually contain CSVs (avoid empty leaves)
    age_dirs <- file.path(year_dir, as.character(ages))
    n_csvs <- vapply(age_dirs,
                     function(d) length(list.files(d, pattern="\\.csv$", full.names=TRUE, recursive=FALSE)),
                     integer(1))
    keep <- n_csvs > 0L
    if (any(keep)) {
      leafs[[j]] <- data.table(
        cause_stub = cause_stub,
        year_id = y,
        age_group_years_start = ages[keep],
        leaf_dir = age_dirs[keep],
        n_csv = n_csvs[keep]
      )
    } else {
      leafs[[j]] <- data.table()
    }
  }
  
  rows[[i]] <- rbindlist(leafs, use.names = TRUE, fill = TRUE)
}

df_params <- rbindlist(rows, use.names = TRUE, fill = TRUE)
stopifnot(nrow(df_params) > 0)

# Attach run context & canonical paths
df_params[, `:=`(
  regcoef_root = regcoef_root,
  base_dir = base_dir,
  date_of_regression = date_of_regression
)]

# (Optional) Filtering knobs to re-run a subset
# df_params <- df_params[cause_stub %in% c("_enteric_all", "cvd_ihd")]
# df_params <- df_params[year_id %in% c(2000, 2010, 2014, 2015, 2016, 2019)]
# df_params <- df_params[age_group_years_start %in% c(65, 70)]

# Stable ordering → stable SGE/SLURM array indexing
setorder(df_params, cause_stub, year_id, age_group_years_start, leaf_dir)

# Write parameters
param_dir <- file.path(base_dir, "05.Aggregation_Summary", "params")
dir.create(param_dir, recursive = TRUE, showWarnings = FALSE)

fp_parameters <- file.path(param_dir, "B3_params_regression_by_cause_year_age.csv")
fwrite(df_params, fp_parameters)

cat("Params written:", fp_parameters, "\n",
    "Rows:", nrow(df_params), " | Distinct causes:", df_params[, uniqueN(cause_stub)],
    " | Distinct (year,age) leaves:", df_params[, uniqueN(sprintf('%s_%s', year_id, age_group_years_start))], "\n")


# Write parameters
param_dir <- file.path(base_dir, "05.Aggregation_Summary", "params")
dir.create(param_dir, recursive = TRUE, showWarnings = FALSE)
fp_parameters <- file.path(param_dir, "B3_params_regression_by_cause.csv")
fwrite(df_params, fp_parameters)
cat("Wrote", nrow(df_params), "cause rows to:", fp_parameters, "\n")

## 3) Submit array job
user <- Sys.info()[["user"]]
script_path <- file.path(h, "repo/Aim1/aim1_scripts/B_analysis/B3_aggregator/B3_worker_regression_by_cause.R")
stopifnot(file.exists(script_path))

log_dir <- file.path(base_dir, "05.Aggregation_Summary", "logs")
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

jid <- SUBMIT_ARRAY_JOB(
  name        = "aim1_B3_regcoef_bycause",
  script      = script_path,
  args        = c(fp_parameters),
  error_dir   = log_dir,
  output_dir  = log_dir,
  queue       = "all.q",
  n_jobs      = nrow(df_params),
  memory      = "40G",      # tune after first run
  threads     = 1,
  time        = "1:00:00",  # tune after first run
  user_email  = paste0(user, "@uw.edu"),
  archive     = FALSE,
  test        = FALSE
)

cat("Submitted array job with", nrow(df_params), "tasks. JID:", jid, "\n")
