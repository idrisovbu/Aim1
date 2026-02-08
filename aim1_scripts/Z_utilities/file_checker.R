##----------------------------------------------------------------
##' Title: file_checker.R
##'
##' Purpose: Checks to ensure we have all the files we expect for each part of the pipeline
##----------------------------------------------------------------

##----------------------------------------------------------------
## 0. Clear environment and set library paths
##----------------------------------------------------------------
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
## 1. TPE File Checker for completeness
##----------------------------------------------------------------
base_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/B_analysis"

# Create full df parameters
fp_parameters_cause <- "/ihme/limited_use//LU_CMS/DEX/hivsud/aim1/resources_aim1//B1_two_part_model_parameters_aim1_BY_CAUSE.csv"
fp_parameters_cause_hivsud <- "/ihme/limited_use//LU_CMS/DEX/hivsud/aim1/resources_aim1//B1_two_part_model_parameters_aim1_BY_CAUSE_HIVSUD.csv"

df_parameters_cause <- fread(fp_parameters_cause)
df_parameters_cause_hivsud <- fread(fp_parameters_cause_hivsud)

df_parameters <- rbind(df_parameters_cause, df_parameters_cause_hivsud)

# 04. Two Part Model Checking
date_tpe <- "20260106"
input_cause_has_0 <- file.path(base_dir, "04.Two_Part_Estimates", date_tpe, "by_cause_has_0", "results")
input_cause_has_1 <- file.path(base_dir, "04.Two_Part_Estimates", date_tpe, "by_cause_has_1", "results")

# Extract cause_name and year_id from string, convert list to df
make_df_from_files <- function(x) {
  if (is.list(x)) {
    files <- unlist(x, use.names = FALSE)
  } else if (is.character(x)) {
    files <- x
  } else {
    stop("x must be a character vector or a list of character strings")
  }
  
  data.frame(
    file = files,
    cause_name = sub("/.*$", "", files),
    year_id = as.integer(sub(".*_year([0-9]{4})\\.csv$", "\\1", files)),
    stringsAsFactors = FALSE
  )
}

compare_to_df_param <- function(x, y) {
  z <- left_join(y %>% select(c("cause_name", "year_id")),
                 x,
                 by = c("cause_name", "year_id"))
  
  z <- z %>% filter(is.na(file))
  
  if(nrow(z) != 0) {
    print(paste0("Missing these cause/years"))
    for (i in 1:nrow(z)) {
      print(paste0("Cause_name: ", z[i, "cause_name"], " Year_id: ", z[i, "year_id"]))
    }
  } else {
    print("No missing files")
  }
}

# has_0
list_tpe_has_0 <- list.files(input_cause_has_0, recursive = TRUE, include.dirs = FALSE)
df_tpe_has_0 <- make_df_from_files(list_tpe_has_0)
compare_to_df_param(df_tpe_has_0, df_parameters)

# has_1
list_tpe_has_1 <- list.files(input_cause_has_1, recursive = TRUE, include.dirs = FALSE)
df_tpe_has_1 <- make_df_from_files(list_tpe_has_1)
compare_to_df_param(df_tpe_has_1, df_parameters)


