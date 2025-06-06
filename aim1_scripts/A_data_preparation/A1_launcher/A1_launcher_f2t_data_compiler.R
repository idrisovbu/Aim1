##----------------------------------------------------------------
##' Title: A1_launcher_f2t_data_compiler.R
##'
##' Purpose:
##' 
##'
##----------------------------------------------------------------


# # Clear environment and set library paths
rm(list = ls())

pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx)
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


# Dynamically read in directories where data is stored to see what data they have in the first place
# Create a table based off their directory hierarchy (use code to scan folder directories to determine available data)
run_id <- 77
fp_input_data <- paste0("/mnt/share/limited_use/LU_CMS/DEX/01_pipeline/MDCR/run_",run_id,"/F2T/")
list_input_data <- list.dirs(fp_input_data, recursive = TRUE)

# filter down to folders we are interested in that contain data
list_input_data_subset <- list()

for (i in 1:length(list_input_data)) {
  if (str_count(list_input_data[i], pattern = "age_group_years_start") == 1) {
    list_input_data_subset[length(list_input_data_subset) + 1] <- list_input_data[i]
  }
}

# unlist to flatten list
list_input_data_subset <- unlist(list_input_data_subset)

# create data frame
df_list_input_data_subset <- data.frame(list_input_data_subset)
colnames(df_list_input_data_subset)[colnames(df_list_input_data_subset) == "list_input_data_subset"] <- "directory"

# add additional identifier columns
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(runid = str_extract(directory, "(?<=run_)\\d+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(year_id = str_extract(directory, "(?<=year_id=)\\d+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(age_group_years_start = str_extract(directory, "(?<=age_group_years_start=)\\d+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(code_system = str_extract(directory, "(?<=code_system=)[^/]+"))
df_list_input_data_subset <- df_list_input_data_subset %>% mutate(toc = str_extract(directory, "(?<=toc=)[^/]+"))

# Convert age_group_years_start to numeric if a character string
df_list_input_data_subset$age_group_years_start <- as.numeric(df_list_input_data_subset$age_group_years_start)

# Filter out ages below 65

# df_list_input_data_subset <- df_list_input_data_subset %>% 
#   filter(age_group_years_start >= 65 & age_group_years_start <= 85 & year_id != 2000)
df_list_input_data_subset <- df_list_input_data_subset %>%
  filter(
    age_group_years_start >= 65,
    age_group_years_start <= 85,
    !year_id %in% c("2002", "2004", "2006", "2000")
  )



# Save the filtered parameters to CSV save list as .csv for runner script to read in based off job
fp_parameters <- paste0(l, "/LU_CMS/DEX/hivsud/aim1/resources_aim1/parameters_aims1.csv")
write.csv(df_list_input_data_subset, file = fp_parameters, row.names = FALSE)


# Define key variables
user <- Sys.info()[["user"]]
script_path <- paste0(h, "/repo/dex_us_county/misc/hivsud/aim1_scripts/A_data_preparation/A2_worker/A2_worker_f2t_data_compiler.R")
output_dir <- "/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/output_aim1/"
# Define output and log directory paths
date_folder <- format(Sys.Date(), "%Y%m%d")
log_dir <- file.path("/mnt/share/limited_use/LU_CMS/DEX/hivsud/aim1/logs", date_folder)


# Create output and log directories
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
if (!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)

# Submit jobs to the cluster - Launcher Script for Aim 1 study - All Years, Ages, TOC from fp_parameter
jid <- SUBMIT_ARRAY_JOB(
  name = "aim1_f2t_df_compiler",
  script = script_path,
  args = c(fp_parameters), #I'm thinking this is the file path to our arguments (fp_arguments) .csv file we created with the parameter permutations
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "all.q",
  n_jobs = length(unique(df_list_input_data_subset$year_id)),
  #n_jobs = 5, #just for testing purposes
  memory = "250G", #determine based off running job on May 18th
  threads = 1, # can usually keep at 1 unless the code itself is doing something complicated
  time = "12:00:00", # determine based off running job on May 18th
  user_email = paste0(user, "@uw.edu"),
  archive = FALSE,
  test = F # In R, T = TRUE
)
