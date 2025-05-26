#'@Desc *Compute monthly mean and maximum statistics of SATDA based on daily images*
#'@Desc *Parallelise by each Year-Month (YYYYMM)*

#'@Note *Due to large processing requirements, the computation was broken into batches / subsets of Year-Month in the study period*
#'@Note *However, the structure of the script is straightforward, so if a high-performance computing system isn't available*
#'@Note *but the turnaround time isn't a key constraint, one could simplify the scripts to be a longer 'for loop' and let it run longer.*

rm(list = ls())

# Load key packages
library(terra)
library(lubridate)
library(dplyr)

work_dir = './' # {work directory, please specify your own}

# Source the functions relevant to the script
path2FUNS = paste0(work_dir, 'functions/')
# Path to SATDA anomalies
path2SATDA_ANOM = paste0(work_dir, 'test_output/4_SATDA_Anomaly/')
# Path to outputs of monthly summary statistics  
path2SATDA_MONTHLY_RAST = paste0(work_dir, 'test_output/5_SATDA_MonthlyStats/')

# 0. Get the year-month to compute monthly summary statistics -------------

# `period`: parallel argument
period = as.numeric(commandArgs(trailingOnly = T))

# Full period to compute the monthly statistics
#'@Note *As the first date of composited value is 28/Aug/2015 due to the selected 4-week backward composite window, *
#'@Note *the first complete Year-Month for computing monthly statistics is Sept 2015*

full_period = seq(as.Date(paste0(2015, "-09-01")), 
                  as.Date(paste0(2022, "-12-31")), 1)

# All year-month (YYYYMM) in the study period
YM_all = unique(format(full_period, "%Y%m"))

# Use the parallel argument to subset the target YM 
target_YM = YM_all[period]

# 1. Compute monthly stats from the monthly raster stack for SATDA -----

# Function to get individual dates for a given year-month (YM) in the format of YYYYMM
source(paste0(path2FUNS, 'get_dates_ym.R'))

# Obtain all dates in this Year-Month
dates_YM = as.Date(get_dates_ym(target_YM))

# Obtain the files of daily SATDA GeoTiffs for the corresponding dates 
# `lapply`: apply the function to each list component
SATDA_dates_YM_fns = unlist(lapply(dates_YM, function(x) 
                            list.files(path = path2SATDA_ANOM,
                                       pattern = paste0("SATDA.*", format(x, '%Y%m%d')),
                                       full.names = T)))

# Read the daily SATDA rasters from the file list
SATDA_dates_YM_r_list = lapply(SATDA_dates_YM_fns, rast)

# Stack the SATDA rasters for the Year-Month
SATDA_YM_stack = rast(SATDA_dates_YM_r_list)

## Compute monthly mean 
SATDA_monthmean_r = app(SATDA_YM_stack, fun = mean, na.rm = T)

# Rounding to 3 d.p.
SATDA_monthmean_r = round(SATDA_monthmean_r, 3)

## Monthly extreme (max)  
SATDA_monthmax_r = app(SATDA_YM_stack, fun = max, na.rm = T)

# Rounding to 3 d.p.
SATDA_monthmax_r = round(SATDA_monthmax_r, 3)

# Write monthly average and maximum raster for the target Year-Month

terra::writeRaster(SATDA_monthmean_r, 
                   paste0(path2SATDA_MONTHLY_RAST,
                          "SATDA_" , "MonthMean_", 
                          YM_c, "_2km.tif"), 
                   overwrite = T, filetype="GTiff",
                   datatype = "FLT4S", NAflag = -99)

terra::writeRaster(SATDA_monthmax_r, 
                   paste0(path2SATDA_MONTHLY_RAST,
                          "SATDA_" , "MonthMax_", 
                          YM_c, "_2km.tif"), 
                   overwrite = T, filetype="GTiff",
                   datatype = "FLT4S", NAflag = -99)
