#'@Desc *Compute the seasonal climatology and the standardised anomalies (z-scores) of daily (Ts-Ta) integral for all Australia to compute the SATDA metric*
#'@Desc *Parallelise by each day-of-year (marked by Month-Date, e.g., 01-10 = 10/Jan)*
#'@Desc *First compute the baseline 'climatology' value for the target Month-Date*
#'@Desc *Then compute the anomaly value for the dates matching this Month-Date across the full analysis period*

rm(list = ls())

library(terra)
library(lubridate)
library(zoo)
library(reshape2)
library(data.table)
library(tidyverse)
library(stringr)

work_dir = './' # {work directory, please specify your own}

# Source the functions relevant to the script
path2FUNS = paste0(work_dir, 'functions/')

# Path to moving-average temporal composite of daily cumulative hourly Ts-Ta
path2SumdT_COMPOSITE  = paste0(work_dir, 'test_output/3_SumdT_MA_Composite/')
# Path to output climatology of daily Ts-Ta integral
path2SATDA_CLIM = paste0(work_dir, 'test_output/4_SATDA_Climatology/')
# Path to output anomalies of daily Ts-Ta integral which leads to SATDA metric
path2SATDA_ANOM = paste0(work_dir, 'test_output/4_SATDA_Anomaly/')

# Define DoY / month-date to compute the climatology and anomaly -----

# Full study period (Aug/2015 to Dec/2022)
#'@Note *Here, the first date of composited value is 28/Aug/2015 due to the selected 4-week backward composite window*
full_period = seq(as.Date(paste0(2015, "-08-28")), 
                  as.Date(paste0(2022, "-12-31")), 1)

# All 366 Month-Date combination 
MonthDates = sort(unique(format(full_period, '%m-%d')))

# Obtain the parallel argument for day-of-year (numeric)
DoY  = as.numeric(commandArgs(trailingOnly = T))

# Subset to one target Month-Date(MMDD)
MMDD = MonthDates[DoY]

#############################################################################################################~
################ MAIN LINE: CLIMATOLOGY & ANOMALY COMPUTATION FOR EACH MONTH-DATE ##########################
#############################################################################################################~

# Dates matching this Month-Date in the full analysis period
dates_md = full_period[which(format(full_period, '%m-%d') == MMDD)]

# Obtain the +/- 15-day window surrounding the target Month-Date in the full analysis period 
# Use +/- 15-day window to mimic a monthly climatology 
#'@Note [use 'climwindow' in the variable names to highlight that this is the window for climatology]

# Function to obtain the multi-year window around each DoY to computing climatology climatology window
source(paste0(path2FUNS, 'get_climwindow_dates.R'))

dates_climwindow_md = get_climwindow_dates(target_date = dates_md[1], 
                                           full_dates = full_period, 
                                           window = 15)

# Obtain rasters for composited (Ts-Ta) for the climatology window for this Month-Date -----

# Create a list marked by the date to store daily raster falling in the climatology window
SumdT_MA_climwindow_list = sapply(as.character(dates_climwindow_md), function(x) NULL)

for (j in seq_along(dates_climwindow_md)) {
  
  # Date in the climatology window
  date_c = dates_climwindow_md[j]
  
  # Obtain file name for the composited (Ts-Ta) for this date
  SumdT_MA_date_fn = list.files(path2SumdT_COMPOSITE, 
                                paste0("SumdT.*4wk.*" , 
                                       format(date_c, '%Y%m%d')),
                                full.names = T)
  
  # Read raster layer for composited (Ts-Ta) for this date
  SumdT_MA_date_r = rast(SumdT_MA_date_fn)
  
  # Store into the list for the climatology window
  SumdT_MA_climwindow_list[[j]] = SumdT_MA_date_r
  
}

#'[Stack the composited Ts-Ta integral for all the dates falling within the climatology window]
SumdT_MA_climwindow_stack = rast(SumdT_MA_climwindow_list)

# Name the raster stack by each date in the climatology window
names(SumdT_MA_climwindow_stack) = dates_climwindow_md

print(paste0('Composited Ts-Ta integral for climatology window for ',  MMDD, ' is collated'))

# 1. Generate maps of climatology statistics ------------

# Compute climatology mean and standard deviation (SD) for daily (Ts-Ta) integral for this Month-Date
# Summary statistics across all dates in the climatology window 
SumdT_MA_MEAN_r = app(SumdT_MA_climwindow_stack, mean, na.rm = T)
SumdT_MA_SD_r  = app(SumdT_MA_climwindow_stack, sd, na.rm = T)

# Rounding to 3 decimal places to reduce data storage while maintaining good precision
SumdT_MA_MEAN_r = round(SumdT_MA_MEAN_r, 3)
SumdT_MA_SD_r = round(SumdT_MA_SD_r, 3)

# Write the raster layer climatology mean and SD for each Month-Date based on the full analysis period
terra::writeRaster(SumdT_MA_MEAN_r,
                   paste0(path2SATDA_CLIM, 
                          "SumdT_" , "Wdiur_", 
                          "ClimMean_" , MMDD, "_2km.tif"), 
                   datatype = "FLT4S", overwrite = T, 
                   filetype = "GTiff", NAflag = -99) 

terra::writeRaster(SumdT_MA_SD_r,
                   paste0(path2SATDA_CLIM, 
                          "SumdT_" , "Wdiur_", 
                          "ClimSD_" , MMDD, "_2km.tif"), 
                   datatype = "FLT4S", overwrite = T, 
                   filetype = "GTiff", NAflag = -99) 

print(paste0("Write climatology raster for ", MMDD))

# 2. Compute anomaly for the dates matching the target Month-Date in this climatology window ------------

# Array indices for dates in the climatology window that matches the target Month-Date 
# e.g., if the target Month-Date is 01-03 (03/Jan),
# the dates would include 2016-01-03, 2017-01-03, ..., 2022-01-03 when the study period is 2016 to 2022

dates_md_indices = which(dates_climwindow_md %in% dates_md)

# Subset the raster stack of composited (Ts-Ta) integral for the climatology window 
# to the dates matching the target Month-Date
SumdT_MA_md_stack = SumdT_MA_climwindow_stack[[dates_md_indices]]

#'@Note [Compute the standardised anomaly for dates matching the target Month-Date with respect to the climatology statistics]
#'@Note [This leads to a raster stack of anomaly for all dates matching the target Month-Date]
SumdT_Anom_md_stack =  (SumdT_MA_md_stack - SumdT_MA_MEAN_r)/SumdT_MA_SD_r

# Rounding to 3 decimal places to reduce data storage while maintaining good precision
SumdT_Anom_md_stack = round(SumdT_Anom_md_stack, 3)

### Loop each date matching the target Month-Date to write the anomaly raster 

for (k in seq_along(dates_md)) {
  
  # Target date matching the Month-Date
  date_c = dates_md[k]
  
  # Filter to this Month-Date in a specific year
  SumdT_Anom_date_r  = SumdT_Anom_md_stack[[k]]
  
  # Write anomaly raster for this date
  #'@Note [The region can be specified in the file name by the user, e.g., Australia]
  SumdT_Anom_date_fn = paste0(path2SATDA_ANOM , 
                              "SATDA_" , format(date_c, "%Y%m%d"), "_2km.tif")
  
  terra::writeRaster(SumdT_Anom_date_r,  
                     SumdT_Anom_date_fn, 
                     datatype = "FLT4S", overwrite = T, 
                     filetype = "GTiff", NAflag = -99)
  
  print(paste0("Write anomaly raster for ", date_c))
  
} 