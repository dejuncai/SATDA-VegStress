#'@Desc *Compute the 4-week backward moving average temporal composite of daily cumulative hourly surface-air temperature difference (Ts-Ta)* 
#'@Desc *This temporal composite step helps fill the cloud-induced gaps in daily cumulative (Ts-Ta) and*
#'@Desc *reduce the short-term noise due to incomplete cloud screening, residual atmospheric effects or abrupt synoptic-scale events*

#'@Note 'backward' means that 

#'@Note *Due to large processing requirements, the computation was broken into batches / subsets of dates in the study period*
#'@Note *However, the structure of the script is straightforward, so if a high-performance computing system isn't available*
#'@Note *but the turnaround time isn't a key constraint, one could simplify the scripts to be a longer 'for loop' and let it run longer.*

rm(list = ls())

library(terra)
library(lubridate)
library(data.table)
library(dplyr)
library(stringr) 

work_dir = './' # {work directory, please specify your own}

# Source the functions relevant to the script
path2FUNS = paste0(work_dir, 'functions/')

# Path to rasters of daily cumulative (Ts-Ta)
path2SumdT_RASTER  = paste0(work_dir, 'test_output/2_SumdT_Raster/')

# Path to output moving-average temporal composite of daily cumulative Ts-Ta
path2SumdT_COMPOSITE  = paste0(work_dir, 'test_output/3_SumdT_MA_Composite/')

# Function to help subset the task for parallel computing 
source(paste0(path2FUNS, 'get_batch_parallel.R'))

# Define batch of dates to implement the moving average -----

# `period`: parallel argument
period = as.numeric(commandArgs(trailingOnly = T))

# Full study period (Aug/2015 to Dec/2022)
#'@Note *Here, the first date of composited value is 28/Aug/2015 due to the selected 4-week backward composite window*
full_period = seq(as.Date(paste0(2015, "-08-28")), 
                  as.Date(paste0(2022, "-12-31")), 1)

# Size of each batch of dates (time period)
date_size = 10

# Subset the batch of dates to perform temporal composite
batch_dates  = get_batch_parallel(batchnum = period, 
                                  entire = full_period, 
                                  size = date_size)

#################################################################################### ~
########## MAIN LINE: Loop each date to compute the 4-week backward average composite of daily cumulative Ts-Ta ------ 
#################################################################################### ~

# Temporal window of moving average (MA) composite (Wcomp in the paper)
MA_window = 28

for (i in seq_along(batch_dates)) {
  
  # Target date in the batch
  date_c = batch_dates[i]
  
  # Obtain the start day of the 4-week backward composite window 
  MA_start_date = date_c - days(MA_window - 1)
  
  # Get sequence of dates in the 4-week backward composite window 
  MA_window_dates = seq(MA_start_date, date_c, 1)
  
  ## Obtain the raster layers of daily (Ts-Ta) integral for each date in the MA composite window ------------
  #'@Note [Since daily (Ts-Ta) integral is stored as monthly raster stack for each year-month (YYYYMM), ]
  #'@Note [we loop across each month covered by the MA window and subset to the specific dates in the month]
  
  MA_window_ym = unique(format(MA_window_dates, '%Y%m'))
  
  # Create a list marked by the month (YYYYMM) to store the daily raster falling in the composite window
  SumdT_window_list = sapply(MA_window_ym, function(x) NULL)
  
  for (ym in MA_window_ym) {
    
    # Obtain the file name for the raster stack of (Ts-Ta) integral for the given YYYYMM
    SumdT_ym_fn = list.files(path2SumdT_RASTER,
                             paste0("Chiba_SumdT.*", ym), 
                             full.names = T)
    # Read the monthly raster stack of daily (Ts-Ta) integral
    SumdT_ym_stack = rast(SumdT_ym_fn)
    
    # Dates included in the composite window that fall within the given YYYYMM
    window_dates_ym = MA_window_dates[which(format(MA_window_dates, '%Y%m') == ym)]
    
    # Get indices of these included dates in monthly raster stack 
    window_dates_ym_inds = day(window_dates_ym) 
    
    # Subset the monthly stack of (Ts-Ta) integral to these included days
    SumdT_ym_stack_SUB = SumdT_ym_stack[[window_dates_ym_inds]]
    
    # Store the subset of raster layers for dates that fall within in this YYMM
    SumdT_window_list[[ym]] = SumdT_ym_stack_SUB
  }
  
  # Stack the raster layers for the entire MA composite window
  SumdT_window_stack = rast(SumdT_window_list) 
  
  # Assign names by the dates in the MA composite windows
  names(SumdT_window_stack) = MA_window_dates 
  
  #'@Note [Compute the 4-week backward moving average composite of daily (Ts-Ta) integral for this date]
  SumdT_MA_r = app(SumdT_window_stack, 'mean', na.rm = T)
  
  # Rounding to 3 decimal places to reduce data storage while maintaining good precision
  SumdT_MA_r = round(SumdT_MA_r, 3)
  
  # Write the 4-week backward moving average composite of daily (Ts-Ta) integral for the given date
  # i.e., a daily raster with 1 layer
  
  terra::writeRaster(SumdT_MA_r,
                     paste0(path2SumdT_COMPOSITE, 
                            "SumdT_", "4wkMA_Composite_", 
                            format(date_c, '%Y%m%d'), "_2km.tif"), 
                     datatype = "FLT4S", overwrite = T, 
                     filetype = "GTiff", NAflag = -99) 
  
  print(date_c)
}