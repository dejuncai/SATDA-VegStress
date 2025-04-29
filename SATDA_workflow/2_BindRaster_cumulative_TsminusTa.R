#'@Desc *Combine the calculated cumulative hourly Ts-Ta integral from individual batchs of grid cells*
#'@Desc *into the raster covering the full spatial extent of the study region*
#'@Desc *To reduce the number of files, stack daily rasters of cumulative hourly Ts-Ta from the same Year-Month (YYYYMM) into a monthly raster stack*

#'@Note *Due to large processing requirements, the computation was broken into batches / subsets of time periods (YYYYMM)*
#'@Note *However, the structure of the script is straightforward, so if a high-performance computing system isn't available*
#'@Note *but the turnaround time isn't a key constraint, one could simplify the scripts to be a longer 'for loop' and let it run longer.*


rm(list = ls())

library(terra)
library(lubridate)
library(reshape2)
library(data.table)
library(dplyr)

work_dir = './' # {work directory, please specify your own}

# Source the functions relevant to the script
path2FUNS = paste0(work_dir, 'functions/')

# Path to individual batches of calculated daily cumulative hourly Ts-Ta
# (herein use dT in the codes to denote 'difference in temperature')
path2SumdT_BATCH  = paste0(work_dir, 'test_output/1_SumdT_Batch/')

# Path to output rasters of daily cumulative hourly Ts-Ta
path2SumdT_RASTER  = paste0(work_dir, 'test_output/2_SumdT_Raster/')

# Function to get individual dates for a given year-month (YM) in the format of YYYYMM
source(paste0(path2FUNS, 'get_dates_ym.R'))

# Read the locations of all grid cells in the study region
Grid_locs = fread(paste0(work_dir, "Himawari_GridLocations_2km.csv"))

# Raster template for daily cumulative hourly Ts-Ta over southeast Australia (SEOz)
#'@Note [User can define their own study region]
SEOz_2km_template = rast(ext(138, 153.62, -39.5, -24),
                         res = 0.02, crs = 'epsg:4326')

# Subset the year-month (YYYYMM) to bind daily cumulative hourly Ts-Ta from all batches of grid cells ---------------

 # `period`: parallel argument, 
period = as.numeric(commandArgs(trailingOnly = T))

# Full study period (Aug/2015 to Dec/2022)
full_period = seq(as.Date(paste0(2015, "-08-01")), 
                  as.Date(paste0(2022, "-12-31")), 1)
# All year-month (YYYYMM) in the study period
YM_all = unique(format(full_period, "%Y%m"))

# Use the parallel argument to subset the target YM 
target_YM = YM_all[period]

#################################################################################### ~
########## MAIN LINE: Bind daily cumulative hourly Ts-Ta across all batches on each day ------ 
########## convert into daily rasters, and finally stack to monthly raster stack ### ~
#################################################################################### ~

# 1. Bind daily cumulative hourly Ts-Ta across all batches from CSV files ----------------

# Search for files for all batches of daily cumulative hourly Ts-Ta for the target year-month
SumdT_ym_fns  = list.files(path = path2SumdT_BATCH,
                           pattern = paste0('SumdT.*', target_YM),
                           full.names = T)
# Obtain the batch number of grid cells 
batches_string = str_extract(basename(SumdT_ym_fns), 
                             pattern = "(?<=Batch)(\\d+)(?=_.*)")

# Sort the batch number 
batches_num = sort(unique(as.numeric(batches_string)), decreasing = F)

# Storage of cumulative hourly Ts-Ta for this YM across all grid cells
SumdT_data_ym = NULL

for (b in batches_num) {
  
  # Find the corresponding file name for the target batch number `b`
  batch_SumdT_ym_fn = grep(pattern = paste0('Batch', b, '_'),
                           x = SumdT_ym_fns, value = T)

  # Read the computed daily cumulative hourly Ts-Ta for this batch (for specific zone, in this year-month)
  batch_SumdT_ym_data = fread(batch_SumdT_ym_fn, header = F)
  
  # Assign column name
  names(batch_SumdT_ym_data) = c("Date", "Grid", "SumdT")
  
  # Convert the data type for the date column
  batch_SumdT_ym_data = batch_SumdT_ym_data %>% 
          mutate(Date = as.Date(Date))
  
  # Combine the cumulative hourly Ts-Ta for this batch with existing batches
  SumdT_data_ym = SumdT_data_ym %>% bind_rows(batch_SumdT_ym_data)
  
  if (b %% 5 ==0) { print(b); gc()}

}

# 2. Loop each date in the YM to convert into raster ----------------

YM_dates = as.Date(get_dates_ym(target_YM))

#'[Create a list marked by the date to store raster layer for cumulative hourly Ts-Ta on each day]
SumdT_ym_list = sapply(as.character(YM_dates), function(x) NULL)

for (i in seq_along(YM_dates)) {
  
  # Subset the combined cumulative hourly Ts-Ta data into the target date
  date_c = YM_dates[i]
  SumdT_data_DD  = SumdT_data_ym %>% filter(Date == date_c) 
  
  # Combine with longitude and latitude of grid cells with the column "Grid" (names of grid cells) as the marker
  SumdT_data_DD_spdf = SumdT_data_DD %>% 
          left_join(Grid_locs, by = "Grid")
  
  # Select lon, lat, and value column (i.e., SumdT) for raster conversion
  SumdT_data_DD_xyz = SumdT_data_DD_spdf %>%
      dplyr::select(lon, lat, "SumdT")
  
  # Convert the cumulative hourly Ts-Ta into the raster 
  SumdT_DD_r = rast(SumdT_data_DD_xyz, crs = 'epsg:4326')
  
  # Round raster cell values to 3 decimal places 
  SumdT_DD_r = round(SumdT_DD_r, 3)
  
  # Apply the raster template for the study region
  # use nearest neighbour method to retain the original cell values 
  # (This is useful in rare caes when there are missing cells that distort the spatial extent during the conversion of data frame into raster) 
  SumdT_DD_r_proj = project(SumdT_DD_r, SEOz_2km_template , method = 'near')
  
  # Store the daily raster into the list 
  SumdT_ym_list[[i]] = SumdT_DD_r_proj
  
  print(date_c)
  gc()
}

# 3. Combine daily layers to monthly stack and write the raster --------------

# Combine the raster layers in the list into the monthly raster stack 
SumdT_ym_stack = rast(SumdT_ym_list)

# Write the monthly raster stack of cumulative hourly Ts-Ta
terra::writeRaster(SumdT_ym_stack,
                   paste0(path2SumdT_RASTER, 
                          "SumdT_", "Wdiur_4hr_" ,
                          target_YM, "_2km.tif"), 
                   datatype = "FLT4S", overwrite = T, 
                   filetype = "GTiff", NAflag = -99) 