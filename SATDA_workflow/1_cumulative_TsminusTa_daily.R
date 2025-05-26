#'@Desc *Compute the daily cumulative hourly land surface temperature minus air temperature (Ts-Ta) integral for the sub-diurnal window (Wdiur) straddling mid-day*
#'@Desc *with Wdiur being the most common 4-hour period with maximum total Ts-Ta*
#'@Note *Due to the large processing requirements, the computation was broken into batches of grid cells and batches of time periods (YYYYMM)*
#'@Note *However, the structure of the script is straightforward, so if a high-performance computing system isn't available*
#'@Note *but the turnaround time isn't a key constraint, one could simplify the scripts to be a longer 'for loop' and let it run longer.*

rm(list = ls())

# Load key packages
library(dplyr)
library(tidyr)
library(stringr)
library(lubridate)
library(data.table)
library(terra)
library(zoo)
library(reshape2)

work_dir = './' # {work directory, please specify your own}
# Path to geostationary land surface temperature (LST/Ts) data (here use Himawari LST from Chiba University)
path2LST = paste0(work_dir, 'input_data/geos_lst/')
# Path to sub-diurnal (hourly) air temperature grids from spatial interpolation
path2TAIR = paste0(work_dir, 'input_data/hourly_ta/')

# Source the functions relevant to the script
path2FUNS = paste0(work_dir, 'functions/')
# Path to output for individual batches of calculated daily cumulative Ts-Ta (herein use dT in the codes to denote 'difference in temperature')
path2SumdT_BATCH  = paste0(work_dir, 'test_output/1_SumdT_Batch/')

# Function to extract geostationary LST 
source(paste0(path2FUNS, 'extract_GEOSLST_UTC.R'))
# Function to extract hourly tair 
source(paste0(path2FUNS, 'extract_hourlyTair_UTC.R'))
# Function to get individual dates for a given year-month (YM) in the format of YYYYMM
source(paste0(path2FUNS, 'get_dates_ym.R'))

# Get the argument to subset to batches of grid cells and time period (i.e., Year-Month, YYYYMM) -------
 
Grid_YM_Batch  = as.character(commandArgs(trailingOnly = T))
# e.g.,
# Grid_YM_Batch = '30-11'

# Break down the batch numbers for the grid cells and the time
GridBatch = as.numeric(strsplit(Grid_YM_Batch, split = "-")[[1]][1])
YMBatch = as.numeric(strsplit(Grid_YM_Batch, split = "-")[[1]][2])

# Function to help subset the task for parallel computing 
source(paste0(path2FUNS, 'get_batch_parallel.R'))

# (1) Subset batches of grid cells 
#'@Note [Please prepare a .CSV file (or equivalent) that record the longitude and latitude of each grid cells in your study region]
#'@Note [Here the study region is the southeast Australia. An extra column to denote the grid cell number is also added, however it's optional]

Grid_locs = fread(paste0(work_dir, "Himawari_GridLocations_2km.csv"))

# Size of each batch of grid cells
grid_size = 8000

# Indices for the subset of grid cells by the automatic function 
# entire: sequence of all grid cells (to be subsetted)
Grids_Batch_inds = get_batch_parallel(batchnum = GridBatch, 
                                      entire = 1:nrow(Grid_locs), 
                                      size = grid_size)
# Batch subset of grid locations
Grids_Batch = Grid_locs[Grids_Batch_inds, ] 

# Longitude and latitude for the batch of grid locations
Grids_Batch_lonlat = Grids_Batch %>% dplyr::select(lon, lat)
# Optional: the name tag for each grid cell (in format of 'Grid1', 'Grid2', etc)
Grid_names = Grids_Batch$Grid 

# (2) Subset batches of Year-Month (YYYYMM)
Periods = seq(as.Date(paste0(2015, "-08-01")),  as.Date(paste0(2022, "-12-31")), 1)

# Individual YM for the study period
Periods_YM = unique(format(Periods, "%Y%m"))
# Size of each batch of YM (time period)
YM_size = 5

# Indices for the subset of YM by the automatic function
# Entire: sequence of all YM to be subsetted
Batch_YM = get_batch_parallel(batchnum = YMBatch, 
                              entire = Periods_YM, size = YM_size)

#################################################################################### ~
########## MAIN LINE: Compute daily cumulative Ts-Ta at the 4-hour sub-diurnal window with maximum total Ts-Ta   ------
#################################################################################### ~

#'@Note [Read the raster for the most common 4-hour period (Wdiur) with maximum total Ts-Ta]
#'@Note [For simpler data storage, this timing raster is marked by the start hour of Wdiur in UTC time] 

MaxdT_Wdiur_UTCStart_r = rast(paste0(work_dir, 'SEOz_MaxdT_Wdiur_4hr_UTCStart.tif'))

for (ym in Batch_YM) {
  
  # Get the sequence of dates in the target YM 
  YM_dates = get_dates_ym(ym)
  
  for (i in seq_along(YM_dates)) {
    # Current date
    date_c = YM_dates[i]
    
    # 1. Organise data frame for timesteps of the 4-hour (Ts-Ta) window for each grid cell -----

    # Extract the start UTC hour for the 4-hour (Ts-Ta) window (Wdiur)
    Wdiur_UTCStart_values = terra::extract(MaxdT_Wdiur_UTCStart_r, 
                                           Grids_Batch_lonlat, ID = F)[,1]
    
    # Create the data frame with lon, lat and grid names
    Grids_UTCStart_df = data.frame(Grids_Batch, UTCStart = Wdiur_UTCStart_values)
    
    # Convert the start UTC hour to a UTC date time variable 
    Grids_UTCStart_df  = Grids_UTCStart_df %>% 
      mutate(UTCStart = paste0(date_c,  " ", 
                               paste0(sprintf('%02d', UTCStart), ":00")) ) %>%
      # Parse to date-time object by specifying format 
      mutate(UTCStart = as.POSIXct(UTCStart, tz="UTC",
                                   format = '%Y-%m-%d %H:%M') )
    
    # Add columns of each hourly timestep for the 4-hour window specific to each grid cell
    Grids_Wdiur_4hr_UTC_df  = Grids_UTCStart_df %>% 
      mutate(Hr2_UTC = UTCStart + hours(1),
             Hr3_UTC = UTCStart + hours(2),
             Hr4_UTC = UTCStart + hours(3),
             EndHr_UTC = UTCStart + hours(4))
    
    # Melt timesteps of 4-hour Wdiur to long format, facilitating later data manipulation
    # This acts as the look-up table for the grid cells and corresponding timesteps required
    Grids_Wdiur_4hr_UTC_melt = reshape2::melt(Grids_Wdiur_4hr_UTC_df, 
                                              id.vars = c("lon", "lat", "Grid"),
                                              variable.name = 'TimeStamp', 
                                              value.name = "DateTime_UTC") %>%
                    dplyr::select(-TimeStamp)
    
    # All unique sub-diurnal timesteps for the 4-hour window across all grid cells for later data extraction 
    #'[They are subset for extracting hour mark + 10-min window data]
    Wdiur_4hr_UTC_time = seq(min(Grids_Wdiur_4hr_UTC_melt$DateTime_UTC, na.rm = T),
                             max(Grids_Wdiur_4hr_UTC_melt$DateTime_UTC, na.rm = T), 
                             '1 hour')

    # 2. Extract geostationary LST time series for the required sub-diurnal timesteps on the current date ---------
    
    LST_Wdiur_4hr_UTC_ts  = extract_GEOSLST_UTC(lon = Grids_Batch_lonlat$lon,
                                               lat = Grids_Batch_lonlat$lat, 
                                               UTC_datetime_seq = Wdiur_4hr_UTC_time, 
                                               path2LST = path2LST)
    # Add the corresponding UTC timesteps
    LST_Wdiur_4hr_UTC_ts = LST_Wdiur_4hr_UTC_ts %>% 
      mutate(DateTime_UTC = as.POSIXct(DateTime_UTC, tz = 'UTC'))
    
    # Assign the names for columns using the name tags for the grid cells
    names(LST_Wdiur_4hr_UTC_ts) = c("DateTime_UTC", Grid_names)
    
    # Melt the multiple grid-cell LST time series to long format, facilitating later data manipulation
    LST_Wdiur_4hr_UTC_melt = reshape2::melt(LST_Wdiur_4hr_UTC_ts, 
                                          id.vars = c( "DateTime_UTC"),
                                          variable.name = "Grid", value.name = "LST") 
    
    # Bind the extracted LST to the grid cell look-up table with locations, names and the required timesteps 
    # This can remove unneeded time steps not covered by grid cell-wise 4-hour window (Wdiur)
    
    LST_Wdiur_4hr_UTC_df = Grids_Wdiur_4hr_UTC_melt %>% 
      left_join(LST_Wdiur_4hr_UTC_melt,
                by = c("Grid",  "DateTime_UTC")) %>% 
      # Arrange by grid
      dplyr::arrange(Grid, DateTime_UTC) 
    
    # Extra check to ensure that each grid cell has exactly 5 extracted values for the 4-hour window
    LST_Wdiur_4hr_UTC_df = LST_Wdiur_4hr_UTC_df %>% 
      group_by(Grid) %>% 
      # Number of LST observations for each grid cell
      mutate(N_Ts = length(LST)) %>% 
      ungroup() %>% filter(N_Ts ==5) %>% 
      # Remove the column for checking purpose
      dplyr::select(-N_Ts)

    ####################################################################################################~
    
    # 3. Extract hourly tair time series for the required sub-diurnal timesteps on the current date ----------------
    
    # Extract hourly tair time series
    Tair_Wdiur_4hr_UTC_ts =  extract_hourlyTair_UTC(lon = Grids_Batch_lonlat$lon,
                                                   lat = Grids_Batch_lonlat$lat, 
                                                   UTC_datetime_seq = Wdiur_4hr_UTC_time,
                                                   path2TAIR = path2TAIR )
    # Add the corresponding UTC timesteps
    Tair_Wdiur_4hr_UTC_ts = Tair_Wdiur_4hr_UTC_ts %>% 
        mutate(DateTime_UTC = as.POSIXct(DateTime_UTC, tz = 'UTC'))
    
    # Assign the names for columns using the name tags for the grid cells
    names(Tair_Wdiur_4hr_UTC_ts) = c("DateTime_UTC", Grid_names)
    
    # Melt the multiple grid-cell hourly tair time series to long format, facilitating later data manipulation
    Tair_Wdiur_4hr_UTC_melt = reshape2::melt(Tair_Wdiur_4hr_UTC_ts, 
                                             id.vars = c( "DateTime_UTC"),
                                             variable.name = "Grid", value.name = "Tair") %>% 
      #'Convert tair from degree Celsius to Kelvin]
      mutate(Tair = Tair + 273.15)
    
    # Bind to the data frame for LST by Grid and UTC timesteps
    LSTTair_Wdiur_4hr_UTC_df = LST_Wdiur_4hr_UTC_df %>% 
      left_join(Tair_Wdiur_4hr_UTC_melt,
                by = c("DateTime_UTC", "Grid"))
    
    # Add the current date
    LSTTair_Wdiur_4hr_UTC_df = LSTTair_Wdiur_4hr_UTC_df %>% 
      mutate(Date = date_c) %>%
      relocate(Date, .before = DateTime_UTC ) %>% 
      # Arrange by grid
      dplyr::arrange(Grid, DateTime_UTC) 

    # 4. Compute the 4-hour Ts-Ta integral for the 4-hour window at each grid cell------
    
    # Compute difference in temperatuer (`dT`) between land surface and air
    SumdT_Wdiur_4hr_df  =  LSTTair_Wdiur_4hr_UTC_df %>%
            mutate(dT = round(LST-Tair, 3)) 
    
    # Compute (Ts-Ta) integral for each grid cell (signified by the `group_by()`)
    #'@Note [na.rm = F so strictly sum over 5 consecutive cloud-free LST in the 4-hour window]
    #'@Note [this is to ensure direct comparability of Ts-Ta integral between days by computing from the same number of observations)]
    
    SumdT_Wdiur_4hr_df =  SumdT_Wdiur_4hr_df %>% 
      group_by(Date, Grid) %>% 
      summarise(Sum_dT = sum(dT, na.rm = F)) %>% ungroup() 
    
    # 5. Write to the output directory of batch-specific (Ts-Ta) integral --------
    
    write.table(SumdT_Wdiur_4hr_df, 
                #'@Note *Output file marked by the batch number of grid cells and the specific YM
                file = paste0(path2SumdT_OUT,  
                              "SumdT_", "Wdiur_4hr_" ,
                              "Batch",  GridBatch, "_",   ym,   "_2km.csv"), 
                quote = F, row.names = F, sep = ",",
                # Append to the output file for each date to track the progress
                append = T, col.names = F)
    
    if (i%%2 == 0) { print(paste0("---------- (Ts-Ta) integral for ", date_c, " has finished computation. ----------")) }
    
    gc()
    
  }  # Loop for each date
  
  print(paste0('------ (Ts-Ta) integral  completed for ', ym, " ---------------"))
  
} # Loop for each YM

