#'@Desc *Extract hourly time series of gridded air temperature by the specified target UTC hourly time steps*
#'@param lon vector of longitudes of locations (numeric)
#'@param lat vector of latitudes of locations (numeric)
#'@param UTC_datetime_seq vector of time steps in UTC time for the time series
#'@param path2TAIR path to the directory that stores hourly air temperature (user-specific)

extract_hourlyTair_UTC = function(lon, lat, UTC_datetime_seq, path2TAIR) {

  #'@Note *Use generic situation with lon-lat as inputs for illustration purposes*

  # Convert target grid cell locations to spatial points for later extraction
  point_vect_lonlat = vect(data.frame(lon = lon, lat =  lat),
                           geom = c("lon", "lat"),
                           crs = 'epsg:4326')
  # Projection to the corresponding Australian albers projection for the gridded hourly dataset
  point_vect_proj = project(point_vect_lonlat, 'epsg:3577')

  # Obtain the unique UTC dates (the hourly tair data are stacked into daily grids, each layer being one hour)
  UTC_dates = unique( date(UTC_datetime_seq) )
  
  # Loop over each date to extract data from each daily stack
  HourlyTa_ts = NULL
  
  for (i in seq_along(UTC_dates)) {
    # Current UTC date
    date_c = UTC_dates[i]
    
    # File name for the hourly tair for the current UTC date 
    HourlyTa_fn_date = list.files(path2TAIR,
                                  paste0(format(date_c, '%Y%m%d'), ".*tif$"),
                                  full.names = T)
    # Load the daily stack for the hourly tair for the current UTC date
    HourlyTa_date_stack = rast(HourlyTa_fn_date)
    
    # Get subset of hours in the current date that are required in the input datetime steps
    UTC_hours_date_c = UTC_datetime_seq[which(date(UTC_datetime_seq) == date_c)]
    
    # Get the hour mark to get layers of the index 
    #'@Note *Hour mark plus 1 is the index because the 24th layer is 23:00 UTC*
    
    UTC_hours_layer = hour(UTC_hours_date_c) + 1
    
    # Subset the included hours for this date in the input datetime steps
    HourlyTa_UTC_date_SUB = HourlyTa_date_stack[[UTC_hours_layer]]  
    
    #'@Note Extract monthly time series from raster stack
    HourlyTa_date_DATA  = t(as.matrix(terra::extract(HourlyTa_UTC_date_SUB, 
                                                     point_vect_proj, ID = FALSE)))
    
    #'@Note [Account for one point vs multi-point extraction]
    if (nrow(point_vect_proj) == 1 ) {
      HourlyTa_ts = c(HourlyTa_ts, HourlyTa_date_DATA)
    } else {
      HourlyTa_ts = rbind(HourlyTa_ts, HourlyTa_date_DATA)
    }
    
  }
  
  rownames(HourlyTa_ts) = NULL
  
  # Robust handling of UTC datetime, store as string
  UTC_datetime_string = as.character(UTC_datetime_seq)
  
  #' Fill HMS 00:00:00 on the UTC hour mark to avoid issues in downstream conversion]
  UTC_datetime_YMDHMS = ifelse(str_length(UTC_datetime_string) < 19, 
                               paste0(UTC_datetime_string, " 00:00:00"),
                               UTC_datetime_string)
  
  # Combine the extracted time series with *DateTime* in UTC to get a complete data frame
  HourlyTa_df = data.frame(DateTime_UTC = UTC_datetime_YMDHMS, Tair = HourlyTa_ts)
  
  return(HourlyTa_df) 
  
}
