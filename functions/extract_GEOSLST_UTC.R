#'@Desc *Extract hourly time series of ChibaLST on UTC hour mark by the specified target UTC hourly time steps*

# UTC_datetime_seq = midday_UTC_Hours_all

extract_GEOSLST_UTC = function(lon, lat, UTC_datetime_seq, path2LST) {
  
  # Empty template for the Himawari data for Australia if the data aren't available
  #'[This is case-specific:Please update that to your study region and target spatial resolution]
  Aus_empty_template = rast(ext(112, 154, -45, -10),
                            crs = 'epsg:4326', res = 0.02)
  values(Aus_empty_template)= NA

  # Convert target grid cell locations to spatial points for later extraction
  point_vect_lonlat = vect(data.frame(lon = lon, lat =  lat),
                           crs = 'epsg:4326')
  
  # Obtain the unique UTC dates covered by the specified datetime steps 
  UTC_datetime_seq = sort(UTC_datetime_seq)
  UTC_dates = unique( date(UTC_datetime_seq) )

  # Loop over each date in the specified datetime steps to extract geostationary Ts
  GEOS_LST_ts = NULL

  for (i in seq_along(UTC_dates)) {
    
    # Current UTC date
    date_c = UTC_dates[i]
    
    # Get subset of datetime steps in the current UTC date
    UTC_datetime_today = UTC_datetime_seq[date(UTC_datetime_seq) == date_c]
    
    # A list to store LST imagery for each datetime
    #'@Note [This is to account for the infrequent situation where data are missing for the timestep but are available in the neighbouring 10-min steps]
    GEOS_LST_UTC_datetime_list = sapply(as.character(UTC_datetime_today), function(x) NULL)
    
    for (j in seq_along(UTC_datetime_today)) {
    
      # Each datetime step on the current UTC date
      UTC_datetime_c = UTC_datetime_today[j]
      
      # Obtain string for the date and hour
      date_c = date(UTC_datetime_c)
      # Time string 
      HHMM_c = format(UTC_datetime_c, '%H%M')
      
      # Search the file name for the LST imagery for the current datetime step
      GEOS_LST_fn  = list.files(path2LST,
                                paste0(format(date_c, '%Y%m%d'), HHMM_c),
                                full.names = T)
      
      # If data are missing, try the +/-10min window and take the average to gap-fill
      if (length(GEOS_LST_fn) < 1) {
        
        # Datetime in the neighbouring 10-min window
        UTC_10min_window = UTC_datetime_c + minutes(10) * c(-1,1)
        
        # Obtain the HHMM for the 10-min window
        HHMM_10min_window = format(UTC_10min_window, '%H%M')
        
        # Search files for each datetime step for the 10-min window
        GEOS_LST_fn_10minwindow  = unlist(lapply(HHMM_10min_window, function(x) 
                list.files(path2LST, paste0(format(date_c, '%Y%m%d'), 
                                            x), full.names = T)))
        
        # Gap fill the timestep using the neighbouring 10-min window
        if (length(GEOS_LST_fn_10minwindow) == 1) {
          GEOS_LST_fn_10minwindow_mean_r = rast(GEOS_LST_fn_10minwindow)
          
        } else if ( length(GEOS_LST_fn_10minwindow) ==2) {
          # Take average of the 2 timesteps (`app` is only applicable to multiple layers)
          GEOS_LST_fn_10minwindow_mean_r = app( rast(GEOS_LST_fn_10minwindow),
                                                'mean', na.rm = T) 
        } else {
          # If neither the neighbouring 10-min timesteps have observed data, fill with NA values
          GEOS_LST_fn_10minwindow_mean_r = Aus_empty_template
        }
        
        # Store the Chiba LST for the target timestep
        GEOS_LST_UTC_datetime_list[[j]] = GEOS_LST_fn_10minwindow_mean_r
      }  else {
        
        # If hour mark data available, directly read it and store
        GEOS_LST_UTC_datetime_list[[j]] = rast(GEOS_LST_fn)
      }
    
    }
    
    # Stack the corresponding geostationary LST raster for all the datetimes on the current UTC date
    GEOS_LST_today_stack = rast(GEOS_LST_UTC_datetime_list)
    
    # Extract the LST time series for the target grid locations on the current UTC date
    GEOS_LST_today_ts = t(as.matrix(terra::extract(GEOS_LST_today_stack, 
                                                   point_vect_lonlat, ID = FALSE)))
    
    rownames(GEOS_LST_today_ts) = NULL
    
    #'@Note [Bind with available dates to ensure rows are matching ]
    GEOS_LST_today_df  = data.frame(DateTime_UTC = UTC_datetime_today, 
                                    LST = GEOS_LST_today_ts)
    
    # Bind to the full time series for the current UTC date
    GEOS_LST_ts = GEOS_LST_ts %>% bind_rows(GEOS_LST_today_df)
    
    print(paste0("Geostationary LST data are extracted for ", date_c))
    
  } # Loop for date 
  
  return(GEOS_LST_ts)
  
}
