#'@Desc *Obtain the dates for the multi-year climatology window surrounding a specific date*
#'@param target_date target date for computing multi-year climatology window
#'@param full_dates the full sequence of dates considered in the analysis
#'@param window length of the temporal window surrounding the centre day (default is 15 equivalent to a 31-day moving window around the target date)

get_climwindow_dates = function(target_date, full_dates, window = 15) {
  
  # Obtain the target date's month-date 
  md_target_date = format(target_date, "%m-%d")
  # The month-date of all the dates in the period considered
  md_full_dates = format(full_dates, "%m-%d")
  
  if (md_target_date == '02-29') {
    
    ## Special case of the leap year
    # If the target date is on 29/Feb, take the climatology window like in the leap year across the full analysis period
    # This is because 29/Feb is not present in non-leap years, so we can't directly get the climatology window 
    md_2016 = as.Date(paste0(2016, '-', md_target_date))
    
    # Climatology window surrounding this date in the leap year
    leap_yr_window = seq(md_2016 - days(window),
                         md_2016 + days(window), 1)
    
    # Obtain all month-dates in the climatology window, then sampled across full analysis period
    leap_yr_window_md = unique(format(leap_yr_window, '%m-%d'))
    
    #'[Subset the full period to dates matching all the month-dates present in this window]
    # This obtains all the dates in the analysis period that fall into the climatology window for the target date
    climwindow_dates = full_dates[md_full_dates %in% leap_yr_window_md]
    
  } else {
    
    # For other month-dates, directly take the window around the target month-date in each year
    
    ### Dates matching this month-date in the full analysis period
    target_date_allyrs = full_dates[which(md_full_dates == md_target_date)]
    
    # Use `lapply` to vectorise the computation of the window of dates surrounding this month-date in each year
    climwindow_list = lapply(target_date_allyrs, function(x)
      as.character(seq(x - days(window), x + days(window), 1))
    )
    # Collect all the dates and convert to Date data type
    climwindow_dates = as.Date(unlist(climwindow_list))
    
    # Filter the climatology dates to the dates contained in the full analysis period 
    # This extra step is optional, but it's more robust if the full analysis period is not full calendar year
    climwindow_dates = climwindow_dates[climwindow_dates %in% full_dates]
  }
  
  return(climwindow_dates)
}