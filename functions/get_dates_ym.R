#'@Desc *Obtain the list of dates for the target Year-Month (ym)*
#'@param ym the year-month in the form of YYYYMM (e.g., 201612 for Dec 2016); character

get_dates_ym = function(ym) {
  library(lubridate)
  
  # First date of the month
  first_date = as.Date(paste(ym, "01"), format = '%Y%m%d')
  # Last date of the month
  end_date = ceiling_date(first_date, unit = "month") - days(1)
  # Obtain the daily sequence
  seq_date = seq(first_date, end_date, 1)
  
  return(as.character(seq_date))
}