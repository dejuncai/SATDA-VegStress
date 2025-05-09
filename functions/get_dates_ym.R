get_dates_ym = function(ym) {
  
  first_date = as.Date(paste(ym, "01"), format = '%Y%m%d')
  end_date = ceiling_date(first_date, unit = "month") - days(1)
  seq_date = seq(first_date, end_date, 1)
  
  return(as.character(seq_date))
  
}
