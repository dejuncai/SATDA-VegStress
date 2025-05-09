#'@param batchnum the number for the order of the 'batch' or the 'subset'; can be a subset of grid cells or temporal periods.
#'@param entire entire list of arguments to separate out; most likely a vector 
#'@param size size of the batch

get_batch_parallel = function(batchnum, entire, size) {
  
  # Total number of arguments to be subsetted
  N_args = length(entire)
  
  # Start and the end index for the batch given the input batch number
  start_ind = size * (batchnum - 1) + 1
  upper_bound = size * batchnum
  
  # If the end index of the batch is greater than the total number: 
  # the end index would be the final number of the argument
  if (upper_bound > N_args & start_ind <= N_args) {
    batch_args = entire[start_ind : N_args]
  } else if (upper_bound <= N_args) {
    # If the end index of the batch is within the total number of argument 
    # Subset the full batch
    end_ind = size * batchnum
    batch_args = entire[start_ind : end_ind]
  } else if (start_ind > N_args) {
    # If start index is greater than the total number of argument: can't run, pass*
    stop("Batch index exceeds total points, stop")
  }
  
  return(batch_args)
}
