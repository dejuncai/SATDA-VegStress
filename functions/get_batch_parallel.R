#'@Desc *Function to subset the large-scale task by the batch number and the batch size*
#'@Desc *The task to be subset can be grid cells and/or timesteps*

#'@param batchnum the number for the 'batch' / 'subset'; normally passed by the cluster argument (numeric)
#'@param entire entire list of values / arguments to subset into batches; a vector 
#'@param size size of the batch (numeric)

get_batch_parallel = function(batchnum, entire, size) {
  
  # Total number of values / arguments to be subsetted
  N_args = length(entire)
  
  # Start and the end index for the batch given the input batch number
  start_ind = size * (batchnum - 1) + 1
  upper_bound = size * batchnum
  
  if (upper_bound > N_args & start_ind <= N_args) {
    # If the upper bound of the batch is greater than the total number: 
    # the upper bound would be the final index of the entire values
    batch_args = entire[start_ind : N_args]
    
  } else if (upper_bound <= N_args) {
    # If the upper bound of the batch is within the total number of values 
    # It will be a normal full subset of values with the desired size 
    end_ind = size * batchnum
    batch_args = entire[start_ind : end_ind]
    
  } else if (start_ind > N_args) {
    # If start index is greater than the total number of values: it's an error, stop.
    stop("Batch index exceeds total points, stop")
  }
  
  return(batch_args)
}
