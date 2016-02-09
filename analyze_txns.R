load_txn_data <- function()
{
  library(data.table)
  txn_data <- data.table()
  for (i in 1:4)
  {
    #cat(paste("i = ", i, "\n", sep = ""))
    filename <- paste("/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_", i, "/TC", i, "_Transactions_DataSet.csv", sep = "")
    this_set <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
    this_set[, LocalTime := strftime(strptime(this_set$LocalTime, "%d/%b/%Y %H:%M:%S"), "%H:%M:%S")]     
              
    #Get the event corresponding to each transaction
    filename <- paste("/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_", i, "/Twitter_TC_", i, ".csv", sep = "")
    events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "character", "Date"),
                    data.table = TRUE)
    events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
    events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
    #print(events)
    
    #For each transaction, find the event such that the timestamp of the transaction falls between the starttime and the end-time of the event (both boundaries included)
    this_set[, Event := apply(this_set, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
    
    txn_data <- rbindlist(list(txn_data, this_set))
  }
  txn_data
}

lookup_event <- function(LocalTime, events)
{
  #cat(paste("LocalTime = ", LocalTime, "\n", sep = ""))
  setkey(events, StartTime, EndTime) #the following line gives wrong result without setkey
  matching_row <- events[((LocalTime >= as.character(StartTime)) & (LocalTime <= as.character(EndTime))),]
  #print(matching_row)
  matching_row[, Event]
}

