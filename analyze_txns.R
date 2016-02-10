library(rpart)

load_txn_data <- function()
{
  library(data.table)
  txn_data <- data.table()
  for (i in 1:4)
  {
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
    #For each transaction, find the event such that the timestamp of the transaction falls between the starttime and the end-time of the event (both boundaries included)
    this_set[, Event := apply(this_set, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
    this_set[, Event := apply(this_set, 1, function(row) standardize_event(as.character(row["Event"])))]
    
    #After standardization, there are 18 possible values of event: the most common ones are Scroll Timeline (406/1196, or 34%), Click on Profile (205/1196, or 17%), Login Twitter (150/1196, or, 12.5%) 
    #and Tweet + Image (127/1196, or, 10.6%).
    
    txn_data <- rbindlist(list(txn_data, this_set))
  }
  #Check by sort(as.character(unique(txn_data$Event)))
  txn_data
}

lookup_event <- function(LocalTime, events)
{
  setkey(events, StartTime, EndTime) #the following line gives wrong result without setkey
  matching_row <- events[((LocalTime >= as.character(StartTime)) & (LocalTime <= as.character(EndTime))),]
  matching_row[, Event]
}

standardize_event <- function(event)
{
  if (event %in% c("click on home", "click on Home")) 
     return("Click on Home")
  if (event %in% c("click on profile", "click on Profile", "click on profile chennai", "click on profile HT", "click on profile Melbourne", "click on profile sunny"))
    return("Click on Profile")
  if (event %in% c("Like favorites", "Like Favorites"))
    return("Like Favorites")
  if (event %in% c("login Twitter", "LogIn Twitter"))
    return("Login Twitter")
  if (event %in% c("retweet", "Retweet"))
    return("Retweet")
  if (event %in% c("scroll timeline", "scroll Timeline", "Scroll Timeline"))
    return("Scroll Timeline")
  if (event %in% c("tweet + Image", "Tweet + Image"))
    return("Tweet + Image")
  event
}

#Can we predict event for a transaction based on Tx, Rx and DomainName?
event_from_txn <- function()
{
  require(nnet)
  set.seed(1)
  txn_data <- load_txn_data()
  txn_data$Event <- as.factor(txn_data$Event)
  train = sample(1:nrow(txn_data), 0.5*nrow(txn_data))
  test = (-train)
  cat(paste("Size of training data = ", length(train), ", size of test data = ", (nrow(txn_data) - length(train)), "\n", sep = ""))
  
  training_data <- txn_data[train, ]
  test_data <- txn_data[test, ]
  
  #training_data$Event <- relevel(training_data$Event, ref = "Login Twitter")
  #logr <- multinom("Event ~ Tx + Rx + factor(DomainName)", data = training_data)
  model <- rpart("Event ~ Tx + Rx + factor(DomainName)", data = training_data)
  test_data[, predicted_event := as.character(predict(model, newdata = test_data, type = "class"))]
  print(table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted')))
}

