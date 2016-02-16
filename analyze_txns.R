library(rpart)
library(ggplot2)
library(data.table)

load_txn_data <- function()
{
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
    #and Tweet + Image (127/1196, or, 10.6%). The events of major interest are: Tweet + Image (127/1196, or, 10.6%), Retweet (14/1196, or, 1.17%), Tweet (there is none, but there are 
    #13 or 1.08% transactions labeled as "direct message tweet + Image"), reply Tweet (2, or 0.16%). However, in the first set, there are 31 out of 301 transactions that correspond to Tweet + Image (between 
    #16:09:17 and 16:09:22, and for all of them except one, the domain name is *.twimg.com. For Retweet, in the first set, there is only one out of 301 transactions with domain name twitter.com - it would take
    #further analysis to see if Tx or Rx stand out - Rx seems low at 3028 bytes.
    
    txn_data <- rbindlist(list(txn_data, this_set))
  }
  #Check by sort(as.character(unique(txn_data$Event)))
  txn_data
}

load_packet_data <- function()
{
  packet_data <- data.table()
  for (i in 1:4)
  {
    filename <- paste("/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_", i, "/TC", i, "_PacketData_DataSet.csv", sep = "")
    this_set <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
    this_set[, LocalTime := strftime(strptime(this_set$LocalTime, "%d/%b/%Y %H:%M:%S"), "%H:%M:%S")]     
              
    #Get the event corresponding to each packet
    filename <- paste("/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_", i, "/Twitter_TC_", i, ".csv", sep = "")
    events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "character", "Date"),
                    data.table = TRUE)
    events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
    events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
    #For each packet, find the event such that the timestamp of the packet falls between the starttime and the end-time of the event (both boundaries included)
    this_set[, Event := apply(this_set, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
    this_set[, Event := apply(this_set, 1, function(row) standardize_event(as.character(row["Event"])))]
    
    #After standardization, there are 18 possible values of event: the most common ones are Scroll Timeline (8993/26204, or 34%), Click on Profile (4672/26204, or 17.8%), Login Twitter (4181/26204, or, 16%) 
    #and Tweet + Image (2512/26204, or, 9.58%). The events of major interest are: Tweet + Image (2512/26204, or, 9.58%), Retweet (997/26204, or, 3.8%), Tweet (there is none, but there are 
    #325 or 1.24% packets labeled as "direct message tweet + Image"), reply Tweet (14, or 0.05%).
    
    packet_data <- rbindlist(list(packet_data, this_set))
  }
  #Check by sort(as.character(unique(packet_data$Event)))
  packet_data
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

is_interesting_event <- function(event)
{
  if (event %in% c("Tweet + Image", "Retweet", "reply Tweet")) 
     return(event)
  return("Other")
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
  
  model <- rpart("Event ~ Tx + Rx + factor(DomainName)", data = training_data)
  #print(varImp(model))
  test_data[, predicted_event := as.character(predict(model, newdata = test_data, type = "class"))]
  prec_recall <- table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted'))
  
  #Measure overall accuracy
  setkey(test_data, Event, predicted_event)
  cat(paste("Overall accuracy = ", nrow(test_data[(Event == predicted_event),])/nrow(test_data), "\n\n", sep = "")) #0.43143
  dt_prec_recall <- as.data.table(prec_recall)
  
  #Compute the micro-average recall values of the classes
  
  setkey(dt_prec_recall, actual)
  row_totals <- dt_prec_recall[, list(row_total = sum(N)), by = actual]
  setkey(row_totals, actual)
  for_recall <- dt_prec_recall[row_totals, nomatch = 0]
  setkey(for_recall, actual, predicted)
  for_recall <- for_recall[(actual == predicted),]
  for_recall[, recall := N/row_total]
  for_recall <- for_recall[, .SD, .SDcols = c("actual", "recall")]
  setnames(for_recall, "actual", "Event")
  print(for_recall)
  cat("\n")
  
  #Compute the micro-average precision values of the classes
  
  setkey(dt_prec_recall, predicted)
  column_totals <- dt_prec_recall[, list(column_total = sum(N)), by = predicted]
  setkey(column_totals, predicted)
  for_precision <- dt_prec_recall[column_totals, nomatch = 0]
  setkey(for_precision, actual, predicted)
  for_precision <- for_precision[(actual == predicted),]
  for_precision[, precision := N/column_total]
  for_precision <- for_precision[, .SD, .SDcols = c("predicted", "precision")]
  print(for_precision)
  
  #prec_recall
  model
}

#Do import pycrfsuite in python. Documentation is at http://python-crfsuite.readthedocs.org/en/latest/index.html

visualizations <- function()
{
  #input_data <- load_txn_data()
  input_data <- load_packet_data()
  input_data[, Event := apply(input_data, 1, function(row) is_interesting_event(as.character(row["Event"])))]
  #filename <- "./figures/bytes_received_by_transactions.png"  
  filename <- "./figures/bytes_received_by_packets.png" 
  png(filename, width = 600, height = 480, units = "px")

  p <- ggplot(input_data, aes(x = Rx, colour = Event)) + geom_histogram(aes(y = ..density..)) + geom_density() + scale_x_log10() + 
              labs(x = "Bytes received by packets") + ylab("Density")
  print(p)
  aux <- dev.off()
  
  setkey(input_data, Event)
  for (event in c("Tweet + Image", "Retweet", "reply Tweet", "Other"))
  {
    this_set <- input_data[(Event == event),]
    cat(paste("event = ", event, ", nrow(this_set) = ", nrow(this_set), "\n", sep = ""))
    cat("Distribution of Rx is\n")
    print(fivenum(this_set$Rx))
  }
  cat("\n\n")
  for (event in c("Tweet + Image", "Retweet", "reply Tweet", "Other"))
  {
    this_set <- input_data[(Event == event),]
    cat(paste("event = ", event, ", nrow(this_set) = ", nrow(this_set), "\n", sep = ""))
    cat("Distribution of Tx is\n")
    print(fivenum(this_set$Tx))
  }
}

#To train with CRF++, from /Users/blahiri/cleartrail_analytics, run the following command: /Users/blahiri/crf++/CRF++-0.58/crf_learn crf_template_ct /Users/blahiri/cleartrail_osn/for_CRF/for_CRF.data model_ct

prepare_packet_data_for_CRF <- function()
{
  packet_data <- data.table()
  for (i in 1:1)
  {
    filename <- paste("/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_", i, "/TC", i, "_PacketData_DataSet.csv", sep = "")
    this_set <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
    this_set[, LocalTime := strftime(strptime(this_set$LocalTime, "%d/%b/%Y %H:%M:%S"), "%H:%M:%S")]     
              
    #Get the event corresponding to each packet
    filename <- paste("/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_", i, "/Twitter_TC_", i, ".csv", sep = "")
    events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "character", "Date"),
                    data.table = TRUE)
    events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
    events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
    #For each packet, find the event such that the timestamp of the packet falls between the starttime and the end-time of the event (both boundaries included)
    this_set[, Event := apply(this_set, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
    this_set[, Event := apply(this_set, 1, function(row) standardize_event(as.character(row["Event"])))]
    
    packet_data <- rbindlist(list(packet_data, this_set))
  }
  filename <- "/Users/blahiri/cleartrail_osn/for_CRF/for_CRF.data"
  packet_data <- packet_data[, .SD, .SDcols = c("Tx", "Rx", "DomainName", "Event")]
  packet_data[, Event := apply(packet_data, 1, function(row) gsub(" ", "_", as.character(row["Event"])))] #Do cut -f 4 -d ' ' for_CRF.data | uniq to check results
  write.table(packet_data, filename, sep = " ", row.names = FALSE, col.names = FALSE, quote = FALSE)
}

