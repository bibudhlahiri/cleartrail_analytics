library(data.table)
library(rpart)

label_packets <- function()
{
  #filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/RevisedPacketData_DevQA_TestCase1.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET2/Production_DataSet_2/RevisedPacketData_ProducionTestCase2.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
                    
  #Get the event corresponding to each transaction
  #filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/FP_Twitter_17_Feb.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET2/Production_DataSet_2/Twittertestcase_10_Feb.csv"
  events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("character", "Date", "Date"),
                    data.table = TRUE)
  events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
  
  #Eliminate timestamps for which we do not have any labels
  user_starts_at <- events[1, StartTime]
  user_ends_at <- events[nrow(events), EndTime]
  setkey(revised_packets, LocalTime)
  revised_packets <- revised_packets[((LocalTime >= as.character(user_starts_at)) & (LocalTime <= as.character(user_ends_at))),]
  
  #Model it like HMM and solve it with CRF where the activity at a point in time is the hidden state, and the session and flow-related features at the same point in time
  #create the visible states. The session and flow-related features come by aggregating session and flow-related data at each instant.
  
  revised_packets <- revised_packets[order(LocalTime),]
  hidden_and_vis_states <- revised_packets[, list(n_packets = length(Rx), n_sessions = uniqueN(session_id), n_flows = uniqueN(flow_id), 
                                                  n_downstream_packets = get_n_downstream_packets(.SD),
                                                  n_upstream_packets = get_n_upstream_packets(.SD), 
                                                  majority_domain = get_majority_domain(.SD),
                                                  upstream_bytes = get_upstream_bytes(.SD),
                                                  downstream_bytes = get_downstream_bytes(.SD)
                                            ), by = LocalTime,
                                           .SDcols=c("Direction", "Rx", "Tx", "session_id", "flow_id", "DomainName")]
  hidden_and_vis_states$majority_domain <- vapply(hidden_and_vis_states$majority_domain, paste, collapse = ", ", character(1L))
  hidden_and_vis_states[, Event := apply(hidden_and_vis_states, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
  hidden_and_vis_states$Event <- as.character(hidden_and_vis_states$Event)
  
  #There are some "holes" in time when we do not know what happened. Let us skip those for now.
  setkey(hidden_and_vis_states, Event)
  hidden_and_vis_states <- hidden_and_vis_states[((nchar(Event) > 0) & (Event != "character(0)")),]
  
  #Add a few more features
  hidden_and_vis_states[, frac_upstream_packets := n_upstream_packets/n_packets]
  hidden_and_vis_states[, frac_downstream_packets := n_downstream_packets/n_packets]
  hidden_and_vis_states[, avg_packets_per_session := n_packets/n_sessions]
  hidden_and_vis_states[, avg_packets_per_flow := n_packets/n_flows]
  
  #filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET2/Production_DataSet_2/hidden_and_vis_states.csv"
  
  #Re-order by time before writing to CSV
  setkey(hidden_and_vis_states, LocalTime)
  hidden_and_vis_states <- hidden_and_vis_states[order(LocalTime),]
  write.table(hidden_and_vis_states, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  hidden_and_vis_states
}

get_n_downstream_packets <- function(dt)
{
  nrow(dt[(Direction == "downstream")])
}

get_n_upstream_packets <- function(dt)
{
  nrow(dt[(Direction == "upstream")])
}

get_majority_domain <- function(dt)
{
  tt <- table(dt$DomainName)
  ret_value <- names(tt[which.max(tt)])
  #print(ret_value)
  #cat(paste("class(ret_value) = ", class(ret_value), "\n", sep = ""))
  ret_value
}

get_upstream_bytes <- function(dt)
{
  upstream_packets <- dt[(Direction == "upstream")]
  sum(upstream_packets$Tx)
}

get_downstream_bytes <- function(dt)
{
  downstream_packets <- dt[(Direction == "downstream")]
  sum(downstream_packets$Rx)
}

lookup_event <- function(LocalTime, events)
{
  setkey(events, StartTime, EndTime) #the following line gives wrong result without setkey
  matching_row <- events[((LocalTime >= as.character(StartTime)) & (LocalTime <= as.character(EndTime))),]
  matching_row[, Event]
}

apply_decision_tree <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states_DevQA_TestCase1.csv"
  hidden_and_vis_states <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric", 
                                   "character", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  hidden_and_vis_states$Event <- as.factor(hidden_and_vis_states$Event)
                   
  train = sample(1:nrow(hidden_and_vis_states), 0.7*nrow(hidden_and_vis_states))
  test = (-train)
  cat(paste("Size of training data = ", length(train), ", size of test data = ", (nrow(hidden_and_vis_states) - length(train)), "\n", sep = ""))
  
  training_data <- hidden_and_vis_states[train, ]
  test_data <- hidden_and_vis_states[test, ]
  
  #Because of the random split, if we encounter values of Event in test_data that were not encountered in training_data, then there will be a problem. Avoid that.
  test_data <- test_data[test_data$majority_domain %in% unique(training_data$majority_domain),] 
  
  model <- rpart("Event ~ n_packets + n_sessions + n_flows + n_downstream_packets + n_upstream_packets + 
                  factor(majority_domain) + upstream_bytes + downstream_bytes + frac_upstream_packets + frac_downstream_packets + 
                  avg_packets_per_session + avg_packets_per_flow", data = training_data)
  #print(varImp(model))
  test_data[, predicted_event := as.character(predict(model, newdata = test_data, type = "class"))]
  prec_recall <- table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, Event, predicted_event)
  cat(paste("Overall accuracy = ", nrow(test_data[(Event == predicted_event),])/nrow(test_data), "\n\n", sep = "")) #0.608391
  
  #Compute the micro-average recall values of the classes
  
  dt_prec_recall <- as.data.table(prec_recall)
  setkey(dt_prec_recall, actual)
  row_totals <- dt_prec_recall[, list(row_total = sum(N)), by = actual]
  setkey(row_totals, actual)
  for_recall <- dt_prec_recall[row_totals, nomatch = 0]
  setkey(for_recall, actual, predicted)
  for_recall <- for_recall[(actual == predicted),]
  for_recall[, recall := N/row_total]
  setnames(for_recall, "actual", "Event")
  print(for_recall) #Re-Tweet, Reply Tweet and Tweet + Image have recall values 0.5961538, 0.7391304 and 0.8148148 respectively
  cat("\n")
  
  #Compute the micro-average precision values of the classes
  
  setkey(dt_prec_recall, predicted)
  column_totals <- dt_prec_recall[, list(column_total = sum(N)), by = predicted]
  setkey(column_totals, predicted)
  for_precision <- dt_prec_recall[column_totals, nomatch = 0]
  setkey(for_precision, actual, predicted)
  for_precision <- for_precision[(actual == predicted),]
  for_precision[, precision := N/column_total]
  print(for_precision) #Re-Tweet, Reply Tweet and Tweet + Image have recall values 0.6078431, 0.4857143 and 1.0 respectively
  
  #Some of the most important predictors are: majority_domain (0.14384041), upstream_bytes (0.14375551), n_packets (0.14084035), n_upstream_packets (0.13911527), avg_packets_per_session (0.13765281),
  #n_flows (0.09794633)
  model
}


#We take the packets in sessions, and look up for the events corresponding to the packets through timestamps. We group the packets in sessions as if packets are words/tokens and
#sessions are sentences. Then, we apply CRF on the training data and fit the model on test data. We should split all the available sessions into two halves: training and testing, but should not 
#split the packets in a single session. 
#To train with CRF++, from ~/cleartrail_analytics, run the following command: ~/crf++/CRF++-0.58/crf_learn SET2/crf_template_ct ~/cleartrail_osn/for_CRF/SET2/train_ct_CRF.data model_ct
#To test with CFR++, run ~/crf++/CRF++-0.58/crf_test -m model_ct ~/cleartrail_osn/for_CRF/SET2/test_ct_CRF.data > ~/cleartrail_osn/for_CRF/SET2/predicted_labels_ct.data

prepare_packet_data_for_CRF <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/RevisedPacketData_DevQA_TestCase1.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
  setkey(revised_packets, session_id, LocalTime)
  revised_packets <- revised_packets[order(session_id, LocalTime),]
  
  #Get the event corresponding to each packet
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/FP_Twitter_17_Feb.csv"
  events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("character", "Date", "Date"),
                    data.table = TRUE)
  events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
  #For each packet, find the event such that the timestamp of the packet falls between the starttime and the end-time of the event (both boundaries included)
  revised_packets[, Event := apply(revised_packets, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
  revised_packets$Event <- as.character(revised_packets$Event)
  
  #Eliminate timestamps for which we do not have any labels because they are prior to the user starts or after the user ends
  user_starts_at <- events[1, StartTime]
  user_ends_at <- events[nrow(events), EndTime]
  setkey(revised_packets, LocalTime)
  revised_packets <- revised_packets[((LocalTime >= as.character(user_starts_at)) & (LocalTime <= as.character(user_ends_at))),]
  
  #There can still be some "holes" in time when we do not know what happened. Let us skip those for now.
  setkey(revised_packets, Event)
  revised_packets <- revised_packets[((nchar(Event) > 0) & (Event != "character(0)")),]
  
  #Change the key back to session_id and LocalTime so that all packets for a session are printed together
  setkey(revised_packets, session_id, LocalTime)
  #We need to retain the timestamp in the test data so that we can group by it later to get the majority vote among labeled events for a timestamp.
  revised_packets <- revised_packets[, .SD, .SDcols = c("session_id", "LocalTime", "Tx", "Rx", "DomainName", "Direction", "Event")]
  revised_packets[, Event := apply(revised_packets, 1, function(row) gsub(" ", "_", as.character(row["Event"])))]
  
  #Count the sessions and split in two
  n_sessions <- max(revised_packets$session_id)
  train <- 1:(floor(n_sessions/2))
  test <- (floor(n_sessions/2) + 1):n_sessions
    
  training_data <- revised_packets[(session_id %in% train),]
  test_data <- revised_packets[(session_id %in% test),]
  cat(paste("n_sessions = ", n_sessions, ", size of training data = ", nrow(training_data), ", size of test data = ", nrow(test_data), "\n", sep = ""))
  
  print(table(training_data$Event)) #Reply_Tweet:354, Retweet:406, Tweet:100, Tweet_+_Image:4419
  print(table(test_data$Event)) #Reply_Tweet:1649, Retweet:1794, Tweet:188, Tweet_+_Image:254
  
  print_crf_format(training_data, "/Users/blahiri/cleartrail_osn/for_CRF/SET2/train_ct_CRF.data")
  print_crf_format(test_data, "/Users/blahiri/cleartrail_osn/for_CRF/SET2/test_ct_CRF.data")
}

print_crf_format <- function(input_data, filename)
{
  sink(filename)
  nrows <- nrow(input_data)
  curr_session_id <- input_data[1, session_id]
  for (i in 1:nrows)
  {
    if (input_data[i, session_id] != curr_session_id)
    {
      cat(". . . . . . end_of_session\n\n")
      curr_session_id <- input_data[i, session_id]
    }
    cat(paste(input_data[i, LocalTime], input_data[i, session_id], input_data[i, Tx], input_data[i, Rx], input_data[i, DomainName], input_data[i, Direction], input_data[i, Event], collapse = " "))
    cat("\n")
  }
  sink()
}

measure_precision_recall <- function()
{
  #Remove the blank lines after end of each session so that fread() does not halt
  system("sed -i '.bak' '/^[[:space:]]*$/d' /Users/blahiri/cleartrail_osn/for_CRF/SET2/predicted_labels_ct.data")
  filename <- "~/cleartrail_osn/for_CRF/SET2/predicted_labels_ct.data"
  crf_outcome <- fread(filename, header = FALSE, sep = "\t", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "numeric", "numeric", "numeric", "character", "character", "character", "character"),
                    data.table = TRUE)
  setnames(crf_outcome, names(crf_outcome), c("LocalTime", "session_id", "Tx", "Rx", "DomainName", "Direction", "Event", "predicted_event"))
  prec_recall <- table(crf_outcome[, Event], crf_outcome[, predicted_event], dnn = list('actual', 'predicted'))
  
  #Measure overall accuracy
  setkey(crf_outcome, Event, predicted_event)
  cat(paste("Overall accuracy = ", nrow(crf_outcome[(Event == predicted_event),])/nrow(crf_outcome), "\n\n", sep = "")) #0.4604
  
  print(prec_recall)
  
  #Compute the micro-average recall values of the classes
  
  dt_prec_recall <- as.data.table(prec_recall)
  setkey(dt_prec_recall, actual)
  row_totals <- dt_prec_recall[, list(row_total = sum(N)), by = actual]
  setkey(row_totals, actual)
  for_recall <- dt_prec_recall[row_totals, nomatch = 0]
  setkey(for_recall, actual, predicted)
  for_recall <- for_recall[(actual == predicted),]
  for_recall[, recall := N/row_total]
  #for_recall <- for_recall[, .SD, .SDcols = c("actual", "recall")]
  setnames(for_recall, "actual", "Event")
  print(for_recall) #Reply Tweet and Retweet have recall values 0 and 0.99 respectively. Most packets simply get mapped to Retweet.
  cat("\n")
  
  #Compute the micro-average precision values of the classes
  
  setkey(dt_prec_recall, predicted)
  column_totals <- dt_prec_recall[, list(column_total = sum(N)), by = predicted]
  setkey(column_totals, predicted)
  for_precision <- dt_prec_recall[column_totals, nomatch = 0]
  setkey(for_precision, actual, predicted)
  for_precision <- for_precision[(actual == predicted),]
  for_precision[, precision := N/column_total]
  #for_precision <- for_precision[, .SD, .SDcols = c("predicted", "precision")] 
  print(for_precision) #Reply Tweet and Retweet have precision values 0 and 0.4655489 respectively
}
