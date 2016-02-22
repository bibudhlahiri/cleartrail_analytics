library(data.table)
library(rpart)

label_packets <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/RevisedPacketData_DevQA_TestCase1.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
                    
  #Get the event corresponding to each transaction
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/FP_Twitter_17_Feb.csv"
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
  
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states.csv"
  
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
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states.csv"
  hidden_and_vis_states <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric", 
                                   "character", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  hidden_and_vis_states$Event <- as.factor(hidden_and_vis_states$Event)
                   
  train = sample(1:nrow(hidden_and_vis_states), 0.5*nrow(hidden_and_vis_states))
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
  cat(paste("Overall accuracy = ", nrow(test_data[(Event == predicted_event),])/nrow(test_data), "\n\n", sep = "")) #0.43143
  model
}
