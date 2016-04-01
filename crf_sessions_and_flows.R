library(data.table)
library(rpart)
library(e1071)

label_packets <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
                    
  #Get the event corresponding to each transaction
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv"
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
  
  #The session and flow-related features come by aggregating session and flow-related data at each instant.
  
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
  
  #New features added on 02/24/2015
  hidden_and_vis_states[, total_bytes := upstream_bytes + downstream_bytes]
  hidden_and_vis_states[, frac_upstream_bytes := upstream_bytes/total_bytes]
  hidden_and_vis_states[, frac_downstream_bytes := downstream_bytes/total_bytes]
  hidden_and_vis_states[, avg_bytes_per_packet := total_bytes/n_packets]
  hidden_and_vis_states[, avg_upstream_bytes_per_packet := upstream_bytes/n_packets]
  hidden_and_vis_states[, avg_downstream_bytes_per_packet := downstream_bytes/n_packets]
  
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv"
  
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
  names(tt[which.max(tt)])
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


prepare_data_for_detecting_endtimes <- function(hidden_and_vis_states_file, events_file)
{
  hidden_and_vis_states <- fread(hidden_and_vis_states_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric", 
                                   "character", "numeric", "numeric", "numeric", "numeric", 
                                   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  
  #Take the timestamps from the event data that indicate ends of events. If they are found in hidden_and_vis_states, mark them; if not, mark the timestamp that comes closest. Break ties arbitrarily.
  
  events <- fread(events_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("character", "Date", "Date"),
                    data.table = TRUE)
  events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, MatchingPacketTimestamp := apply(events, 1, function(row) get_matching_timestamp(as.character(row["EndTime"]), hidden_and_vis_states))]
  
  #Take the matched timestamps and mark them in hidden_and_vis_states
  hidden_and_vis_states[(LocalTime %in% events$MatchingPacketTimestamp), end_of_event := TRUE]
  hidden_and_vis_states[(is.na(end_of_event)), end_of_event := FALSE]
  hidden_and_vis_states$end_of_event <- as.factor(hidden_and_vis_states$end_of_event)
  hidden_and_vis_states
}

#Decision tree. Check this: https://zyxo.wordpress.com/2011/07/04/how-to-use-the-settings-to-control-the-size-of-decision-trees/

detect_endtimes_rpart <- function(training_data, test_data, hidden_and_vis_states_with_eoe_file)
{
  #Because of the random split, if we encounter values of Event in test_data that were not encountered in training_data, then there will be a problem. Avoid that.
  test_data <- test_data[test_data$majority_domain %in% unique(training_data$majority_domain),]
  
  print(table(training_data$end_of_event)) 
  print(table(test_data$end_of_event))
  
  #Note: Event cannot be kept as a predictor as it would not be available in real data
  tune.out <- tune.rpart(end_of_event ~ n_packets + n_sessions + n_flows + n_downstream_packets + n_upstream_packets + 
                         factor(majority_domain) + upstream_bytes + downstream_bytes + frac_upstream_packets + frac_downstream_packets + 
                         avg_packets_per_session + avg_packets_per_flow + 
                         total_bytes + frac_upstream_bytes + frac_downstream_bytes + avg_bytes_per_packet + avg_upstream_bytes_per_packet + avg_downstream_bytes_per_packet, 
                         data = training_data, minsplit = c(5, 10, 15, 20), maxdepth = seq(5, 30, 5))
  print(summary(tune.out))
  plot(tune.out)
  
  bestmod <- tune.out$best.model
  test_data[, predicted_end_of_event := as.character(predict(bestmod, newdata = test_data, type = "class"))]
  
  probabilities <- predict(bestmod, newdata = test_data, type = "prob")
  print(data.frame(timestamp = test_data$LocalTime, n_upstream_packets = test_data$n_upstream_packets,
                   avg_downstream_bytes_per_packet = test_data$avg_downstream_bytes_per_packet,
                   avg_bytes_per_packet = test_data$avg_bytes_per_packet,
                   total_bytes = test_data$total_bytes,
                   avg_packets_per_session = test_data$avg_packets_per_session,
                   prob_end_of_event = probabilities[, "TRUE"]))
  
  prec_recall <- table(test_data[, end_of_event], test_data[, predicted_end_of_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, end_of_event, predicted_end_of_event)
  cat(paste("Overall accuracy = ", nrow(test_data[(end_of_event == predicted_end_of_event),])/nrow(test_data), 
            ", recall = ", prec_recall[2,2]/sum(prec_recall[2,]), 
            ", precision = ", prec_recall[2,2]/sum(prec_recall[,2]), "\n\n", sep = "")) #0.790697674418
  #The end_of_events can be identified with recall of 0.875 and precision of 0.2916667
  
  #Write the test data back with the predicted values of end_of_event. No need to write the data points that come from training data because predicted_end_of_event will be NA for them.
  test_data <- test_data[, .SD, .SDcols = c("LocalTime", "Event", "end_of_event", "predicted_end_of_event")]
  write.table(test_data, hidden_and_vis_states_with_eoe_file, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)

  tune.out
}

#SVM

detect_endtimes_svm <- function(training_data, test_data, hidden_and_vis_states_with_eoe_file)
{
  #Because of the random split, if we encounter values of Event in test_data that were not encountered in training_data, then there will be a problem. Avoid that.
  test_data <- test_data[test_data$majority_domain %in% unique(training_data$majority_domain),]
  
  print(table(training_data$end_of_event)) 
  print(table(test_data$end_of_event))
  
  #Note: Event cannot be kept as a predictor as it would not be available in real data
  cols <- c("end_of_event", "LocalTime", "Event", "majority_domain")            
  tune.out = tune.svm(training_data[, .SD, .SDcols = -cols], training_data$end_of_event, kernel = "radial", 
                      cost = c(0.001, 0.01, 0.1, 1, 10, 100, 1000), gamma = c(0.125, 0.25, 0.5, 1, 2, 3, 4, 5))
  bestmod <- tune.out$best.model
  
  test_data_end_of_event <- test_data[, end_of_event] #Retain this before dropping as we will need it for contingency table
  test_data <- test_data[, .SD, .SDcols = -cols]
  test_data[, predicted_end_of_event := as.character(predict(bestmod, newdata = test_data))]
  
  prec_recall <- table(test_data_end_of_event, test_data[, predicted_end_of_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  #Restore back end_of_event in test_data
  test_data[, end_of_event := test_data_end_of_event]
  setkey(test_data, end_of_event, predicted_end_of_event)
  cat(paste("Overall accuracy = ", nrow(test_data[(end_of_event == predicted_end_of_event),])/nrow(test_data), 
            ", recall = ", prec_recall[2,2]/sum(prec_recall[2,]), 
            ", precision = ", prec_recall[2,2]/sum(prec_recall[,2]), "\n\n", sep = "")) #0.8725868
  #The end_of_events can be identified with recall of 0.05882 and precision of 0.66666
  
  tune.out
}


#Take the timestamp-aggregated data and try to detect which ones among the timestamps indicate end of events.
detect_endtimes_from_single_file <- function()
{
  hidden_and_vis_states <- prepare_data_for_detecting_endtimes("/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states_DevQA_TestCase1.csv",
                                                               "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/FP_Twitter_17_Feb.csv")
  #Apply classification model on end_of_event
  train = sample(1:nrow(hidden_and_vis_states), 0.7*nrow(hidden_and_vis_states))
  test = (-train)
  cat(paste("Size of training data = ", length(train), ", size of test data = ", (nrow(hidden_and_vis_states) - length(train)), "\n", sep = ""))
  
  training_data <- hidden_and_vis_states[train, ]
  training_data <- create_bs_by_over_and_undersampling(training_data)
  test_data <- hidden_and_vis_states[test, ]
  
  model <- detect_endtimes(training_data, test_data, "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states_with_eoe_DevQA_TestCase1.csv")
}


detect_endtimes_from_separate_files <- function()
{
  training_data <- prepare_data_for_detecting_endtimes("/Users/blahiri/cleartrail_osn/SET3/TC1/hidden_and_vis_states_23_Feb_2016_Set_I.csv",
                                                       "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv")
  training_data <- create_bs_by_over_and_undersampling(training_data)
  test_data <- prepare_data_for_detecting_endtimes("/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv", 
                                                 "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv")
  #Overall accuracy = 0.8378, recall = 0.147, precision = 0.278
  tune.out <- detect_endtimes_rpart(training_data, test_data, "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_with_eoe_23_Feb_2016_Set_II.csv")
}


get_matching_timestamp <- function(end_time, hidden_and_vis_states)
{
  hidden_and_vis_states$difference <- abs(as.numeric(difftime(strptime(hidden_and_vis_states$LocalTime, "%H:%M:%S"), strptime(end_time, "%H:%M:%S"), units = "secs")))
  setkey(hidden_and_vis_states, difference)
  hidden_and_vis_states <- hidden_and_vis_states[order(difference),]
  hidden_and_vis_states[1, LocalTime]
}
 
#Sample balancing for arbitrary number of classes
create_bs_by_over_and_undersampling <- function(df)
{
  n_df <- nrow(df)
  classes <- unique(df$Event)
  n_classes <- length(classes)
  size_each_part <- round(n_df/n_classes)
  bal_df <- data.table()
  setkey(df, Event)
  
  for (i in 1:n_classes)
  {
     this_set <- df[(Event == classes[i]),]
     n_this_set <- nrow(this_set)
     if (n_this_set >= size_each_part)
     {
       #undersample
       sample_ind <- sample(1:n_this_set, size_each_part, replace = FALSE)
       sample_from_this_set <- this_set[sample_ind, ]
       bal_df <- rbindlist(list(bal_df, sample_from_this_set))
     }
     else
     {
       rep_times <- size_each_part%/%n_this_set
       oversampled_set <- this_set
       if (rep_times >= 2)
       {
         for (i in 1:(rep_times - 1))
         {
           oversampled_set <- rbindlist(list(oversampled_set, this_set))
         }
       }
       rem_sample_id <- sample(1:n_this_set, size_each_part%%n_this_set, replace = FALSE)
       rem_sample <- this_set[rem_sample_id, ]
       oversampled_set <- rbindlist(list(oversampled_set, rem_sample))
       bal_df <- rbindlist(list(bal_df, oversampled_set))
     }
  }
  bal_df
}

prepare_packet_data_for_CRF <- function(revised_pkt_data_file, events_file, hidden_and_vis_states_file)
{
  revised_packets <- fread(revised_pkt_data_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
  setkey(revised_packets, session_id, LocalTime)
  revised_packets <- revised_packets[order(session_id, LocalTime),]
  
  #Get the event corresponding to each packet
  events <- fread(events_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
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
  
  #Join the timestamp-related aggregated features with the timestamps in this data to introduce additional features.
  hidden_and_vis_states <- fread(hidden_and_vis_states_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "numeric", "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric", 
                                   "character", "numeric", "numeric", "numeric", "numeric", 
                                   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  hidden_and_vis_states[, Event := NULL]
  setkey(hidden_and_vis_states, LocalTime)
  setkey(revised_packets, LocalTime)
  revised_packets <- revised_packets[hidden_and_vis_states, nomatch = 0]
    
  #Change the key back to session_id and LocalTime so that all packets for a session are printed together
  setkey(revised_packets, session_id, LocalTime)
  
  #Keep only one between Tx and Rx as one of them is always 0
  revised_packets[, pkt_bytes := ifelse(Tx > 0, Tx, Rx)]
  
  #Drop columns that are not needed for modeling. We need to retain the timestamp in the test data so that we can group by it later to get the majority vote among labeled events for a timestamp.
   
  revised_packets[ ,`:=`(Timestamp = NULL, SourcePort = NULL, DestPort = NULL, SourceIP = NULL, DestIP = NULL, ServerIP = NULL, ClientIP = NULL, 
                        ServerPort = NULL, ClientPort = NULL, flow_id = NULL, Tx = NULL, Rx = NULL)]
  revised_packets[, Event := apply(revised_packets, 1, function(row) gsub(" ", "_", as.character(row["Event"])))]
  
  revised_packets$DomainName <- as.factor(revised_packets$DomainName)
  revised_packets$Direction <- as.factor(revised_packets$Direction)
  revised_packets$Event <- as.factor(revised_packets$Event)
  revised_packets$majority_domain <- as.factor(revised_packets$majority_domain)
  
  revised_packets
}

#We take the packets in sessions, and look up for the events corresponding to the packets through timestamps. We group the packets in sessions as if packets are words/tokens and
#sessions are sentences. Then, we apply CRF on the training data and fit the model on test data. We should split all the available sessions into two halves: training and testing, but should not 
#split the packets in a single session. 
#To train with CRF++, from ~/cleartrail_analytics, run the following command: ~/crf++/CRF++-0.58/crf_learn crf_template_ct ~/cleartrail_osn/for_CRF/SET2/train_ct_CRF.data model_ct
#To test with CFR++, run ~/crf++/CRF++-0.58/crf_test -m model_ct ~/cleartrail_osn/for_CRF/SET2/test_ct_CRF.data > ~/cleartrail_osn/for_CRF/SET2/predicted_labels_ct.data
#With same data and feature set, computation results from CRF remain same even if run multiple times: there is no random factor.

prepare_packet_data_for_CRF_from_single_file <- function()
{
  revised_packets <- prepare_packet_data_for_CRF("/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/RevisedPacketData_DevQA_TestCase1.csv", 
                                                 "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/FP_Twitter_17_Feb.csv",
                                                 "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states_DevQA_TestCase1.csv")
  
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
  revised_packets
}

#To train with CRF++, from ~/cleartrail_analytics, run the following command: ~/crf++/CRF++-0.58/crf_learn -t crf_template_ct ~/cleartrail_osn/for_CRF/SET3/TC1and2/train_ct_CRF.data model_ct
#To test with CFR++, run ~/crf++/CRF++-0.58/crf_test -m model_ct ~/cleartrail_osn/for_CRF/SET3/TC1and2/test_ct_CRF.data > ~/cleartrail_osn/for_CRF/SET3/TC1and2/predicted_labels_ct.data

prepare_packet_data_for_CRF_from_separate_files <- function()
{
  training_data <- prepare_packet_data_for_CRF("/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv", 
                                                 "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv",
                                                 "/Users/blahiri/cleartrail_osn/SET3/TC1/hidden_and_vis_states_23_Feb_2016_Set_I.csv")
                                                 
  #Merge the minor categories of events in training data into one
  #setkey(training_data, Event)
  #training_data[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down")), Event := "Other"]
  #training_data$Event <- droplevels(training_data$Event)
  
  #Sample balancing should NOT be done in a CRF since it will geopardize the sequential nature of the training data.
  
  test_data <- prepare_packet_data_for_CRF("/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv", 
                                                 "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv",
                                                 "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv")
  
  cat(paste("Size of training data = ", nrow(training_data), ", size of test data = ", nrow(test_data), "\n", sep = ""))
  
  print(table(training_data$Event)) 
  print(table(test_data$Event)) 
  
  print_crf_format(training_data, "/Users/blahiri/cleartrail_osn/for_CRF/SET3/TC1and2/train_ct_CRF.data")
  print_crf_format(test_data, "/Users/blahiri/cleartrail_osn/for_CRF/SET3/TC1and2/test_ct_CRF.data")
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
      cat(paste(paste(rep(".", length(names(input_data)) - 1), collapse = " "), " end_of_session\n\n", sep = ""))
      curr_session_id <- input_data[i, session_id]
    }
    cat(paste(input_data[i, LocalTime], input_data[i, session_id], input_data[i, pkt_bytes], input_data[i, DomainName], input_data[i, Direction], 
              input_data[i, n_packets], input_data[i, n_sessions], input_data[i, n_flows], input_data[i, n_downstream_packets], input_data[i, n_upstream_packets], input_data[i, majority_domain], 
              input_data[i, upstream_bytes], input_data[i, downstream_bytes], input_data[i, frac_upstream_packets], input_data[i, frac_downstream_packets], input_data[i, avg_packets_per_session],
              input_data[i, avg_packets_per_flow], 
              input_data[i, total_bytes], input_data[i, frac_upstream_bytes], input_data[i, frac_downstream_bytes], input_data[i, avg_bytes_per_packet], 
              input_data[i, avg_upstream_bytes_per_packet], input_data[i, avg_downstream_bytes_per_packet],
              input_data[i, Event], collapse = " "))
    cat("\n")
  }
  sink()
}

#Current problem: too many of Reply_Tweet, Tweet and Tweet_+_Image are being mapped to Retweet, making the recall of all of these categories low. The reason for this may be the presence 
#of too many Retweet (1794/3885 or 46%) in the test data, but same applies for Reply_Tweet (1649/3885 or 42%), too.

measure_precision_recall <- function()
{
  #Remove the blank lines after end of each session so that fread() does not halt
  system("sed -i '.bak' '/^[[:space:]]*$/d' /Users/blahiri/cleartrail_osn/for_CRF/SET3/TC1and2/predicted_labels_ct.data")
  
  filename <- "~/cleartrail_osn/for_CRF/SET3/TC1and2/predicted_labels_ct.data"
  crf_outcome <- fread(filename, header = FALSE, sep = "\t", stringsAsFactors = FALSE, showProgress = TRUE, 
                       colClasses = c("Date", "numeric", "numeric", "character", 
                                      "character", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "character", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "character", 
                                      "character"),
                       data.table = TRUE)
  setnames(crf_outcome, names(crf_outcome), c("LocalTime", "session_id", "pkt_bytes", "DomainName", 
                                              "Direction", "n_packets", "n_sessions", "n_flows", "n_downstream_packets", 
                                              "n_upstream_packets", "majority_domain", "upstream_bytes", "downstream_bytes", "frac_upstream_packets", 
                                              "frac_downstream_packets", "avg_packets_per_session", "avg_packets_per_flow", "total_bytes", "frac_upstream_bytes", 
                                              "frac_downstream_bytes", "avg_bytes_per_packet", "avg_upstream_bytes_per_packet", "avg_downstream_bytes_per_packet", "Event", 
                                              "predicted_event"))
  prec_recall <- table(crf_outcome[, Event], crf_outcome[, predicted_event], dnn = list('actual', 'predicted'))
  
  #Measure overall accuracy
  setkey(crf_outcome, Event, predicted_event)
  cat(paste("Overall accuracy = ", nrow(crf_outcome[(Event == predicted_event),])/nrow(crf_outcome), "\n\n", sep = "")) #0.72613
  
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
  print(for_recall) #Reply Tweet, Retweet, Tweet and Tweet_+_Image have recall values 0.5585203, 1.0, 0.9893617 and 0.5314961 respectively.
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
  print(for_precision) #Reply Tweet, Retweet, Tweet and Tweet_+_Image have precision values 1.0, 0.6787741, 1.0 and 1.0 respectively. 
  #Tweet_+_Image reached this precision from 0.95 as we included the majority_domain for the previous and next packets also among the features for CRF.
}

#Using data.frame
temporal_aggregation <- function()
{
  filename <- "~/cleartrail_osn/for_CRF/SET3/TC1and2/predicted_labels_ct.data"
  crf_outcome <- fread(filename, header = FALSE, sep = "\t", stringsAsFactors = FALSE, showProgress = TRUE, 
                       colClasses = c("Date", "numeric", "numeric", "numeric", "character", 
                                      "character", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "character", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "character", 
                                      "character"),
                       data.table = TRUE)
  setnames(crf_outcome, names(crf_outcome), c("LocalTime", "session_id", "Tx", "Rx", "DomainName", 
                                              "Direction", "n_packets", "n_sessions", "n_flows", "n_downstream_packets", 
                                              "n_upstream_packets", "majority_domain", "upstream_bytes", "downstream_bytes", "frac_upstream_packets", 
                                              "frac_downstream_packets", "avg_packets_per_session", "avg_packets_per_flow", "total_bytes", "frac_upstream_bytes", 
                                              "frac_downstream_bytes", "avg_bytes_per_packet", "avg_upstream_bytes_per_packet", "avg_downstream_bytes_per_packet", "Event", 
                                              "predicted_event"))
  crf_outcome <- crf_outcome[, .SD, .SDcols = c("LocalTime", "Event", "predicted_event")]
  setkey(crf_outcome, LocalTime)
  #The actual event will be only one, hence unique() works; but predicted events may have multiple distinct values, so take the majority.
  temporal_aggregate <- crf_outcome[, list(actual_event = unique(Event), majority_predicted_event = get_majority_predicted_event(.SD)), by = LocalTime,
                                           .SDcols=c("Event", "predicted_event")]
  cat(paste("Accuracy based on temporal_aggregate is ", nrow(temporal_aggregate[(actual_event == majority_predicted_event),])/nrow(temporal_aggregate), "\n", sep = "")) #0.564102
  
  #Aggregate start and end times based on temporal_aggregate
   
  n_temporal_aggregate <- nrow(temporal_aggregate)
  start_end_event <- data.table(data.frame(Event = character(n_temporal_aggregate), StartTime = character(n_temporal_aggregate), EndTime = character(n_temporal_aggregate)))
  
  start_end_event[1, Event := temporal_aggregate[1, majority_predicted_event]]
  start_end_event[1, StartTime := temporal_aggregate[1, LocalTime]]
  current_event <- temporal_aggregate[1, majority_predicted_event]
  curr_row_in_start_end_event <- 1
  
  for (i in 2:n_temporal_aggregate)
  {
     if (temporal_aggregate[i, majority_predicted_event] != current_event)
     {
       #Start of a new event has been encountered. End the previous event.
        start_end_event[curr_row_in_start_end_event, EndTime := temporal_aggregate[i-1, LocalTime]]
        curr_row_in_start_end_event <- curr_row_in_start_end_event + 1
        start_end_event[curr_row_in_start_end_event, Event := temporal_aggregate[i, majority_predicted_event]]
        start_end_event[curr_row_in_start_end_event, StartTime := temporal_aggregate[i, LocalTime]]
        current_event <- temporal_aggregate[i, majority_predicted_event]
     }
  }
  start_end_event[curr_row_in_start_end_event, EndTime := temporal_aggregate[n_temporal_aggregate, LocalTime]]
  
  #Remove the blank rows from start_end_event
  start_end_event <- data.frame(start_end_event)
  start_end_event <- start_end_event[!((start_end_event$Event == "") | (start_end_event$Event == "end_of_session")),]
  rownames(start_end_event) <- NULL #Adjust row numbers after deletion
  
  #Default type was factor for the columns and that was creating problem in updating
  start_end_event$Event <- as.character(start_end_event$Event)
  start_end_event$StartTime <- as.character(start_end_event$StartTime)
  start_end_event$EndTime <- as.character(start_end_event$EndTime)
  
  #Get the predicted ends of events and merge them with start_end_event.
  #filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/hidden_and_vis_states_with_eoe_DevQA_TestCase1.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_with_eoe_23_Feb_2016_Set_II.csv"
  hidden_and_vis_states <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "character", "character", "character"),
                    data.table = TRUE)
  hidden_and_vis_states[, .SD, .SDcols = c("LocalTime", "predicted_end_of_event")]
  setkey(hidden_and_vis_states, predicted_end_of_event)
  predicted_end_times <- hidden_and_vis_states[(predicted_end_of_event == TRUE),]
  n_predicted_end_times <- nrow(predicted_end_times)
  
  #start_end_event <- data.frame(start_end_event)
  
  for (i in 1:n_predicted_end_times)
  {
     end_time_to_place <- predicted_end_times[i, LocalTime]
          
     #If there is an end time already in start_end_event which matches with end_time_to_place, then no need to do anything more.
     row_with_matching_end_time <- subset(start_end_event, (EndTime == end_time_to_place))
     
     #Restore the order by StartTime
     start_end_event <- start_end_event[with(start_end_event, order(StartTime)), ]
     
     if (nrow(row_with_matching_end_time) == 0)
     {
       #Check if there is a row in start_end_event where end_time_to_place falls in that interval
       n_start_end_event <- nrow(start_end_event)
       for (j in 1:n_start_end_event)
       {
         if ((as.character(start_end_event[j, "StartTime"]) <= end_time_to_place) & (as.character(start_end_event[j, "EndTime"]) > end_time_to_place))
         {
           break #only one match needed for j
         } #end if 
       } #end for (j in 1:n_start_end_event)
             
       #Before inserting end_time_to_place, check if it is less than the EndTime of the last Event.
       if (end_time_to_place < as.character(start_end_event[n_start_end_event, "EndTime"]))
       {
         #Add a junk row at end before we start pushing down.
         start_end_event <- rbind(start_end_event, data.frame(Event = "", StartTime = "", EndTime = ""))
         rownames(start_end_event) <- NULL #Adjust row numbers after addition
         
         #Push down everything from (j+1)-th row to end of start_end_event as we are going to split the j-th event into two events. Run the loop starting from end.
       
         for (k in n_start_end_event:(j+1))
         {
           start_end_event[k + 1, "Event"] <- start_end_event[k, "Event"]
           start_end_event[k + 1, "StartTime"] <- start_end_event[k, "StartTime"]
           start_end_event[k + 1, "EndTime"] <- start_end_event[k, "EndTime"]
         }
         start_end_event[j + 1, "Event"] <- start_end_event[j, "Event"]
         start_end_event[j + 1, "StartTime"] <- strftime(strptime(as.character(end_time_to_place), "%H:%M:%S") + 1, "%H:%M:%S")
         start_end_event[j + 1, "EndTime"] <- start_end_event[j, "EndTime"]
         start_end_event[j, "EndTime"] <- end_time_to_place
       }
     } #end if (nrow(row_with_matching_end_time) == 0)
  } #end for (i in 1:n_predicted_end_times)
  print(start_end_event)
}

get_majority_predicted_event <- function(dt)
{
  tt <- table(dt$predicted_event)
  names(tt[which.max(tt)])
}
