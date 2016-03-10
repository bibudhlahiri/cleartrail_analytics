library(data.table)
library(rpart)
library(e1071)
library(randomForest)

lookup_event <- function(LocalTime, events)
{
  setkey(events, StartTime, EndTime) #the following line gives wrong result without setkey
  matching_row <- events[((LocalTime >= as.character(StartTime)) & (LocalTime <= as.character(EndTime))),]
  matching_row[, Event]
}
 
#Sample balancing for arbitrary number of classes

create_bs_by_over_and_undersampling <- function(df)
{
  n_df <- nrow(df)
  classes <- unique(df$Event)
  n_classes <- length(classes)
  size_each_part <- round(n_df/n_classes)
  cat(paste("n_classes = ", n_classes, ", size_each_part = ", size_each_part, "\n", sep = ""))
  bal_df <- data.table()
  setkey(df, Event)
  
  for (i in 1:n_classes)
  {
     this_set <- df[(Event == classes[i]),]
     n_this_set <- nrow(this_set)
     if (n_this_set >= size_each_part)
     {
       #undersample
       cat(paste("classes[i] = ", classes[i], ", n_this_set = ", n_this_set, ", undersample\n", sep = ""))
       sample_ind <- sample(1:n_this_set, size_each_part, replace = FALSE)
       sample_from_this_set <- this_set[sample_ind, ]
       bal_df <- rbindlist(list(bal_df, sample_from_this_set))
     }
     else
     {
       
       rep_times <- size_each_part%/%n_this_set
       cat(paste("classes[i] = ", classes[i], ", n_this_set = ", n_this_set, ", rep_times = ", rep_times, ", oversample\n", sep = ""))
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

prepare_data_for_detecting_event_types <- function(revised_pkt_data_file, events_file, hidden_and_vis_states_file)
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


classify_packets_random_forest <- function()
{
  training_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv", 
                                                          "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv",
                                                          "/Users/blahiri/cleartrail_osn/SET3/TC1/hidden_and_vis_states_23_Feb_2016_Set_I.csv")
                                                          
  #Merge the minor categories of events in training data into one
  setkey(training_data, Event)
  training_data[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down")), Event := "Other"]
  training_data$Event <- droplevels(training_data$Event)
  
  training_data <- create_bs_by_over_and_undersampling(training_data)
  
  test_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv", 
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv",
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv")
  levels(test_data$DomainName) <- levels(training_data$DomainName)
  levels(test_data$Direction) <- levels(training_data$Direction)
  levels(test_data$Event) <- levels(training_data$Event)
  levels(test_data$majority_domain) <- levels(training_data$majority_domain)
  
  cat(paste("Size of training data = ", nrow(training_data), ", size of test data = ", nrow(test_data), "\n", sep = ""))
  
  print(table(training_data$Event)) 
  print(table(test_data$Event))
  
  cols <- c("LocalTime", "session_id")
  
  #tune.out <- tune.randomForest(Event ~ ., data = training_data[, .SD, .SDcols = -cols], ntree = c(500, 1000), nodesize = seq(10, 30, 10))
  #print(summary(tune.out))
  #bestmod <- tune.out$best.model
  
  bestmod <- randomForest(Event ~ ., data = training_data[, .SD, .SDcols = -cols])
  
  impRF <- bestmod$importance
  impRF <- impRF[, "MeanDecreaseGini"]
  imp <- impRF/sum(impRF)
  print(sort(imp, decreasing = TRUE))
   
  test_data[, predicted_event := as.character(predict(bestmod, newdata = test_data, type = "class"))]
  
  prec_recall <- table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, Event, predicted_event)
  accuracy <- nrow(test_data[(Event == predicted_event),])/nrow(test_data)
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = ""))
  bestmod
}



