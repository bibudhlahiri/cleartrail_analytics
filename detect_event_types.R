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
  
  #Keep only one between Tx and Rx as one of them is always 0
  revised_packets[, pkt_bytes := ifelse(Tx > 0, Tx, Rx)]
  
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
  
  #filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/training_data.csv"
  #write.table(training_data, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  
  training_data <- create_bs_by_over_and_undersampling(training_data)
  
  test_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv", 
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv",
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv")
  levels(test_data$DomainName) <- levels(training_data$DomainName)
  levels(test_data$Direction) <- levels(training_data$Direction)
  levels(test_data$majority_domain) <- levels(training_data$majority_domain)
  
  cat(paste("Size of training data = ", nrow(training_data), ", size of test data = ", nrow(test_data), "\n", sep = ""))
  
  print(table(training_data$Event)) 
  print(table(test_data$Event))
  
  cols <- c("LocalTime", "session_id")
  n_features <- ncol(training_data) - length(cols) - 1
  tune.out <- tune.randomForest(Event ~ ., data = training_data[, .SD, .SDcols = -cols], ntree = c(500, 1000), nodesize = seq(10, 30, 10), mtry = seq(floor(sqrt(n_features)), n_features, 2))
  print(tune.out)
  bestmod <- tune.out$best.model
  
  #bestmod <- randomForest(Event ~ ., data = training_data[, .SD, .SDcols = -cols], mtry = 9)
  
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
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = "")) #0.583388: seems like we need better features: 
  #currently none of the features look very strong as the maximum (normalized) variable importance is 11.2%
  measure_precision_recall(prec_recall)
  
  #cols <- c("LocalTime", "session_id", "Event")
  #tuneRF(training_data[, .SD, .SDcols = -cols], training_data$Event, stepFactor=1.5)
  
  bestmod
}

classify_packets_deep_learning <- function()
{
  library(h2o)
  localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, Xmx = '2g')
                    
  training_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv", 
                                                          "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv",
                                                          "/Users/blahiri/cleartrail_osn/SET3/TC1/hidden_and_vis_states_23_Feb_2016_Set_I.csv")
                                                          
  #Merge the minor categories of events in training data into one
  setkey(training_data, Event)
  training_data[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down")), Event := "Other"]
  training_data$Event <- droplevels(training_data$Event)
  
  #training_data <- create_bs_by_over_and_undersampling(training_data)
  
  test_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv", 
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv",
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv")
  levels(test_data$DomainName) <- levels(training_data$DomainName)
  levels(test_data$Direction) <- levels(training_data$Direction)
  levels(test_data$majority_domain) <- levels(training_data$majority_domain)
  
  cat(paste("Size of training data = ", nrow(training_data), ", size of test data = ", nrow(test_data), "\n", sep = ""))
  
  print(table(training_data$Event)) 
  print(table(test_data$Event))
  
  cols <- c("LocalTime", "session_id", "Event")
  features <- names(training_data) 
  features <- features[!(features %in% cols)]
  
  training_data_h2o <- as.h2o(training_data, destination_frame = 'training_data')
  model <- h2o.deeplearning(x = features,  # column numbers for predictors
                            y = "Event",   # column number for label
                            training_frame = training_data_h2o, # data in H2O format
                            activation = "TanhWithDropout", # or 'Tanh'
                            input_dropout_ratio = 0.2, # % of inputs dropout
                            hidden_dropout_ratios = c(0.5,0.5,0.5), # % for nodes dropout
                            balance_classes = TRUE, 
                            hidden = c(50,50,50), # three layers of 50 nodes
                            epochs = 100) # max. no. of epochs

  test_data_h2o <- as.h2o(test_data, destination_frame = 'test_data')                       
  h2o_yhat_test <- h2o.predict(model, test_data_h2o, type = "class")
  df_yhat_test <- as.data.frame(h2o_yhat_test)
  #h2o.shutdown()
  
  test_data[, predicted_event := as.character(df_yhat_test$predict)]
  
  prec_recall <- table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, Event, predicted_event)
  accuracy <- nrow(test_data[(Event == predicted_event),])/nrow(test_data)
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = "")) #0.583388: seems like we need better features: 
  #currently none of the features look very strong as the maximum (normalized) variable importance is 11.2%
  measure_precision_recall(prec_recall)
}

measure_precision_recall <- function(prec_recall)
{
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
  #for_precision <- for_precision[, .SD, .SDcols = c("predicted", "precision")] 
  print(for_precision)
}

