library(data.table)
library(rpart)
library(e1071)
library(randomForest)
library(klaR)

lookup_event <- function(LocalTime, events)
{
  setkey(events, StartTime, EndTime) #the following line gives wrong result without setkey
  matching_row <- events[((LocalTime >= as.character(StartTime)) & (LocalTime <= as.character(EndTime))),]
  matching_row[, Event]
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


prepare_training_data <- function(revised_pkts_trg, events_trg, hidden_and_vis_states_trg)
{
  training_data <- prepare_data_for_detecting_event_types(revised_pkts_trg, events_trg, hidden_and_vis_states_trg)
  cat("Original distribution of training data\n")
  print(table(training_data$Event))  
                                                         
  #Merge the minor categories of events in training data into one
  setkey(training_data, Event)
  training_data[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down")), Event := "Other"]
  training_data$Event <- droplevels(training_data$Event)
  
  cat("\nDistribution of training data after merging the minor categories\n")
  print(table(training_data$Event))  
  
  #Merge Reply_Tweet_Text_and_Image and Tweet+Image
  training_data[(Event == "Reply_Tweet_Text_and_Image"), Event := "Tweet+Image"]
  training_data$Event <- droplevels(training_data$Event)
  
  cat("\nDistribution of training data after merging Reply_Tweet_Text_and_Image and Tweet+Image\n")
  print(table(training_data$Event))  
  
  training_data <- training_data[!(Event == "Uploading_Image"),] 
  
  training_data[(Event %in% c("Reply_Tweet_Text_Only", "ReTweet", "Tweet_Only", "Tweet", "Reply_Tweet")), Event := "Tweet_Text_Only"]
  training_data$Event <- droplevels(training_data$Event)
  
  cat("\nDistribution of training data after deleting Uploading_Image, and merging Reply_Tweet_Text_Only, ReTweet and Tweet_Only\n")
  print(table(training_data$Event))
  
  training_data <- remove_noise_ae(training_data)
  training_data[, reconstruction_error := NULL]
  cat(paste("\n", "Size of training data after noise removal = ", nrow(training_data), "\n", sep = ""))
  
  training_data
}

prepare_test_data <- function(revised_pkts_test, events_test, hidden_and_vis_states_test)
{
  test_data <- prepare_data_for_detecting_event_types(revised_pkts_test, events_test, hidden_and_vis_states_test)
  
  cat("Original distribution of test data\n")
  print(table(test_data$Event)) 
                                                   
  #Merge the minor categories of events in test data into one
  setkey(test_data, Event)
  test_data[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down", "Like")), Event := "Other"]
  test_data$Event <- droplevels(test_data$Event)
  
  #Merge Reply_Tweet_Text_and_Image and Tweet+Image
  test_data[(Event == "Reply_Tweet_Text_and_Image"), Event := "Tweet+Image"]
  test_data$Event <- droplevels(test_data$Event)
  
  cat("\nDistribution of test data after merging Reply_Tweet_Text_and_Image and Tweet+Image\n")
  print(table(test_data$Event))  
  
  test_data[(Event %in% c("Reply_Tweet_Text_Only", "ReTweet", "Tweet_Only", "Tweet", "Reply_Tweet")), Event := "Tweet_Text_Only"]
  test_data$Event <- droplevels(test_data$Event)
  
  cat("\nDistribution of test data after merging Reply_Tweet_Text_Only, ReTweet and Tweet_Only\n")
  print(table(test_data$Event))
  
  cat(paste("Size of test data = ", nrow(test_data), "\n", sep = ""))
  
  test_data
}


classify_packets_naive_bayes <- function(revised_pkts_trg, events_trg, hidden_and_vis_states_trg, 
                                         revised_pkts_test, events_test, hidden_and_vis_states_test, 
                                         predicted_event_types, saved_model)
{
  training_data <- prepare_training_data(revised_pkts_trg, events_trg, hidden_and_vis_states_trg)
  test_data <- prepare_test_data(revised_pkts_test, events_test, hidden_and_vis_states_test)
  
  #Remove variables that are not suitable for modeling, including variables that are perfectly/highly correlated with other variables, e.g., frac_downstream_packets is perfectly correlated with
  #frac_upstream_packets.
  cols <- c("LocalTime", "session_id", "frac_downstream_packets", "frac_downstream_bytes")  
  bestmod <- NaiveBayes(Event ~ ., data = training_data[, .SD, .SDcols = -cols], usekernel = TRUE, fL = 1) #Naive Bayes with distributions for continuous variables found by KDE
  
  test_data[, predicted_event := as.character((predict(bestmod, newdata = test_data))$class)]
  
  prec_recall <- table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, Event, predicted_event)
  accuracy <- nrow(test_data[(Event == predicted_event),])/nrow(test_data)
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = "")) 
  measure_precision_recall(prec_recall)
  
  write.table(test_data, predicted_event_types, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  saveRDS(bestmod, saved_model)
}

retrain_only <- function(revised_pkts_trg, events_trg, hidden_and_vis_states_trg, saved_model)
{
  training_data <- prepare_training_data(revised_pkts_trg, events_trg, hidden_and_vis_states_trg)
  
  cols <- c("LocalTime", "session_id", "frac_downstream_packets", "frac_downstream_bytes")  
  bestmod <- NaiveBayes(Event ~ ., data = training_data[, .SD, .SDcols = -cols], usekernel = TRUE, fL = 1)
  
  saveRDS(bestmod, saved_model)
}

generate_labels_only <- function(revised_pkts_test, events_test, hidden_and_vis_states_test, 
                                 predicted_event_types, saved_model)
{
  bestmod <- readRDS(saved_model)
  test_data <- prepare_test_data(revised_pkts_test, events_test, hidden_and_vis_states_test)
  
  #Note that we are not going to measure precision/recall as we do not know the actual labels for the test data
  
  test_data[, predicted_event := as.character((predict(bestmod, newdata = test_data))$class)]
  write.table(test_data, predicted_event_types, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
}


remove_noise_ae <- function(training_data, k = 5, n_units_hidden_layer = 10)
{
  library(h2o)
  localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, max_mem_size = '2g')
  
  cols <- c("LocalTime", "session_id", "Event", "frac_downstream_packets", "frac_downstream_bytes"
            , "DomainName", "Direction", "majority_domain")
  features <- names(training_data) 
  features <- features[!(features %in% cols)]
  
  training_data_h2o <- as.h2o(training_data, destination_frame = 'training_data')

  #Train deep autoencoder learning model on training data, y ignored.
  #We have 18 numeric features in the data. We should keep the hidden layer undercomplete, i.e., number of units in the hidden layer should 
  #be less than the number of features in input data.
  anomaly_model <- h2o.deeplearning(x = features,
                                    training_frame = training_data_h2o, activation = "Tanh", autoencoder = TRUE,
                                    hidden = c(n_units_hidden_layer), l1 = 1e-4, epochs = 100)

  # Compute reconstruction error with the anomaly detection app (MSE between output layer and input layer)
  recon_error <- h2o.anomaly(anomaly_model, training_data_h2o)  
  
  #Take off the training data points whose reconstruction error are in the highest k-th percentile
  recon_error <- as.data.frame(recon_error)
  cutoff <- quantile(recon_error$Reconstruction.MSE, c(1 - k/100))
  
  #We are not taking the reconstructed values of X. We are only removing those rows from X for which the reconstruction error are in the highest k-th percentile.
  training_data[, reconstruction_error := recon_error$Reconstruction.MSE]
  setkey(training_data, reconstruction_error)
  training_data[(reconstruction_error <= cutoff),]
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

