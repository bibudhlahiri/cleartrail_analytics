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


prepare_training_and_test_data <- function()
{
  training_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv", 
                                                          "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv",
                                                          "/Users/blahiri/cleartrail_osn/SET3/TC1/hidden_and_vis_states_23_Feb_2016_Set_I.csv")
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
  
  training_data[(Event %in% c("Reply_Tweet_Text_Only", "ReTweet", "Tweet_Only")), Event := "Tweet_Text_Only"]
  training_data$Event <- droplevels(training_data$Event)
  
  cat("\nDistribution of training data after deleting Uploading_Image, and merging Reply_Tweet_Text_Only, ReTweet and Tweet_Only...writing to the file\n")
  print(table(training_data$Event))
  
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/training_data.csv"
  write.table(training_data, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  
  test_data <- prepare_data_for_detecting_event_types("/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv", 
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv",
                                                      "/Users/blahiri/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv")
  
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
  
  test_data[(Event %in% c("Reply_Tweet_Text_Only", "ReTweet", "Tweet_Only")), Event := "Tweet_Text_Only"]
  test_data$Event <- droplevels(test_data$Event)
  
  cat("\nDistribution of test data after merging Reply_Tweet_Text_Only, ReTweet and Tweet_Only\n")
  print(table(test_data$Event))
  
  levels(test_data$DomainName) <- levels(training_data$DomainName)
  levels(test_data$Direction) <- levels(training_data$Direction)
  levels(test_data$majority_domain) <- levels(training_data$majority_domain)
  
  cat(paste("Size of training data = ", nrow(training_data), ", size of test data = ", nrow(test_data), "\n", sep = ""))
  
  cat(paste("\n", "nrow(training_data) before noise removal = ", nrow(training_data), "\n", sep = ""))
  training_data <- remove_noise_ae(training_data)
  training_data[, reconstruction_error := NULL]
  cat(paste("\n", "nrow(training_data) after noise removal = ", nrow(training_data), "\n", sep = ""))
  
  list(training_data = training_data, test_data = test_data)
}


classify_packets_random_forest <- function()
{
  ret_obj <- prepare_training_and_test_data()
  training_data <- ret_obj[["training_data"]]
  test_data <- ret_obj[["test_data"]]
  
  #Remove variables that are not suitable for modeling, including variables that are perfectly/highly correlated with other variables, e.g., frac_downstream_packets is perfectly correlated with
  #frac_upstream_packets.
  cols <- c("LocalTime", "session_id", "frac_downstream_packets", "frac_downstream_bytes")
  n_features <- ncol(training_data) - length(cols) - 1
  
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
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = "")) #0.583388: seems like we need better features: 
  #currently none of the features look very strong as the maximum (normalized) variable importance is 11.2%
  measure_precision_recall(prec_recall)
  
  bestmod
}

#http://stats.stackexchange.com/questions/61034/naive-bayes-on-continuous-variables
classify_packets_naive_bayes <- function()
{
  ret_obj <- prepare_training_and_test_data()
  training_data <- ret_obj[["training_data"]]
  test_data <- ret_obj[["test_data"]]
  
  #Remove variables that are not suitable for modeling, including variables that are perfectly/highly correlated with other variables, e.g., frac_downstream_packets is perfectly correlated with
  #frac_upstream_packets.
  cols <- c("LocalTime", "session_id", "frac_downstream_packets", "frac_downstream_bytes")  
  
  #bestmod <- naiveBayes(Event ~ ., data = training_data[, .SD, .SDcols = -cols]) #Naive Bayes with Gaussian distribution for continuous variables
  bestmod <- NaiveBayes(Event ~ ., data = training_data[, .SD, .SDcols = -cols], usekernel = TRUE) #Naive Bayes with distributions for continuous variables found by KDE
  
  #test_data[, predicted_event := as.character(predict(bestmod, newdata = test_data, type = "class"))]
  test_data[, predicted_event := as.character((predict(bestmod, newdata = test_data))$class)]
  
  prec_recall <- table(test_data[, Event], test_data[, predicted_event], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, Event, predicted_event)
  accuracy <- nrow(test_data[(Event == predicted_event),])/nrow(test_data)
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = "")) 
  measure_precision_recall(prec_recall)
  
  bestmod
}


classify_packets_deep_learning <- function(k = 5, n_units_hidden_layer_ae = 10)
{
  library(h2o)
  localH2O = h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, Xmx = '2g')
                    
  ret_obj <- prepare_training_and_test_data()
  training_data <- ret_obj[["training_data"]]
  test_data <- ret_obj[["test_data"]]
  
  cols <- c("LocalTime", "session_id", "Event", "frac_downstream_packets", "frac_downstream_bytes")
  features <- names(training_data) 
  features <- features[!(features %in% cols)]
  
  training_data_h2o <- as.h2o(training_data, destination_frame = 'training_data')
  model <- h2o.deeplearning(x = features, 
                            y = "Event", 
                            training_frame = training_data_h2o, 
                            activation = "TanhWithDropout", 
                            input_dropout_ratio = 0.2, 
                            hidden_dropout_ratios = c(0.5,0.5,0.5), 
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
  cat(paste("Overall accuracy = ", accuracy, "\n\n", sep = ""))
  measure_precision_recall(prec_recall)
  model
}


remove_noise_ae <- function(training_data, k = 5, n_units_hidden_layer = 10)
{
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
  print(fivenum(recon_error$Reconstruction.MSE))
  cutoff <- quantile(recon_error$Reconstruction.MSE, c(1 - k/100))
  cat(paste("cutoff = ", cutoff, "\n", sep = ""))
  
  #We are not taking the reconstructed values of X. We are only removing those rows from X for which the reconstruction error are in the highest k-th percentile.
  training_data[, reconstruction_error := recon_error$Reconstruction.MSE]
  setkey(training_data, reconstruction_error)
  training_data[(reconstruction_error <= cutoff),]
}

principal_component <- function()
{
  library(ggplot2)
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/training_data.csv"
  training_data <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "character", "character", "numeric", "character", 
                                   "numeric", "numeric", "numeric", "numeric",  "numeric", "numeric", "character",
                                   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", 
                                   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)

  Event <- training_data$Event
  cols <- c("LocalTime", "session_id", "frac_downstream_packets", "frac_downstream_bytes", "majority_domain", "DomainName", "Direction", "Event")
  training_data <- training_data[, .SD, .SDcols = -cols]
  
  training_data <- as.data.frame(training_data)
  
  #Drop columns with variance 0 as that presents a problem in scaling
  training_data <- training_data[, apply(training_data, 2, var, na.rm=TRUE) != 0]
  
  pc <- prcomp(training_data, scale = TRUE)
  
  #The first 4 PCs explain 92.4% of variance in total. Reply_Tweet_Text_and_Image + Tweet+Image are sort of on the left of the figure, and 
  #Uploading_Image is mostly on the right and center. However, at the center, Uploading_Image is pretty mixed up with ReTweet, Tweet_Only and Reply_Tweet_Text_Only, 
  #perhaps that is why these classes get falsely labeled as Uploading_Image so often.
  
  projected <- as.data.frame(pc$x[, c("PC1", "PC2")])
  projected$Event <- as.factor(Event)

  #Anomalous on the right, benign on the left
  png("./figures/events_first_two_pc.png",  width = 600, height = 480, units = "px")
  projected <- data.frame(projected)
  p <- ggplot(projected, aes(x = PC1, y = PC2)) + geom_point(aes(colour = Event), size = 2) + 
         theme(axis.text = element_text(colour = 'blue', size = 14, face = 'bold')) +
         theme(axis.title = element_text(colour = 'red', size = 14, face = 'bold')) + 
         ggtitle("Projections along first two PCs for events")
  print(p)
  dev.off()
  pc
}

analyze_training_data <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/training_data.csv"
  training_data <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("Date", "character", "character", "numeric", "character", 
                                   "numeric", "numeric", "numeric", "numeric",  "numeric", "numeric", "character",
                                   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", 
                                   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  cols <- c("LocalTime", "DomainName", "Direction", "session_id", "Event", "majority_domain")
  cor_matrix <- cor(training_data[, .SD, .SDcols = -cols])
  df <- as.data.frame(cor_matrix)
  df <- cbind(variable1 = rownames(df), df)
  rownames(df) <- NULL
  mdata <- melt(df, id=c("variable1"))
  mdata <- subset(mdata, ((value > 0.9) & (value < 1)))
  colnames(mdata) <- c("variable1", "variable2", "correlation")
  mdata <- mdata[rev(order(mdata$correlation)),]
  mdata <- mdata[(seq(1, nrow(mdata), 2)),]
  
  cat(paste("length(unique(training_data$total_bytes)) = ", length(unique(training_data$total_bytes)), 
            ", length(unique(training_data$pkt_bytes)) = ", length(unique(training_data$pkt_bytes)), "\n", sep = ""))
  mdata
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

