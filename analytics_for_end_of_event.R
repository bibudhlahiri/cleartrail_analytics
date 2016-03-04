library(data.table)
library(rpart)
library(e1071)

#Assign numbers to events. Give an event number to each packet depending on its timestamp. Find the flows that coincide time-wise with ends of events and mark the last flow among them 
#as terminating flow. Then, do an analysis of lengths (in terms of number of packets) of flows that are terminating vs flows that are non-terminating.
  
terminating_flows <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
    
  #Filter for domain names to reduce noise.
  setkey(revised_packets, DomainName)
  revised_packets <- revised_packets[(DomainName %in% c("upload.twitter.com", "syndication.twitter.com", "twitter.com")),]
        
  #Get the event corresponding to each transaction
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv"
  events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("character", "Date", "Date"),
                    data.table = TRUE)
  events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EventNumber := 1:nrow(events)]
  
  #Eliminate timestamps for which we do not have any labels
  user_starts_at <- events[1, StartTime]
  user_ends_at <- events[nrow(events), EndTime]
  setkey(revised_packets, LocalTime)
  revised_packets <- revised_packets[((LocalTime >= as.character(user_starts_at)) & (LocalTime <= as.character(user_ends_at))),]
  
  revised_packets <- revised_packets[order(LocalTime),]
  revised_packets[, EventNumber := apply(revised_packets, 1, function(row) lookup_event(as.character(row["LocalTime"]), events))]
  revised_packets$EventNumber <- as.numeric(revised_packets$EventNumber)
  
  #There are some "holes" in time when we do not know what happened. Let us skip those for now.
  setkey(revised_packets, EventNumber)
  revised_packets <- revised_packets[(!is.na(EventNumber)),]
  
  terminating_flow_ids <- unique(revised_packets[(LocalTime %in% events$EndTime), flow_id])
  revised_packets[(flow_id %in% terminating_flow_ids), terminating_flow := TRUE]
  revised_packets[(is.na(terminating_flow)), terminating_flow := FALSE]
  
  setkey(revised_packets, flow_id)
  flow_summaries <- revised_packets[, list(n_packets = length(Rx), direction = unique(Direction), 
                                           n_downstream_packets = get_n_downstream_packets(.SD),
                                           n_upstream_packets = get_n_upstream_packets(.SD), 
                                           upstream_bytes = get_upstream_bytes(.SD),
                                           downstream_bytes = get_downstream_bytes(.SD),
                                           terminating_flow = unique(terminating_flow)), by = flow_id, 
                                           .SDcols = c("Direction", "Rx", "Tx", "terminating_flow", "flow_id")]
                                           
  flow_summaries[, total_bytes := upstream_bytes + downstream_bytes]
  flow_summaries[, avg_bytes_per_packet := total_bytes/n_packets]
  flow_summaries[, avg_upstream_bytes_per_packet := upstream_bytes/n_packets]
  flow_summaries[, avg_downstream_bytes_per_packet := downstream_bytes/n_packets]
  
  flow_summaries$session_id <- sapply(strsplit(flow_summaries$flow_id, split='_', fixed=TRUE), function(x) (x[1]))
  flow_summaries$flow_id <- sapply(strsplit(flow_summaries$flow_id, split='_', fixed=TRUE), function(x) (x[2]))
  
  setkey(flow_summaries, session_id, flow_id)
  flow_summaries <- flow_summaries[order(session_id, flow_id),]
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/flow_summaries.csv"
  write.table(flow_summaries, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  
  setkey(flow_summaries, terminating_flow)
  length_check <- flow_summaries[, list(median_n_packets = median(n_packets), avg_n_packets = mean(n_packets)), by = terminating_flow] 
  
  #avg_n_packets is 6.889302 for non-terminating flows, 4.12676 for terminating flows. However, median n_packets is 2 for both.
  
  #TODO: Fit a decision tree on flow_summaries to predict terminating_flow
  revised_packets
}

classify_flows <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/flow_summaries.csv"
  flow_summaries <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                           colClasses = c("numeric", "numeric", "character", "numeric", "numeric", "numeric", "numeric", "character", 
                                          "numeric", "numeric", "numeric", "numeric", "numeric"),
                           data.table = TRUE)
  flow_summaries$terminating_flow <- as.factor(flow_summaries$terminating_flow)
  train = sample(1:nrow(flow_summaries), 0.7*nrow(flow_summaries))
  test = (-train)
  cat(paste("Size of training data = ", length(train), ", size of test data = ", (nrow(flow_summaries) - length(train)), "\n", sep = ""))
  
  training_data <- flow_summaries[train, ]
  training_data <- create_bs_by_over_and_undersampling(training_data)
  test_data <- flow_summaries[test, ]
  
  print(table(training_data$terminating_flow)) 
  print(table(test_data$terminating_flow))
  
  #Note: Event cannot be kept as a predictor as it would not be available in real data
  tune.out <- tune.rpart(terminating_flow ~ n_packets + factor(direction) + n_downstream_packets + n_upstream_packets + 
                          + upstream_bytes + total_bytes + avg_bytes_per_packet + avg_upstream_bytes_per_packet + avg_downstream_bytes_per_packet, 
                         data = training_data, minsplit = c(5, 10, 15, 20), maxdepth = seq(5, 30, 5))
  print(summary(tune.out))
   
  bestmod <- tune.out$best.model
  test_data[, predicted_terminating_flow := as.character(predict(bestmod, newdata = test_data, type = "class"))]
  
  prec_recall <- table(test_data[, terminating_flow], test_data[, predicted_terminating_flow], dnn = list('actual', 'predicted'))
  print(prec_recall)
  
  #Measure overall accuracy
  setkey(test_data, terminating_flow, predicted_terminating_flow)
  cat(paste("Overall accuracy = ", nrow(test_data[(terminating_flow == predicted_terminating_flow),])/nrow(test_data), 
            ", recall = ", prec_recall[2,2]/sum(prec_recall[2,]), 
            ", precision = ", prec_recall[2,2]/sum(prec_recall[,2]), "\n\n", sep = "")) #0.79360465
  #terminating_flow can be identified with recall of 0.6521 and precision of 0.1923
  
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/flow_summaries_with_predicted_terminating_flow.csv"
  #Write the test data back with the predicted values of terminating_flow. No need to write the data points that come from training data because predicted_terminating_flow will be NA for them.
  setkey(test_data, session_id, flow_id)
  test_data <- test_data[order(session_id, flow_id),]
  write.table(test_data, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  test_data
}

create_bs_by_over_and_undersampling <- function(df)
{
  set.seed(1)
  n_df <- nrow(df)
  size_each_part <- n_df/2

  majority_set <- df[(terminating_flow == FALSE),]
  n_majority <- nrow(majority_set)
  cat(paste("n_majority = ", n_majority, ", n_df = ", n_df, ", size_each_part = ", size_each_part, "\n", sep = ""))
  sample_majority_ind <- sample(1:n_majority, size_each_part, replace = FALSE)
  sample_majority <- majority_set[sample_majority_ind, ]
    
  minority_set <- df[(terminating_flow == TRUE),]
  n_minority <- nrow(minority_set)
  rep_times <- size_each_part%/%nrow(minority_set)
  oversampled_minority_set <- minority_set
  for (i in 1:(rep_times - 1))
  {
    oversampled_minority_set <- rbindlist(list(oversampled_minority_set, minority_set))
  }
  rem_sample_id <- sample(1:n_minority, size_each_part%%nrow(minority_set), replace = FALSE)
  rem_sample <- minority_set[rem_sample_id, ]
  oversampled_minority_set <- rbindlist(list(oversampled_minority_set, rem_sample))

  bal_df <- rbindlist(list(sample_majority, oversampled_minority_set))
}

lookup_event <- function(LocalTime, events)
{
  setkey(events, StartTime, EndTime) #the following line gives wrong result without setkey
  matching_row <- events[((LocalTime >= as.character(StartTime)) & (LocalTime <= as.character(EndTime))),]
  matching_row[, EventNumber]
}

get_n_downstream_packets <- function(dt)
{
  nrow(dt[(Direction == "downstream")])
}

get_n_upstream_packets <- function(dt)
{
  nrow(dt[(Direction == "upstream")])
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
