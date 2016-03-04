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
  
  #We rename the original flow_id column as session_flow_id and split it into session_id and flow_id
  revised_packets$session_flow_id <- revised_packets$flow_id
  revised_packets$session_id <- sapply(strsplit(revised_packets$session_flow_id, split='_', fixed=TRUE), function(x) (x[1]))
  revised_packets$flow_id <- sapply(strsplit(revised_packets$session_flow_id, split='_', fixed=TRUE), function(x) (x[2]))
  revised_packets$flow_id <- as.numeric(revised_packets$flow_id)
  revised_packets$session_id <- as.numeric(revised_packets$session_id)
  
  #For each session and end timestamp, take the last matching flow as terminating flow.
  
  flow_ids_matching_endtimes <- revised_packets[(LocalTime %in% events$EndTime),]
  flow_ids_matching_endtimes <- flow_ids_matching_endtimes[, .SD, .SDcols = c("flow_id", "session_id", "LocalTime")]
  setkey(flow_ids_matching_endtimes, session_id, LocalTime)
  terminating_flows <- flow_ids_matching_endtimes[, list(max_flow_id = max(flow_id)), by = list(session_id, LocalTime)]
  terminating_flow_ids <- paste(terminating_flows$session_id, "_", terminating_flows$max_flow_id, sep = "")
  print(terminating_flow_ids)
  
  revised_packets[(session_flow_id %in% terminating_flow_ids), terminating_flow := TRUE]
  revised_packets[(is.na(terminating_flow)), terminating_flow := FALSE]
  
  setkey(revised_packets, session_flow_id)
  flow_summaries <- revised_packets[, list(n_packets = length(Rx), direction = unique(Direction), 
                                           n_downstream_packets = get_n_downstream_packets(.SD),
                                           n_upstream_packets = get_n_upstream_packets(.SD), 
                                           upstream_bytes = get_upstream_bytes(.SD),
                                           downstream_bytes = get_downstream_bytes(.SD),
                                           terminating_flow = unique(terminating_flow),
                                           avg_packets_last_k_flows = get_avg_packets_last_k_flows(.SD, 2, revised_packets)), by = session_flow_id, 
                                           .SDcols = c("Direction", "Rx", "Tx", "terminating_flow", "session_id", "flow_id")]
                                           
  flow_summaries[, total_bytes := upstream_bytes + downstream_bytes]
  flow_summaries[, avg_bytes_per_packet := total_bytes/n_packets]
  flow_summaries[, avg_upstream_bytes_per_packet := upstream_bytes/n_packets]
  flow_summaries[, avg_downstream_bytes_per_packet := downstream_bytes/n_packets]
  
  flow_summaries$session_id <- sapply(strsplit(flow_summaries$session_flow_id, split='_', fixed=TRUE), function(x) (x[1]))
  flow_summaries$flow_id <- sapply(strsplit(flow_summaries$session_flow_id, split='_', fixed=TRUE), function(x) (x[2]))
  flow_summaries$flow_id <- as.numeric(flow_summaries$flow_id)
  flow_summaries$session_id <- as.numeric(flow_summaries$session_id)
  
  setkey(flow_summaries, session_id, flow_id)
  flow_summaries <- flow_summaries[order(session_id, flow_id),]
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/flow_summaries.csv"
  write.table(flow_summaries, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  
  setkey(flow_summaries, terminating_flow)
  length_check <- flow_summaries[, list(median_n_packets = as.double(median(n_packets)), avg_n_packets = mean(n_packets)), by = terminating_flow] 
  print(length_check)
  
  #avg_n_packets is 6.839640 for non-terminating flows, 2.972222 for terminating flows. median n_packets is 2 for non-terminating flows, and 2.5 for terminating flows.
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
                          + upstream_bytes + total_bytes + avg_bytes_per_packet + avg_upstream_bytes_per_packet + avg_downstream_bytes_per_packet + avg_packets_last_k_flows, 
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
            ", precision = ", prec_recall[2,2]/sum(prec_recall[,2]), "\n\n", sep = "")) #0.9273255
  #terminating_flow can be identified with recall of 0.727272 and precision of 0.266666
  
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/flow_summaries_with_predicted_terminating_flow.csv"
  #Write the test data back with the predicted values of terminating_flow. No need to write the data points that come from training data because predicted_terminating_flow will be NA for them.
  setkey(test_data, session_id, flow_id)
  test_data <- test_data[order(session_id, flow_id),]
  write.table(test_data, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  bestmod
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

#What is the average number of packets combining this flow and the immediately previous (k-1) flows in this session
get_avg_packets_last_k_flows <- function(dt, k = 2, revised_packets)
{
  #dt has the subset for one flow id 
   
  this_session_id <- dt[1, session_id]
  this_flow_id <- dt[1, flow_id]
  setkey(revised_packets, session_id, flow_id)
  
  min_flow_id_this_session <- min(revised_packets[(session_id == this_session_id),]$flow_id)
  flow_at_window_start <- max(min_flow_id_this_session, this_flow_id - k + 1)
  cat(paste("this_session_id = ", this_session_id, ", this_flow_id = ", this_flow_id, ", min_flow_id_this_session = ", min_flow_id_this_session, 
            ", flow_at_window_start = ", flow_at_window_start, "\n", sep = ""))
  subset_packets <- revised_packets[((session_id == this_session_id) & (flow_id >= flow_at_window_start) & (flow_id <= this_flow_id)),]
  nrow(subset_packets)/k
}
