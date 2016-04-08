library(data.table)
library(rpart)
library(e1071)

#The following function creates timestamp-specific aggregates from revised packet data.

create_timestamp_specific_features <- function()
{
  #filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
                    
  #The session and flow-related features come by aggregating session and flow-related data at each instant.
  
  revised_packets <- revised_packets[order(LocalTime),]
  ts_specific_features <- revised_packets[, list(n_packets = length(Rx), n_sessions = uniqueN(session_id), n_flows = uniqueN(flow_id), 
                                                  n_downstream_packets = get_n_downstream_packets(.SD),
                                                  n_upstream_packets = get_n_upstream_packets(.SD), 
                                                  majority_domain = get_majority_domain(.SD),
                                                  upstream_bytes = get_upstream_bytes(.SD),
                                                  downstream_bytes = get_downstream_bytes(.SD)
                                            ), by = LocalTime,
                                           .SDcols=c("Direction", "Rx", "Tx", "session_id", "flow_id", "DomainName")]
  ts_specific_features$majority_domain <- vapply(ts_specific_features$majority_domain, paste, collapse = ", ", character(1L))
  
  #Add a few more features
  ts_specific_features[, frac_upstream_packets := n_upstream_packets/n_packets]
  ts_specific_features[, frac_downstream_packets := n_downstream_packets/n_packets]
  ts_specific_features[, avg_packets_per_session := n_packets/n_sessions]
  ts_specific_features[, avg_packets_per_flow := n_packets/n_flows]
  
  ts_specific_features[, total_bytes := upstream_bytes + downstream_bytes]
  ts_specific_features[, frac_upstream_bytes := upstream_bytes/total_bytes]
  ts_specific_features[, frac_downstream_bytes := downstream_bytes/total_bytes]
  ts_specific_features[, avg_bytes_per_packet := total_bytes/n_packets]
  ts_specific_features[, avg_upstream_bytes_per_packet := upstream_bytes/n_packets]
  ts_specific_features[, avg_downstream_bytes_per_packet := downstream_bytes/n_packets]
  
  #filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/ts_specific_features_23_Feb_2016_Set_I.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/ts_specific_features_23_Feb_2016_Set_II.csv"
  
  #Re-order by time before writing to CSV
  setkey(ts_specific_features, LocalTime)
  ts_specific_features <- ts_specific_features[order(LocalTime),]
  write.table(ts_specific_features, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  ts_specific_features
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