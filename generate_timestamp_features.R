library(data.table)
library(rpart)
library(e1071)

#When labeled events are not available, and we only need to re-generate labels on a daily basis, this function is to be used.
label_packets_without_events <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv"
  revised_packets <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character", 
                                   "numeric", "numeric", "numeric", "numeric", "character", "numeric", "numeric"),
                    data.table = TRUE)
                    
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
  
  #Add a few more features
  hidden_and_vis_states[, frac_upstream_packets := n_upstream_packets/n_packets]
  hidden_and_vis_states[, frac_downstream_packets := n_downstream_packets/n_packets]
  hidden_and_vis_states[, avg_packets_per_session := n_packets/n_sessions]
  hidden_and_vis_states[, avg_packets_per_flow := n_packets/n_flows]
  
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