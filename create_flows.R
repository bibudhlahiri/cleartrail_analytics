library(data.table)

create_session_data <- function()
{
  #filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/23_Feb_2016_Packets_DataSet_TC1.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Packets_DataSet_TC2.csv"
  
  this_set <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
  this_set[, LocalTime := strftime(strptime(this_set$LocalTime, "%d/%b/%Y %H:%M:%S"), "%H:%M:%S")] 
              
  #Get the sessions (unique combinations of server IP, server port, client IP, client port) first. Get the earliest and last timestamps for each, and get the number of packets exchanged also.
  #Identify the server and client IP for each packet. Both can be senders and receivers.
  this_set[, ServerIP := apply(this_set, 1, function(row) get_server_IP(as.character(row["SourceIP"]), as.numeric(row["SourcePort"]), as.character(row["DestIP"]), as.numeric(row["DestPort"])))]
  this_set[, ClientIP := apply(this_set, 1, function(row) get_client_IP(as.character(row["SourceIP"]), as.numeric(row["SourcePort"]), as.character(row["DestIP"]), as.numeric(row["DestPort"])))]
  this_set[, ServerPort := apply(this_set, 1, function(row) get_server_port(as.numeric(row["SourcePort"]), as.numeric(row["DestPort"])))]
  this_set[, ClientPort := apply(this_set, 1, function(row) get_client_port(as.numeric(row["SourcePort"]), as.numeric(row["DestPort"])))]
  this_set[, Direction := apply(this_set, 1, function(row) get_direction(as.numeric(row["DestPort"])))]
  
  #The major domains are *.twimg.com, pbs.twimg.com, twitter.com and upload.twitter.com. Put everything else in "minor_domain"
  this_set[!(DomainName %in% c("*.twimg.com", "pbs.twimg.com", "twitter.com", "upload.twitter.com")), DomainName := "minor_domain"]
  
  setkey(this_set, ServerIP, ServerPort, ClientIP, ClientPort)
  sessions <- this_set[, list(start_time = min(LocalTime), end_time = max(LocalTime), n_packets = length(LocalTime)), by = list(ServerIP, ServerPort, ClientIP, ClientPort)] 
  sessions$duration <- as.numeric(difftime(strptime(sessions$end_time, "%H:%M:%S"), strptime(sessions$start_time, "%H:%M:%S"), units = "secs"))
  sessions[, session_id := 1:nrow(sessions)]
  revised_packet_data <- create_flow_data(this_set, sessions)
  
  #filename <- "/Users/blahiri/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv"
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv"
  write.table(revised_packet_data, filename, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
}

create_flow_data <- function(packets, sessions)
{
  revised_packet_data <- data.table()
  n_sessions <- nrow(sessions)
  setkey(packets, ServerIP, ServerPort, ClientIP, ClientPort)
  
  for (i in 1:n_sessions)
  {
    #Get the packets corresponding to this session. Find which ones belong to the same flow. Set the flow_id and session_id in the packet data. A flow is a set of contiguous packets
    #that move in a session, all in the same direction.
    packets_this_session <- packets[((ServerIP == sessions[i, ServerIP]) & (ClientIP == sessions[i, ClientIP]) & (ServerPort == sessions[i, ServerPort]) & (ClientPort == sessions[i, ClientPort])),]
    n_packets_this_session <- nrow(packets_this_session)
    
    direction <- packets_this_session[1, Direction]
    current_flow_id <- 1
    packets_this_session[, session_id := numeric(.N)]
    packets_this_session[, flow_id := character(.N)]
    
    packets_this_session[1, session_id := sessions[i, session_id]]
    packets_this_session[1, flow_id := paste(sessions[i, session_id], "_", current_flow_id, sep = "")]
    
    if (n_packets_this_session > 1)
    {
      for (j in 2:n_packets_this_session)
      {
        #Is the direction of this packet different from the direction of the previous packet in the same flow? If so, then we need to start a new flow.
        if (packets_this_session[j, Direction] != direction)
        {
          direction <- packets_this_session[j, Direction]
          current_flow_id <- current_flow_id + 1
        }
        packets_this_session[j, session_id := sessions[i, session_id]]
        packets_this_session[j, flow_id := paste(sessions[i, session_id], "_", current_flow_id, sep = "")]
      }
    }
    revised_packet_data <- rbindlist(list(revised_packet_data, packets_this_session))
  }
  revised_packet_data
}


get_server_IP <- function(SourceIP, SourcePort, DestIP, DestPort)
{
  if (DestPort == 443)  #Packet from client to server
    return(DestIP)
  return(SourceIP)
}

get_server_port <- function(SourcePort, DestPort)
{
  if (SourcePort == 443)  #Packet from server to client
    return(SourcePort)
  return(DestPort)
}

get_client_IP <- function(SourceIP, SourcePort, DestIP, DestPort)
{
  if (DestPort == 443)  #Packet from client to server
    return(SourceIP)
  return(DestIP)
}

get_client_port <- function(SourcePort, DestPort)
{
  if (SourcePort == 443)  #Packet from server to client
    return(DestPort)
  return(SourcePort)
}

get_direction <- function(DestPort)
{
  if (DestPort == 443)  #Packet from client to server
    return("upstream")
  return("downstream")
}