library(data.table)

load_packet_data <- function()
{
  filename <- "/Users/blahiri/cleartrail_osn/SET2/DevQA_DataSet1/PacketData_DevQA_TestCase1.csv"
  this_set <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
  this_set[, LocalTime := strftime(strptime(this_set$LocalTime, "%d/%b/%Y %H:%M:%S"), "%H:%M:%S")]     
              
  #Get the sessions (unique combinations of server IP, server port, client IP, client port) first. Get the earliest and last timestamps for each, and get the number of packets exchanged also.
  #Identify the server and client IP for each packet. Both can be senders and receivers.
  this_set[, ServerIP := apply(this_set, 1, function(row) get_server_IP(as.character(row["SourceIP"]), as.character(row["DestIP"])))]
  this_set[, ClientIP := apply(this_set, 1, function(row) get_client_IP(as.character(row["SourceIP"]), as.character(row["DestIP"])))]
  this_set[, ServerPort := apply(this_set, 1, function(row) get_server_port(as.numeric(row["SourcePort"]), as.numeric(row["DestPort"])))]
  this_set[, ClientPort := apply(this_set, 1, function(row) get_client_port(as.numeric(row["SourcePort"]), as.numeric(row["DestPort"])))]
  this_set[, Direction := apply(this_set, 1, function(row) get_direction(as.character(row["SourceIP"]), as.character(row["DestIP"])))]
  
  setkey(this_set, ServerIP, ServerPort, ClientIP, ClientPort)
  sessions <- this_set[, list(start_time = min(LocalTime), end_time = max(LocalTime), n_packets = length(LocalTime)), by = list(ServerIP, ServerPort, ClientIP, ClientPort)]
}


get_server_IP <- function(SourceIP, DestIP)
{
  if (substr(SourceIP, 1, 4) == "192.")  #Packet from client to server
    return(DestIP)
  return(SourceIP)
}

get_server_port <- function(SourcePort, DestPort)
{
  if (SourcePort == 443)  #Packet from server to client
    return(SourcePort)
  return(DestPort)
}

get_client_IP <- function(SourceIP, DestIP)
{
  if (substr(SourceIP, 1, 4) == "192.")  #Packet from client to server
    return(SourceIP)
  return(DestIP)
}

get_client_port <- function(SourcePort, DestPort)
{
  if (SourcePort == 443)  #Packet from server to client
    return(DestPort)
  return(SourcePort)
}

get_direction <- function(SourceIP, DestIP)
{
  if (substr(SourceIP, 1, 4) == "192.")  #Packet from client to server
    return("upstream")
  return("downstream")
}