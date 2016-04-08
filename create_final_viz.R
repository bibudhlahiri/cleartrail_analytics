library(data.table)
library(ggplot2)
library(plyr)
require(scales)
library(grid)
library(gridExtra)

#Temporal aggregation for label generation only, when the actual events will not be present. Just generate the start and end times of events based on the 
#majority predicted event at each timestamp, i.e., if there are multiple packets at the same timestamp, each packet will have its own predicted event type, take 
#the majority across all the packets at that timestamp.

temporal_aggregation_for_label_gen_only <- function(predicted_event_types_file, start_end_event_file)
{
  predicted_event_types <- fread(predicted_event_types_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                       colClasses = c("Date", "character", "character", "numeric",  
                                      "numeric", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "character", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "character"),
                       data.table = TRUE)
  predicted_event_types <- predicted_event_types[, .SD, .SDcols = c("LocalTime", "predicted_event")]
  setkey(predicted_event_types, LocalTime)
  
  #Predicted events may have multiple distinct values, so take the majority.
  temporal_aggregate <- predicted_event_types[, list(majority_predicted_event = get_majority_predicted_event(.SD)), by = LocalTime,
                                              .SDcols=c("predicted_event")]
  start_end_event <- weave_start_end_times(temporal_aggregate, start_end_event_file)
}

#Temporal aggregation for full-fledged training and testing. The difference with temporal_aggregation_for_label_gen_only() is that the file with predicted events will have the 
#actual events too, and hence this method will tell what fraction of the timestamps have predicted events same as the actual event.

temporal_aggregation <- function(predicted_event_types_file, start_end_event_file)
{
  predicted_event_types <- fread(predicted_event_types_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                       colClasses = c("Date", "character", "character", "numeric", "character", 
                                      "numeric", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "character", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "numeric", 
                                      "numeric", "numeric", "numeric", "numeric", "character"),
                       data.table = TRUE)
  predicted_event_types <- predicted_event_types[, .SD, .SDcols = c("LocalTime", "Event", "predicted_event")]
  setkey(predicted_event_types, LocalTime)
  #The actual event will be only one, hence unique() works; but predicted events may have multiple distinct values, so take the majority.
  temporal_aggregate <- predicted_event_types[, list(actual_event = unique(Event), majority_predicted_event = get_majority_predicted_event(.SD)), by = LocalTime,
                                              .SDcols=c("Event", "predicted_event")]
  cat(paste("\nAccuracy based on temporal_aggregate is ", nrow(temporal_aggregate[(actual_event == majority_predicted_event),])/nrow(temporal_aggregate), "\n", sep = "")) 
  
  start_end_event <- weave_start_end_times(temporal_aggregate, start_end_event_file)
}

#This method actually derives the start and end times and delivers the final result based on the consecutive predicted labels of timestamps.
#Called from both temporal_aggregation_for_label_gen_only() and temporal_aggregation().

weave_start_end_times <- function(temporal_aggregate, start_end_event_file)
{
  #Aggregate start and end times based on temporal_aggregate
   
  n_temporal_aggregate <- nrow(temporal_aggregate)
  start_end_event <- data.table(data.frame(Event = character(n_temporal_aggregate), StartTime = character(n_temporal_aggregate), EndTime = character(n_temporal_aggregate)))
  
  start_end_event[1, Event := temporal_aggregate[1, majority_predicted_event]]
  start_end_event[1, StartTime := temporal_aggregate[1, LocalTime]]
  current_event <- temporal_aggregate[1, majority_predicted_event]
  curr_row_in_start_end_event <- 1
  
  for (i in 2:n_temporal_aggregate)
  {
     if (temporal_aggregate[i, majority_predicted_event] != current_event)
     {
       #Start of a new event has been encountered. End the previous event.
        start_end_event[curr_row_in_start_end_event, EndTime := temporal_aggregate[i-1, LocalTime]]
        curr_row_in_start_end_event <- curr_row_in_start_end_event + 1
        start_end_event[curr_row_in_start_end_event, Event := temporal_aggregate[i, majority_predicted_event]]
        start_end_event[curr_row_in_start_end_event, StartTime := temporal_aggregate[i, LocalTime]]
        current_event <- temporal_aggregate[i, majority_predicted_event]
     }
  }
  start_end_event[curr_row_in_start_end_event, EndTime := temporal_aggregate[n_temporal_aggregate, LocalTime]]
  
  #Remove the blank rows from start_end_event
  start_end_event <- data.frame(start_end_event)
  start_end_event <- start_end_event[(start_end_event$Event != ""),]
  rownames(start_end_event) <- NULL #Adjust row numbers after deletion
  
  #Default type was factor for the columns and that was creating problem in updating
  start_end_event$Event <- as.character(start_end_event$Event)
  start_end_event$StartTime <- as.character(start_end_event$StartTime)
  start_end_event$EndTime <- as.character(start_end_event$EndTime)
  write.table(start_end_event, start_end_event_file, sep = ",", row.names = FALSE, col.names = TRUE, quote = FALSE)
  start_end_event
}

get_majority_predicted_event <- function(dt)
{
  tt <- table(dt$predicted_event)
  names(tt[which.max(tt)])
}

fill_gaps <- function(input_data) 
{
  i <- 2
  n_input_data <- nrow(input_data)

  #Fill in the gaps in input_data  
  while (TRUE)
  {
    gap <- as.numeric(difftime(strptime(input_data[i, "StartTime"], "%H:%M:%S"), strptime(input_data[i-1, "EndTime"], "%H:%M:%S"), units = "secs"))
    if (is.na(gap)) 
    {
      break
    }
    if (gap > 1)
    {
      #Add a junk row at end before we start pushing down.
      input_data <- rbind(input_data, data.frame(Event = "", StartTime = "", EndTime = ""))
      rownames(input_data) <- NULL #Adjust row numbers after addition
         
      #Push down everything from i-th row to end of input_data. Run the loop starting from end.
      for (k in n_input_data:i)
      {
           input_data[k + 1, "Event"] <- input_data[k, "Event"]
           input_data[k + 1, "StartTime"] <- input_data[k, "StartTime"]
           input_data[k + 1, "EndTime"] <- input_data[k, "EndTime"]
      }
      input_data[i, "Event"] <- "Missing"
      input_data[i, "StartTime"] <- strftime(strptime(as.character(input_data[i-1, "EndTime"]), "%H:%M:%S") + 1, "%H:%M:%S")
      input_data[i, "EndTime"] <- strftime(strptime(as.character(input_data[i+1, "StartTime"]), "%H:%M:%S") - 1, "%H:%M:%S")
      
      n_input_data <- nrow(input_data) #Needs update for use in the inner for loop with k in the next iteration
      i <- i + 2 #Incrementing by 2, instead of 1, since a new row has been added
    }
    else
    {
      i <- i + 1 #No gap was found, just climb down input_data
    }
  }
  input_data
}

#How many of the actual events have a matching event of the same type and with an end time within 1 minute of the end time of the actual event?
find_match_actual_predicted <- function(events, start_end_event)
{
  events$has_match <- apply(events, 1, function(row) find_match(as.character(row["EndTime"]), as.character(row["Event"]), start_end_event))
  events
}

find_match <- function(input_endtime, input_event, events)
{
  matching <- subset(events, ((events$Event == input_event) & 
                  (abs(as.numeric(difftime(strptime(events$EndTime, "%H:%M:%S"), strptime(input_endtime, "%H:%M:%S"), units = "secs"))) <= 60))) 
  (nrow(matching) > 0)
}

visualize_events <- function(image_file, start_end_event_file, events_test)
{  
  start_end_event <- as.data.frame(fread(start_end_event_file, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                       colClasses = c("character", "character", "character"),
                       data.table = TRUE))
  #Process the predicted events and timeline data
  start_end_event <- fill_gaps(start_end_event)
  
  start_end_event$duration <- as.numeric(difftime(strptime(start_end_event$EndTime, "%H:%M:%S"), strptime(start_end_event$StartTime, "%H:%M:%S"), units = "secs")) + 1
  start_end_event$source <- rep("Predicted", nrow(start_end_event))
  time_start_predicted <- start_end_event[1, "StartTime"]
  
  #Process the actual events and timeline data
  events <- fread(events_test, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("character", "Date", "Date"),
                    data.table = TRUE)
  events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, Event := apply(events, 1, function(row) gsub(" ", "_", as.character(row["Event"])))]
  events[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down", "Like")), Event := "Other"]
  events[(Event == "Reply_Tweet_Text_and_Image"), Event := "Tweet+Image"]
  events[(Event %in% c("Reply_Tweet_Text_Only", "ReTweet", "Tweet_Only", "Tweet", "Reply_Tweet")), Event := "Tweet_Text_Only"]
  
  events <- as.data.frame(events)
  events <- fill_gaps(events)
  events$duration <- as.numeric(difftime(strptime(events$EndTime, "%H:%M:%S"), strptime(events$StartTime, "%H:%M:%S"), units = "secs")) + 1
  events$source <- rep("Actual", nrow(events))
  
  time_start_actual <- events[1, "StartTime"]
  ylab_common <- paste("Time (in seconds since ", min(c(time_start_actual, time_start_predicted)), ")\n", sep = "")
  
  #Check which one between start_end_event and events starts earlier. Fill the other one with a row of Missing.
  if (time_start_predicted > time_start_actual)
  {
    end_time <- strftime(strptime(time_start_predicted, "%H:%M:%S") - 1, "%H:%M:%S")
    filler_duration <- as.numeric(difftime(strptime(end_time, "%H:%M:%S"), strptime(time_start_actual, "%H:%M:%S"), units = "secs")) + 1
    filler <- data.frame(Event = "Missing", StartTime = time_start_actual, 
                         EndTime = end_time, duration = filler_duration,
                         source = "Predicted")
    start_end_event <- rbind(filler, start_end_event)
  }
  else
  {
    end_time <- strftime(strptime(time_start_actual, "%H:%M:%S") - 1, "%H:%M:%S")
    filler_duration <- as.numeric(difftime(strptime(end_time, "%H:%M:%S"), strptime(time_start_predicted, "%H:%M:%S"), units = "secs")) + 1
    filler <- data.frame(Event = "Missing", StartTime = time_start_predicted, 
                         EndTime = end_time, duration = filler_duration,
                         source = "Actual")
    events <- rbind(filler, events)
  }
  
  all_together <- rbind(events, start_end_event)
  all_together$Event <- factor(all_together$Event)
  
  events <- find_match_actual_predicted(events, start_end_event)
  has_matching <- subset(events, (events$has_match == TRUE))
  cat(paste("Fraction of actual events that have a matching event of the same type and with an end time within 1 minute of the end time of the actual event is ",
            nrow(has_matching)/nrow(events), "\n", sep = ""))
  
  png(image_file, width = 1200, height = 400)
  
  positions <- c("Predicted", "Actual")
  p <- ggplot(all_together, aes(x = source, y = duration, fill = Event)) + geom_bar(stat = "identity", width = 0.4) + 
       scale_x_discrete(limits = positions) + labs(title = "Timeline view of actual vs predicted events") + 
       xlab("\nEvent Type") + ylab(ylab_common) + 
        scale_y_continuous(breaks = seq(0, 1500, 300), limits = c(0, 1500)) +  theme_bw() + 
        theme(axis.text.x = element_text(colour = "grey20", size=12, face = "bold"),
              axis.text.y = element_text(colour = "grey20", size=12, face = "bold"),  
              axis.title.x = element_text(colour="grey20", size=12, face = "bold"),
              axis.title.y = element_text(colour="grey20", size=12, face = "bold"),
              plot.title = element_text(size = rel(2), colour = "blue")) + coord_flip()
  print(p)
  aux <- dev.off()
  #list(start_end_event = start_end_event, events = events)
}