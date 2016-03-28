library(data.table)
library(ggplot2)
library(plyr)
require(scales)
library(grid)
library(gridExtra)

#Using data.frame
temporal_aggregation <- function()
{
  filename <- "~/cleartrail_osn/SET3/TC2/predicted_event_types.csv"
  predicted_event_types <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
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
  cat(paste("Accuracy based on temporal_aggregate is ", nrow(temporal_aggregate[(actual_event == majority_predicted_event),])/nrow(temporal_aggregate), "\n", sep = "")) # 0.819230769230769
  
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
  start_end_event
}

get_majority_predicted_event <- function(dt)
{
  tt <- table(dt$predicted_event)
  names(tt[which.max(tt)])
}

visualize_events <- function()
{
  image_file <- "~/cleartrail_analytics/figures/timeline_view.png"
  
  #Process the predicted events and timeline data
  
  start_end_event <- temporal_aggregation()
  n_start_end_event <- nrow(start_end_event)

  #Fill in the gaps in start_end_event  
  for (i in 2:n_start_end_event)
  {
    gap <- as.numeric(difftime(strptime(start_end_event[i, "StartTime"], "%H:%M:%S"), strptime(start_end_event[i-1, "EndTime"], "%H:%M:%S"), units = "secs"))
    if (gap > 1)
    {
      cat(paste("gap found for row = ", i, "\n", sep = ""))
      cat("start_end_event before pushdown\n")
      print(start_end_event)
      #Add a junk row at end before we start pushing down.
      start_end_event <- rbind(start_end_event, data.frame(Event = "", StartTime = "", EndTime = ""))
      rownames(start_end_event) <- NULL #Adjust row numbers after addition
         
      #Push down everything from i-th row to end of start_end_event. Run the loop starting from end.
      for (k in n_start_end_event:i)
      {
           start_end_event[k + 1, "Event"] <- start_end_event[k, "Event"]
           start_end_event[k + 1, "StartTime"] <- start_end_event[k, "StartTime"]
           start_end_event[k + 1, "EndTime"] <- start_end_event[k, "EndTime"]
      }
      start_end_event[i, "Event"] <- "Missing"
      start_end_event[i, "StartTime"] <- strftime(strptime(as.character(start_end_event[i-1, "EndTime"]), "%H:%M:%S") + 1, "%H:%M:%S")
      start_end_event[i, "EndTime"] <- strftime(strptime(as.character(start_end_event[i+1, "StartTime"]), "%H:%M:%S") - 1, "%H:%M:%S")
      
      n_start_end_event <- nrow(start_end_event) #Needs update for the next iteration
      cat("start_end_event after pushdown\n")
      print(start_end_event)
    }
  }
  start_end_event$duration <- as.numeric(difftime(strptime(start_end_event$EndTime, "%H:%M:%S"), strptime(start_end_event$StartTime, "%H:%M:%S"), units = "secs")) + 1
  start_end_event$source <- rep("Predicted", nrow(start_end_event))
  
  time_start_predicted <- start_end_event[1, "StartTime"]
  start_end_event <- start_end_event[, !(colnames(start_end_event) %in% c("StartTime", "EndTime"))]
  print(start_end_event)
  
  #Process the actual events and timeline data
  filename <- "/Users/blahiri/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv"
  events <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("character", "Date", "Date"),
                    data.table = TRUE)
  events[, StartTime := strftime(strptime(events$StartTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, EndTime := strftime(strptime(events$EndTime, "%H:%M:%S"), "%H:%M:%S")]
  events[, Event := apply(events, 1, function(row) gsub(" ", "_", as.character(row["Event"])))]
  events[(Event %in% c("User_Login", "User_mouse_drag_end", "User_mouse_wheel_down", "Like")), Event := "Other"]
  events[(Event == "Reply_Tweet_Text_and_Image"), Event := "Tweet+Image"]
  events[(Event %in% c("Reply_Tweet_Text_Only", "ReTweet", "Tweet_Only")), Event := "Tweet_Text_Only"]
  events$duration <- as.numeric(difftime(strptime(events$EndTime, "%H:%M:%S"), strptime(events$StartTime, "%H:%M:%S"), units = "secs")) + 1
  events[, source := rep("Actual", nrow(events))]
  
  
  time_start_actual <- events[1, StartTime]
  ylab_common <- paste("Time (in seconds since ", min(c(time_start_actual, time_start_predicted)), ")\n", sep = "")
  events[ ,`:=`(StartTime = NULL, EndTime = NULL)]
  
  print(events)
  
  
  #p1 at bottom, p2 at top
  p1 <- ggplot(events, aes(x = source, y = duration, fill = Event)) + geom_bar(stat = "identity", width = 0.1) + xlab("\nEvent Type") + ylab(ylab_common) + 
        scale_y_continuous(breaks = seq(0, 1500, 300), limits = c(0, 1500)) +  theme_bw() + coord_flip() 
        #+ theme(plot.margin = unit(c(-1,0.5,0.5,0.5), "lines"))
  p2 <- ggplot(start_end_event, aes(x = source, y = duration, fill = Event)) + geom_bar(stat = "identity", width = 0.1) + xlab("\nEvent Type") + labs(y = NULL) + 
        scale_y_continuous(breaks = seq(0, 1500, 300), limits = c(0, 1500)) + theme_bw() + coord_flip() 
        #+ theme(axis.text.x=element_blank(), axis.title.x=element_blank(), plot.title=element_blank(), axis.ticks.x=element_blank(), plot.margin = unit(c(0.5,0.5,-1,0.5), "lines"))
  
  
  gp1 <- ggplot_gtable(ggplot_build(p1))
  gp2 <- ggplot_gtable(ggplot_build(p2))
  
  frame_grob <- grid.arrange(gp2, gp1, ncol = 1, nrow=2)
  grob <- grid.grab()

  png(image_file, width = 600, height = 600)
  grid.newpage()
  grid.draw(grob)
  dev.off()
  list(start_end_event = start_end_event, events = events)
}