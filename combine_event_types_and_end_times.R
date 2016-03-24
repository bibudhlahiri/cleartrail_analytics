library(data.table)

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
  
  print(temporal_aggregate)
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

#http://stackoverflow.com/questions/20349929/stacked-bar-plot-in-r
visualize_events <- function()
{
  dat <- read.table(text = "A   B   C   D   E   F    G
1 480 780 431 295 670 360  190
2 720 350 377 255 340 615  345
3 460 480 179 560  60 735 1260
4 220 240 876 789 820 100   75", header = TRUE)

library(reshape2)

dat$row <- seq_len(nrow(dat))
dat2 <- melt(dat, id.vars = "row")
print(dat2)

library(ggplot2)

ggplot(dat2, aes(x=variable, y=value, fill=row)) + 
  geom_bar(stat="identity") +
  xlab("\nType") +
  ylab("Time\n") +
  guides(fill=FALSE) +
  theme_bw()
}