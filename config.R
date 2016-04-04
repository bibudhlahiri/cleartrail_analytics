#This is a configuation file. Just define your variables here.

revised_pkts_trg <- "~/cleartrail_osn/SET3/TC1/RevisedPacketData_23_Feb_2016_TC1.csv"

#File where the human user saves the event names, start and end times. Please make sure the times are entered in a consistent manner (HH24:MM:SS),
#the first row has the following column names: Event, StartTime and EndTime, and the events are of the following types: User Login, Tweet+Image,
#Tweet Only, ReTweet, Reply Tweet Text Only, Reply Tweet Text and Image, Like, User mouse drag end, User mouse wheel down.

events_trg <- "~/cleartrail_osn/SET3/TC1/23_Feb_2016_Set_I.csv"

#This is the file that contains the aggregate characteristics of individual timestamps. 
hidden_and_vis_states_trg <- "~/cleartrail_osn/SET3/TC1/hidden_and_vis_states_23_Feb_2016_Set_I.csv"

revised_pkts_test <- "~/cleartrail_osn/SET3/TC2/RevisedPacketData_23_Feb_2016_TC2.csv"
events_test <- "~/cleartrail_osn/SET3/TC2/23_Feb_2016_Set_II.csv"
hidden_and_vis_states_test <- "~/cleartrail_osn/SET3/TC2/hidden_and_vis_states_23_Feb_2016_Set_II.csv"

predicted_event_types <- "~/cleartrail_osn/SET3/TC2/predicted_event_types.csv"

saved_model <- "~/cleartrail_analytics/nb_model.rds"
start_end_event_file <- "~/cleartrail_osn/SET3/TC2/start_end_event.csv"