#This is a wrapper script that can be run in training or test mode. In the test mode, it reads a pre-built model and generates predictions on a given pre-processed test dataset.
#In the training mode, it creates and saves a model on a given pre-processed training dataset.

run_everything <- function()
{
  source("config.R")
  source("detect_event_types.R")
  source("create_final_viz.R")
  
  #In which mode is the function run_everything() in wrapper.R run? The three options are as follows:
  #1. training_and_testing is the full package where you (re)train the model. It assumes you have a training dataset with start and end times of events, 
  #   as well as a test dataset with start and end times of events. It gives you the precision/recall of the individual classes on the test data.
  #2. retraining_only is applicable when you want to re-train the model but you do not have a test data to test it on. It just updates the saved model file.
  #   Does not return precision/recall.
  #3. label_generatation is something you do most days: massage the raw packet captures and send it through the saved model, and get the predicted start and end times of events.

  run_mode <- readline("What do you want to do? 1) Label generation, 2) Retraining only, 3) Training and testing: ")
  
  if (run_mode == "1")
  {
    suppressWarnings(generate_labels_only(revised_pkts_test, events_test, hidden_and_vis_states_test, 
                                 predicted_event_types, saved_model))
    temporal_aggregation(predicted_event_types, start_end_event_file)
  }
  else if (run_mode == "2")
  {
    retrain_only(revised_pkts_trg, events_trg, hidden_and_vis_states_trg, saved_model)
  }
  else 
  {
    #run_mode == "3"
    suppressWarnings(classify_packets_naive_bayes(revised_pkts_trg, events_trg, hidden_and_vis_states_trg, 
                                 revised_pkts_test, events_test, hidden_and_vis_states_test, 
                                 predicted_event_types, saved_model))
    temporal_aggregation(predicted_event_types, start_end_event_file)
  }
}






