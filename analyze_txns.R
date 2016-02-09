load_txn_data <- function()
{
  library(data.table)
  filename <- "/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_1/TC1_Transactions_DataSet.csv" 
  txn_data1 <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
  filename <- "/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_2/TC2_Transactions_DataSet.csv" 
  txn_data2 <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
  filename <- "/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_3/TC3_Transactions_DataSet.csv"
  txn_data3 <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
  filename <- "/Users/blahiri/cleartrail_osn/SampleDataSetForTwitter/TestCase_4/TC4_Transactions_DataSet.csv"
  txn_data4 <- fread(filename, header = TRUE, sep = ",", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "Date", "numeric", "numeric", "numeric", "numeric", "character", "character", "character"),
                    data.table = TRUE)
  txn_data <- rbindlist(list(txn_data1, txn_data2, txn_data3, txn_data4))
}

earth.dist <- function (long1, lat1, long2, lat2)
{
  rad <- pi/180
  dlon <- (long2 - long1)*rad
  dlat <- (lat2 - lat1)*rad
  a <- (sin(dlat/2))^2 + cos(lat1 * rad) * cos(lat2 * rad) * (sin(dlon/2))^2
  c <- 2 * atan2(sqrt(a), sqrt(1 - a))
  R <- 6378.145
  d <- R * c
  return(d)
}

calc_normalized_geo_dist <- function(input_data, idx1, idx2, distance_or_similarity)
{
  long1 <- input_data$longitude[idx1]
  lat1 <- input_data$latitude[idx1]
  long2 <- input_data$longitude[idx2]
  lat2 <- input_data$latitude[idx2]
  geo_dist <- earth.dist(long1, lat1, long2, lat2)
  #Greatest distance between any two points within the 50 states: 5,859 miles (9,429 km, HI to FL) Source: https://en.wikipedia.org/wiki/Extreme_points_of_the_United_States
  ifelse(distance_or_similarity == 1, (geo_dist/9429), 1 - (geo_dist/9429))
}

#The data given by D&B has SIC codes and not NAICS. The first two digits indicate the major group, and the 
#first 3 digits of the SIC code indicate the industry group.
calc_industry_type_dist <- function(input_data, idx1, idx2, distance_or_similarity)
{
  sic1 <- sprintf("%04d", input_data$sic1_base1_cd[idx1]) 
  sic2 <- sprintf("%04d", input_data$sic1_base1_cd[idx2]) 
  #sic1 <- str_pad(input_data[idx1, sic1_base1_cd], width = 4, side = "left", pad = "0") 
  #sic2 <- str_pad(input_data[idx2, sic1_base1_cd], width = 4, side = "left", pad = "0") 
  if (sic1 == sic2)
  {
    similarity <- 1
  }
  else if (substr(sic1, 1, 3) == substr(sic2, 1, 3))
  {
    similarity <- 0.66
  }
  else if (substr(sic1, 1, 2) == substr(sic2, 1, 2))
  {
    similarity <- 0.33
  }
  else
  {
    similarity <- 0
  }
  ifelse(distance_or_similarity == 1, 1 - similarity, similarity)
}

calc_revenue_dist <- function(input_data, idx1, idx2, distance_or_similarity)
{
  range <- log(600000000, 10) - log(4000000, 10)
  revenue1 <- log(input_data$revenue[idx1], 10)
  revenue2 <- log(input_data$revenue[idx2], 10)
  ifelse(distance_or_similarity == 1, abs(revenue1 - revenue2)/range, 1 - abs(revenue1 - revenue2)/range)
}
 
#Calculate the similarity/dissimilarity between two clients, given their geographic coordinates, revenues and industry types
#distance_or_similarity: 1 for dissimilarity, 2 for similarity
pairwise_distance <- function(input_data, idx1, idx2, distance_or_similarity, flag)
{
  normalized_geo_dist <- calc_normalized_geo_dist(input_data, idx1, idx2, distance_or_similarity)
  industry_type_dist <- calc_industry_type_dist(input_data, idx1, idx2, distance_or_similarity)
  revenue_dist <- calc_revenue_dist(input_data, idx1, idx2, distance_or_similarity)
  
  if (flag == "all")
  {
   total_measure <- normalized_geo_dist + industry_type_dist + revenue_dist
  }
  else if (flag == "geo")
  {
    total_measure <- normalized_geo_dist
  }
  else if (flag == "industry_type")
  {
    total_measure <- industry_type_dist
  }
  else if (flag == "revenue")
  {
    total_measure <- revenue_dist
  }
  total_measure
}

#Computes pairwise distances in the clients given in input_data. input_data may be a sample or a section of the data whose
#clients belong to some common cluster. An optimization that we apply is that 
#we compute the pairwise distance between two clients only when their SIC codes match upto first two digits. 
compute_all_pairwise_distance <- function(input_data, flag)
{
  library(data.table)
  n <- nrow(input_data)
  pairwise_distances <- as.data.table(expand.grid(idx1 = 1:(n-1), idx2 = 2:n))
  setkey(pairwise_distances, idx1, idx2)
  pairwise_distances <- pairwise_distances[(idx2 > idx1),]
  
  #Further optimization: compute the pairwise distance between two clients only when their SIC codes match upto first two digits
  pairwise_distances[, sic1 := input_data[idx1, sic1_base1_cd]]
  pairwise_distances[, sic2 := input_data[idx2, sic1_base1_cd]]
  pairwise_distances[, sic1_prefix := substr(sic1, 1, 2)]
  pairwise_distances[, sic2_prefix := substr(sic2, 1, 2)]
  setkey(pairwise_distances, sic1_prefix, sic2_prefix)
  pairwise_distances <- pairwise_distances[(sic1_prefix == sic2_prefix),]
  
  print(system.time(pairwise_distances[, total_measure := apply(pairwise_distances, 1, function(row) pairwise_distance(input_data,  
													                                   as.numeric(row["idx1"]), as.numeric(row["idx2"]), 1, flag))])) 
  pairwise_distances[, company1 := input_data[idx1, bus_nm]]
  pairwise_distances[, company2 := input_data[idx2, bus_nm]]
  pairwise_distances[, clnt_orgn_id1 := input_data[idx1, clnt_orgn_id]]
  pairwise_distances[, clnt_orgn_id2 := input_data[idx2, clnt_orgn_id]]
  pairwise_distances[, city1 := input_data[idx1, bus_city_nm]]
  pairwise_distances[, city2 := input_data[idx2, bus_city_nm]]
  pairwise_distances[, state1 := input_data[idx1, bus_city_cd]]
  pairwise_distances[, state2 := input_data[idx2, bus_city_cd]]
  pairwise_distances[, latitude1 := input_data[idx1, latitude]]
  pairwise_distances[, latitude2 := input_data[idx2, latitude]]
  pairwise_distances[, longitude1 := input_data[idx1, longitude]]
  pairwise_distances[, longitude2 := input_data[idx2, longitude]]
  
  pairwise_distances[, revenue1 := input_data[idx1, revenue]]
  pairwise_distances[, revenue2 := input_data[idx2, revenue]]
  pairwise_distances[, sic1_prefix := NULL]
  pairwise_distances[, sic2_prefix := NULL]
  
  pairwise_distances
}

pairwise_distance_using_clusters <- function()
{
  all_data <- load_data()
  
  model <- kmeans(log(all_data$revenue, 10), centers = 4)
  all_data$revenue_bucket <- as.factor(model$cluster) 
  
  model <- kmeans(all_data[, .SD, .SDcols = c("latitude", "longitude")], centers = 5)
  all_data$region_bucket <- as.factor(model$cluster)
  
  setkey(all_data, revenue_bucket, region_bucket)
  pairwise_distances <- data.table()
  #Go by combinations of revenue_bucket and region_bucket. For each combination, find all the clients belonging to that combination,
  #and use only that subset to compute all pairwise peers.
  sink("/idn/home/blahiri/pairwise_distance_using_clusters.log")
  for (this_revenue_bucket in 1:4)
  {
    for (this_region_bucket in 1:5)
	{
	  input_data <- all_data[((revenue_bucket == this_revenue_bucket) & (region_bucket == this_region_bucket)),]
	  pairwise_distances <- rbindlist(list(pairwise_distances, compute_all_pairwise_distance(input_data, flag = "all")))
	}
  }
  unlink("/idn/home/blahiri/pairwise_distance_using_clusters.log")
  filename <- "/idn/home/blahiri/pairwise_distances.data"
  write.table(pairwise_distances, filename, sep = "|", row.names = FALSE, quote = FALSE) #7,645,419 lines
  pairwise_distances
}

find_peers_for_specific_clients <- function(clnt_orgn_ids)
{
  library(data.table)
  filename <- "/idn/home/blahiri/pairwise_distances.data" #7,645,419 rows
  pairwise_distances <- fread(filename, header = TRUE, sep = "|", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", 
					               "character", "character", "character", "character", "character", "character",
								   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  setkey(pairwise_distances, clnt_orgn_id1)
  peerset <- pairwise_distances[(clnt_orgn_id1 %in% clnt_orgn_ids),]
  
  setkey(peerset, clnt_orgn_id1, total_measure)
  peerset <- peerset[order(clnt_orgn_id1, total_measure),]
  
  peerset$company1 <- gsub(",", " ", peerset$company1)
  peerset$company2 <- gsub(",", " ", peerset$company2)
  filename <- "/idn/home/blahiri/peerset_specific_clients_second_set.csv"
  write.table(peerset, filename, sep = ",", row.names = FALSE, quote = FALSE)
  peerset
}

run_find_peers <- function()
{
  find_peers_for_specific_clients(c("037000587","037051934", "037008226", "037012969", "037360913", "037510833"))
}

kmeans_revenue <- function()
{
  all_data <- load_data()
  cluster_quality <- numeric(9)
  models <- list()
  ## Note that "k=1" won't work!
  for (k in 2:10)
  {
    model <- kmeans(log(all_data$revenue, 10), k)
	cluster_quality[k] <- model$betweenss/model$totss
	models[[k]] <- model
  }
  print(cluster_quality)
  #Looks like elbow occurs at k = 4 for which between_SS/total_SS = 91.3 %.
  #The centers are at 6.846203, 7.264252, 7.733263 and 8.324453,
  #which correspond to 7,017,831; 18,376,038; 54,108,206 and 211,082,984 respectively.
  #The cluster sizes are 20002, 24666, 18176 and 9268. The largest cluster occurs around the center 18.37 M USD 
  #which is close to the median of 21,211,828 USD.
  filename <- "./figures/elbow_kmeans_revenue.png"  
  png(filename, width = 600, height = 480, units = "px")
  plot(1:10, cluster_quality, type= "l", main = "kmeans_revenue clustering assessment",
       xlab= "k (# clusters)", ylab = "Percent of variance explained")
  dev.off()
  
  #Visualize the clusters on histogram of revenue
  all_data$cluster <- as.factor(models[[4]]$cluster)
  png(file = "./figures/revenue_clusters.png", width = 800, height = 600)
  p <- ggplot(all_data, aes(log(revenue, 10), colour = cluster)) + geom_density(alpha = 0.2) + scale_color_brewer(palette = "Set1") + 
       xlab("log(revenue) base 10") + ylab("Number of clients") + theme(axis.text = element_text(colour = 'blue', size = 14, face = 'bold')) +
         theme(axis.text.x = element_text(angle = 90)) +
         theme(axis.title = element_text(colour = 'red', size = 14, face = 'bold'))
  print(p)
  aux <- dev.off()
  
  models
}

kmeans_lat_long <- function()
{
  all_data <- load_data()
  cluster_quality <- numeric(9)
  models <- list()
  ## Note that "k=1" won't work!
  for (k in 2:10)
  {
    model <- kmeans(all_data[, .SD, .SDcols = c("latitude", "longitude")], k)
	cluster_quality[k] <- model$betweenss/model$totss
	models[[k]] <- model
  }
  print(cluster_quality)
  #Looks like elbow occurs at k = 5 for which between_SS/total_SS = 90.81%.
  #The centers are at (41.23202, -87.15486: Chicago, 14482 clients), (31.46940, -82.61211: Atlanta, 10405 clients), (33.59450, -97.51894: Dallas, 10563 clients),
  #(37.06662, -119.29791: San Francisco, 15250 clients), (40.76113, -74.63883: New York, 21412 clients). Makes sense: number of clients around the cluster centers
  #are roughly proportional to the sizes of the corresponding cities.
  filename <- "./figures/elbow_kmeans_lat_long.png"  
  png(filename, width = 600, height = 480, units = "px")
  plot(1:10, cluster_quality, type= "l", main = "kmeans_lat_long clustering assessment",
       xlab= "k (# clusters)", ylab = "Percent of variance explained")
  dev.off()
  
  #Visualize the clusters on a 2D plane
  all_data$cluster <- as.factor(models[[5]]$cluster)
  png(file = "./figures/lat_long_clusters.png", width = 800, height = 600)
  p <- ggplot(all_data, aes(longitude, latitude)) + geom_point(aes(colour = cluster)) +  
       xlab("Longitude") + ylab("Latitude") + theme(axis.text = element_text(colour = 'blue', size = 14, face = 'bold')) +
         theme(axis.text.x = element_text(angle = 90)) +
         theme(axis.title = element_text(colour = 'red', size = 14, face = 'bold'))
  print(p)
  aux <- dev.off()
  
  models
}

analyze_results <- function()
{
  #Since idx1 and idx2 get re-initialized when input_data is regenerated every time in pairwise_distance_using_clusters, 
  #the final result will have same value of idx1 corresponding to various (20) values of company1.
  library(data.table)
  filename <- "/idn/home/blahiri/pairwise_distances.data" #7,645,419 rows
  pairwise_distances <- fread(filename, header = TRUE, sep = "|", stringsAsFactors = FALSE, showProgress = TRUE, 
                    colClasses = c("numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", 
					               "character", "character", "character", "character", "character", "character",
								   "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"),
                    data.table = TRUE)
  #setkey(pairwise_distances, clnt_orgn_id1, total_measure)
  #pairwise_distances <- pairwise_distances[order(clnt_orgn_id1, total_measure),]
  setkey(pairwise_distances, clnt_orgn_id1)
  peers_by_client <- pairwise_distances[, list(n_peers = length(clnt_orgn_id2)), by = clnt_orgn_id1] #Peers found for 76,572 unique clients
  peers_by_client <- peers_by_client[order(-n_peers),] 
  print(fivenum(peers_by_client$n_peers))  #1, 19, 52, 135, 801
  
  filename <- "./figures/n_peers_distribution.png"  
  png(filename, width = 600, height = 480, units = "px")
  p <- ggplot(peers_by_client, aes(n_peers)) + geom_density(alpha = 0.2) +  
       xlab("No. of peers") + ylab("Probability") + theme(axis.text = element_text(colour = 'blue', size = 14, face = 'bold')) +
         theme(axis.title = element_text(colour = 'red', size = 14, face = 'bold'))
  print(p)
  aux <- dev.off()
  #What is the distribution of median distance with peers?
  peers_by_client
}

