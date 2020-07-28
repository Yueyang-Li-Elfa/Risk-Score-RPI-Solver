# Data Cleaning script
# To run in your environment, don't forget to change the path included in the script

library(readr)
library(dplyr)

# First Read in the poi dataset: core_poi
# set new wd as the path where you store the poi data
setwd("~/Desktop/COVID-19 Challenge/SafeGraph/core_places/core_poi")

# Get the list of file names
file_list <- list.files()
# Create an empty dataframe
core_poi <- data.frame()
# Read in all files
for (i in 1:length(file_list)) {
  temp <- read_csv(file_list[i])
  core_poi <- rbind(core_poi, temp)
}

# set wd back to original one
setwd("~/Desktop/COVID-19 Challenge")

# Select only POIs located in the City of LA
core_poi_LA <- core_poi %>% filter(region == "CA", city == "Los Angeles")

# Please find script POI_location.ipynb
# Read in the output file here
comm <- read_csv("infection_rate/POI_comm_rates_0625.csv")

# Add city and community
core_poi_LA <- core_poi_LA %>% 
  left_join(comm %>% select(safegraph_id, community, city),
            by = c("safegraph_place_id" = "safegraph_id")) %>% 
  rename(city_original = city.x, city = city.y)

# Read in square feet dataset
poi_SquareFeet <- read_csv("SafeGraph/core_places/poi_SquareFeet.csv")

# Add square feet
core_poi_LA <- core_poi_LA %>% 
  left_join(poi_SquareFeet %>% select(safegraph_place_id, area_square_feet), 
            by = c("safegraph_place_id"))

# Save the file
write_csv(core_poi_LA, path = "Data/poi_extended.csv")

# Impute open hours
# Please find script POI_location.ipynb
# Read in the output file here
open_hours <- read_csv("SafeGraph/core_places/poi_hour.csv") 

# Impute open hours, using median value
# when calculating median, exclude 0s
# IF this poi is noy closed this day, it will open for ... hours.
impute_median <- function (x) {
  value <- median(x[x != 0], na.rm = T)
  for (i in 1:length(x)) {
    if (is.na(x[i])) {
      x[i] <- value
    }
  }
  return(x)
}
open_hours[, 2:8] <- map_dfr(open_hours[, 2:8], impute_median)

# Gather the open hours data to a tidy dataframe
open_hours <- open_hours %>% 
  gather("weekday", "open_hours", -safegraph_place_id)

# Save the file
write_csv(open_hours, path = "Data/poi_hour_imputed.csv")