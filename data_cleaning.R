# Data Cleaning script

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