# Pipeline: Generate Risk Scores
# Updated on Jul 12

# Before you run the script
# please make sure to set the [Working Directory] as where the script is located

# How to use this script
# 1. Clone the whole repository from GitHub
# 2. Download the latest 3 weeks [Weekly Places Patterns v2] data from SafeGraph, store in the [Data] folder, and unzip all files
# 3. Download COVID-19 data from: http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/
#    Click [Table: Community Case/Death], then click [Download this table]
#    Click [Table: Community Testing], then click [Download this table]
#    Store these two csv files in the [Data] folder
# 4. Set up date1, date2, date3 as "mm-dd" format
# 5. Run through the scirpt. The result will be stored in the [Risk Score] folder.

library(readr)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(sjmisc)

date1 <- "06-15"
date2 <- "06-22"
date3 <- "06-29"

# ====================================================
# Weekly Pattern: Subset POIs in LA 
# ----------------------------------------------------

# Read in SafeGraph data stored in new schema
# intermediate function, will not be called directly
read_newschema <- function(date = NA, state = "CA", city_name = "Los Angeles", part = 1) {
  file <- read_csv(str_c("Data/", date, "/patterns-part", part, ".csv"))
  file <- file %>% filter(region == state, city == city_name)
  file
}

# Read in SafeGraph data stored in either old or new schema
## date: date of the start_range column in SafeGraph weekly patterns data, in "mm-dd" format
## state: the state that you would like to focus on, default value is "CA"
## city_name: the city that you would like to focus on, default value is "Los Angeles"
read_weeklypattern <- function(date = NA, state = "CA", city_name = "Los Angeles") {
  weekly <- try(weekly <- read_csv(str_c("Data/2020-", date, "-weekly-patterns.csv")) %>%
                  filter(region == state, city == city_name ), silent = T)
  if (class(weekly) == "try-error") {
    weekly <- data.frame()
    for (i in 1:4) {
      weekly <- weekly %>% rbind(read_newschema(date = date, state = state, city_name = city_name, part = i))
    }
  }
  weekly
}

# ====================================================

# ====================================================
# Weekly Pattern: Parse the data and take average for last three weeks
# ----------------------------------------------------

# Get information related to daily visits from SafeGraph weekly patterns dataset
## x: a SafeGraph weekly patterns dataset
get_daily_visits <- function (x) {
  # select daily visit data
  temp_daily <- x %>%
    # select columns related to daily visits
    select(safegraph_place_id, date_range_start, date_range_end, visits_by_day, median_dwell) %>%
    # Parse visits_by_day [json] into columns in dataframe
    # Each represents a weekday: from Monday to Sunday
    mutate(visits_by_day = str_extract(visits_by_day, "([0-9]+,)+[0-9]")) %>%
    separate(visits_by_day, into = c("Mon", "Tue", "Wed", "Thur", "Fri", "Sat", "Sun"), 
             sep = ",", convert = T) %>%
    # Gather: to make the data tidy
    gather(key = "weekday", value = "visits_by_day", -safegraph_place_id, -date_range_start, -date_range_end, -median_dwell)
  temp_daily
}

# Get information related to hourly visits from SafeGraph weekly patterns dataset
## x: a SafeGraph weekly patterns dataset
get_hourly_visits <- function(x) {
  temp_hourly <- x %>%
    # select columns related to hourly visits
    select(safegraph_place_id, date_range_start, date_range_end, visits_by_each_hour) %>%
    # Parse visits_by_each_hour [json] into columns in dataframe
    mutate(visits_by_each_hour = str_extract(visits_by_each_hour, "([0-9]+,)+[0-9]")) %>%
    separate(visits_by_each_hour, into = c(str_c("Mon", 1:24, sep = "_"), 
                                           str_c("Tue", 1:24, sep = "_"),
                                           str_c("Wed", 1:24, sep = "_"),
                                           str_c("Thur", 1:24, sep = "_"),
                                           str_c("Fri", 1:24, sep = "_"),
                                           str_c("Sat", 1:24, sep = "_"),
                                           str_c("Sun", 1:24, sep = "_")), sep = ",", convert = T) %>%
    # Gather: to make the data tidy
    gather(key = "weekday", value = "visits_by_each_hour", -safegraph_place_id, -date_range_start, -date_range_end) %>%
    separate(weekday, into = c("weekday", "hour"), sep = "_", convert = T)
  temp_hourly
}

# Process all three datasets to get daily visits data, bind them together, and take averages
## file_1: name of the first week data
## file_2: name of the second week data
## file_3: name of the third week data
daily_process <- function(file_1 = weekly1, file_2 = weekly2, file_3 = weekly3) {
  temp_daily_processed <- get_daily_visits(file_1) %>%
    # bind three datasets together
    rbind(get_daily_visits(file_2)) %>%
    rbind(get_daily_visits(file_3)) %>%
    # take average of last three weeks
    group_by(safegraph_place_id, weekday) %>%
    summarize(avg_visits = mean(visits_by_day, na.rm = T),
              avg_median_dwell = mean(median_dwell, na.rm = T))
  temp_daily_processed
}

# Process all three datasets to get hourly visits data, bind them together, and take averages
## file_1: name of the first week data
## file_2: name of the second week data
## file_3: name of the third week data
hourly_process <- function(file_1 = weekly1, file_2 = weekly2, file_3 = weekly3) {
  temp_hourly_processed <- get_hourly_visits(file_1) %>%
    # bind 3 datasets together
    rbind(get_hourly_visits(file_2)) %>%
    rbind(get_hourly_visits(file_3)) %>%
    # calculate cv & peak visits for each week
    group_by(safegraph_place_id, weekday, date_range_start) %>%
    summarise(sd = sd(visits_by_each_hour),
              mean = mean(visits_by_each_hour),
              daily = sum(visits_by_each_hour),
              cv = ifelse(mean == 0 | daily == 1, 0, sd/mean),
              peak = max(visits_by_each_hour)) %>%
    # take average of last three weeks
    group_by(safegraph_place_id, weekday) %>%
    summarise(cv = mean(cv),
              peak = mean(peak))
  temp_hourly_processed
}

# ====================================================

# ====================================================
# Infection Rate: Calculate the infection rate for each community
# ----------------------------------------------------

# Calculate infection rate for each community, and then match them with each POI
## poi_data: poi dataset
## case_death_table: the community case and death table downloaded from http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/
## testing_table: the community testing table downloaded from http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/

calculate_infection_rate <- function(poi_data = poi, 
                                     case_daath_table = case_daath_table,
                                     testing_table = testing_table) {
  
  # Calculate infection rate for each community
  rate_original <- case_daath_table %>% 
    left_join(testing_table, by = c("geo_merge")) 
  
  rate <- rate_original %>%
    mutate(infection_rate = (cases_final-deaths_final)/(persons_tested_final-deaths_final)) %>%
    select(geo_merge, infection_rate)
  
  # Calculate infection rate for the entire LA city
  rate_LA <- rate_original %>%
    filter(str_detect(geo_merge, "^Los Angeles")) %>%
    summarise(cases_final = sum(cases_final),
              deaths_final = sum(deaths_final),
              persons_tested_final = sum(persons_tested_final)) %>%
    mutate(infection_rate = (cases_final-deaths_final)/(persons_tested_final-deaths_final))
  rate_LA <- rate_LA$infection_rate
  
  # Add infection rate for each poi
  poi_data$infection_rate <- 0
  
  for (i in 1:nrow(poi_data)) {
    city=poi_data$city[i]
    comm=poi_data$community[i]
    for (j in 1:nrow(rate)){
      if(str_contains(rate$geo_merge[j], comm,ignore.case = TRUE)==TRUE) {
        poi_data$infection_rate[i]=rate$infection_rate[j]
      }
      else if(str_contains(rate$geo_merge[j], city,ignore.case = TRUE)==TRUE & city!="Los Angeles") {
        poi_data$infection_rate[i]=rate$infection_rate[j]
      }
    }
  }
  
  # Impute rest pois (those that can't find matches) with the infection rate of LA city
  poi_data <- poi_data %>%
    mutate(infection_rate = ifelse(infection_rate == 0, rate_LA, infection_rate))
  
  # return the poi dataset with infection rate for each place
  poi_data
}

# ====================================================

# ====================================================
# Risk Score: Joining the data, and calculate the risk scores
# ----------------------------------------------------

# Calculate the risk score for each POI
## poi: poi dataset
## open_hours: open hours dataset
## daily: processed daily visits dataset, output of the function "daily_process"
## hourly: processed houly visits dataset, output of the function "hourly_process"

calculate_risk_score <- function(poi = poi,
                                 open_hours = open_hours,
                                 daily = daily,
                                 hourly = hourly) {
  
  # Joining the data
  risk <- open_hours %>%
    # Join the poi data
    left_join(poi %>% select(safegraph_place_id, location_name, top_category, latitude, longitude, street_address, city, community, postal_code, area_square_feet, infection_rate), 
              by = c("safegraph_place_id")) %>%
    # Join daily visits data
    # For those with open_hours = 0 but still have visitis, adjust the open_hours to median level
    left_join(daily, by = c("safegraph_place_id", "weekday")) %>%
    group_by(weekday) %>%
    mutate(open_hours = ifelse(open_hours == 0 & avg_visits != 0,
                               median(open_hours), open_hours)) %>%
    ungroup() %>%
    # Join the hourly visit data
    left_join(hourly, by = c("safegraph_place_id", "weekday"))
  
  # Calculate the expected number of people encountered when coming to a place
  risk <- risk %>%
    mutate(interval = ifelse(avg_visits == 0, NaN, open_hours*60 / avg_visits),
           round_visit = ceiling(avg_visits),
           encounter_max = ifelse(avg_visits == 0, 0, 
                                  ceiling(avg_median_dwell / interval)),
           encounter = ifelse(round_visit > encounter_max, encounter_max, round_visit)) %>%
    select(-round_visit, -encounter_max)
  
  # Calculate the probability 
  # that at least one person that you expect to encounter is/are infectious
  prob <- double()
  for (i in 1:nrow(risk)) {
    size <- risk$encounter[i]
    p <- risk$infection_rate[i]
    prob[i] <- ifelse(size == 0, 0,
                      sum(dbinom(1:size, size, p)))
  }
  
  # Add to the risk table
  risk$prob <- prob
  
  # Calculate risk scores
  risk <- risk %>%
    # Calculate the area per encountered person (+1 to avoid producing Inf)
    mutate(area_per_capita = area_square_feet / (encounter + 1)) %>%
    # Calculate percentile ranks for 3 main attributes:
    # 1. area_per_capita: (reversed) higher value means denser space, greater risk
    # 2. prob: higher value means greater risk
    # 3. hourly distribution (time density): higher value means greater risk
    # The final risk score is the percentile rank of previous three percentile ranks added together
    # Higher value means relatively more risk
    # This is a RELATIVE risk score
    mutate(area_per_capita_perc_rank = 1 - percent_rank(area_per_capita),
           prob_perc_rank = percent_rank(prob),
           cv_perc_rank = percent_rank(cv),
           peak_perc_rank = percent_rank(peak),
           time_density_perc_rank = percent_rank(cv_perc_rank + peak_perc_rank),
           risk_score = percent_rank(area_per_capita_perc_rank + prob_perc_rank + time_density_perc_rank))
  
  # Reorder columns and add risk level
  risk <- risk %>%
    mutate(risk_level = ifelse(risk_score <= 0.1, "low", 
                               ifelse(risk_score <= 0.5, "moderate",
                                      ifelse(risk_score <= 0.9, "high", "super high")))) %>%
    select(safegraph_place_id, location_name, top_category, latitude, longitude, street_address, postal_code, city, community, everything())
  
  # Return the final risk scores
  risk
}


# ====================================================

# ====================================================
# Call functions
# ----------------------------------------------------

# Read in SafeGraph data
weekly1 <- read_weeklypattern(date1)
gc()
weekly2 <- read_weeklypattern(date2)
gc()
weekly3 <- read_weeklypattern(date3)
gc()

# Read in infection rate table, poi data, open hours data
case_daath_table <- read_csv("Data/LA_County_Covid19_CSA_case_death_table.csv")
testing_table <- read_csv("Data/LA_County_Covid19_CSA_testing_table.csv")
poi <- read_csv("Data/poi_extended.csv")
open_hours <- read_csv("Data/poi_hour_imputed.csv")

# Process daily and hourly visits data
daily <- daily_process(weekly1, weekly2, weekly3)
hourly <- hourly_process(weekly1, weekly2, weekly3)

# Calculate the infection rate for each community
poi <- calculate_infection_rate(poi, case_daath_table, testing_table)

# Calculate the risk scores
risk <- calculate_risk_score(poi, open_hours, daily, hourly)


# ====================================================

# ====================================================
# Output: Write the final risk scores to a csv file
# ----------------------------------------------------

# Save the file
write_csv(risk, str_c("Risk Score/risk_score_", Sys.Date(), "updated.csv"))
#write_csv(risk %>% filter(!is.na(risk_score)), str_c("Risk Score/risk_score_", Sys.Date(), "updated_NA_deleted.csv"))
# ====================================================