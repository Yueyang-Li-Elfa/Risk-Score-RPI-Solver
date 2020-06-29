# Pipeline: Generate Risk Scores

# Before you run the script
# please make sure to set the [Working Directory] as where the script is located

# How to use this script
# 1. Clone the whole repository from GitHub
# 2. Download the latest 3 weeks [Weekly Places Patterns v2] data from SafeGraph, store in the [Data] folder, and unzip the files
# 3. Download COVID-19 data from: http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/
#    Click [City/Community Table], then click [Download this table]
#    Store this csv file in the [Data] folder
# 4. Set up the three dates in [line 24-26] as "mm-dd" format
# 5. Run through the scirpt. The result will be stored in the [Risk Score] folder.

library(readr)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(sjmisc)

# ====================================================
# Weekly Pattern: Subset POIs in LA 
# Estimated runtime: 45 mins
# ----------------------------------------------------

date1 <- "06-01"
date2 <- "06-08"
date3 <- "06-15"

# Week1
weekly1 <- read_csv(str_c("Data/2020-", date1, "-weekly-patterns.csv"))
weekly1_LA <- weekly1 %>%
  filter(region == "CA", city == "Los Angeles")
rm(weekly1)

# Week2
weekly2 <- read_csv(str_c("Data/2020-", date2, "-weekly-patterns.csv"))
weekly2_LA <- weekly2 %>%
  filter(region == "CA", city == "Los Angeles")
rm(weekly2)

# Week3
weekly3 <- read_csv(str_c("Data/2020-", date3, "-weekly-patterns.csv"))
weekly3_LA <- weekly3 %>%
  filter(region == "CA", city == "Los Angeles")
rm(weekly3)
write_csv(weekly3_LA, "Data/weekly_0615_LA.csv")

# ====================================================

# ====================================================
# Weekly Pattern: Parse the data and take average for last three weeks
# Estimated runtime: 10 mins
# ----------------------------------------------------

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

daily <- get_daily_visits(weekly1_LA) %>%
  # bind three datasets together
  rbind(get_daily_visits(weekly2_LA)) %>%
  rbind(get_daily_visits(weekly3_LA)) %>%
  # take average of last three weeks
  group_by(safegraph_place_id, weekday) %>%
  summarize(avg_visits = mean(visits_by_day, na.rm = T),
            avg_median_dwell = mean(median_dwell, na.rm = T))

hourly <- get_hourly_visits(weekly1_LA) %>%
  # bind 3 datasets together
  rbind(get_hourly_visits(weekly2_LA)) %>%
  rbind(get_hourly_visits(weekly3_LA)) %>%
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

# ====================================================

# ====================================================
# Infection Rate: Calculate the infection rate for each community
# Estimated runtime: 15 mins
# ----------------------------------------------------

# Calculate infection rate for each community
rate_original <- read_csv("Data/city_community_table.csv")
rate <- rate_original %>%
  mutate(infection_rate = (cases_final-deaths_final)/(persons_tested_final-deaths_final)) %>%
  select(geo_merge, infection_rate)

# Read in poi data
poi <- read_csv("Data/poi_extended.csv")

# Add infection rate for each poi
poi$infection_rate <- 0

for (i in 1:nrow(poi)) {
  city=poi$city[i]
  comm=poi$community[i]
  for (j in 1:nrow(rate)){
    if(str_contains(rate$geo_merge[j], comm,ignore.case = TRUE)==TRUE) {
      poi$infection_rate[i]=rate$infection_rate[j]
    }
    else if(str_contains(rate$geo_merge[j], city,ignore.case = TRUE)==TRUE & city!="Los Angeles") {
      poi$infection_rate[i]=rate$infection_rate[j]
    }
  }
}

# Impute rest pois (those that can't find matches) with the infection rate of LA city
rate_LA <- rate_original %>%
  filter(str_detect(geo_merge, "^Los Angeles")) %>%
  summarise(cases_final = sum(cases_final),
            deaths_final = sum(deaths_final),
            persons_tested_final = sum(persons_tested_final)) %>%
  mutate(infection_rate = (cases_final-deaths_final)/(persons_tested_final-deaths_final))

rate_LA <- rate_LA$infection_rate

poi <- poi %>%
  mutate(infection_rate = ifelse(infection_rate == 0, rate_LA, infection_rate))

# ====================================================

# ====================================================
# Risk Score: Joining the data, and calculate the risk scores
# ----------------------------------------------------

# Read in data: open hours
open_hours <- read_csv("Data/poi_hour_imputed.csv")

# Joining the data
# (will be modified after we include the infection rate data in this script)
risk <- open_hours %>%
  # Join the poi data
  left_join(poi %>% select(safegraph_place_id, location_name, top_category, latitude, longitude, street_address, area_square_feet, infection_rate), 
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

# ====================================================

# ====================================================
# Output: Write the final risk scores to a csv file
# ----------------------------------------------------

# Reorder columns
risk <- risk %>%
  select(safegraph_place_id, location_name, top_category, latitude, longitude, street_address, everything())

# Save the file
write_csv(risk, str_c("Risk Score/risk_score_", Sys.Date(), "updated.csv"))

# ====================================================