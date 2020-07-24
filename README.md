# Risk-Score-RPI-Solver
Solution for the COVID-19 Challenge co-hosted by the City of Los Angeles and RMDS Lab. Publish risk scores for over 30,000 locations in the City of Los Angeles and nearby commmunities. Keep updating.

Currently using risk_score_pipeline_function.r to update risk scores.
(The r package "COVIDrisk" is on the way)

Before you run the pipeline script, please make sure to set the [Working Directory] as where the script is located

# How to use this script
 1. Clone the whole repository from GitHub
 2. Download the latest 3 weeks [Weekly Places Patterns v2] data from SafeGraph, store in the [Data] folder, and unzip all files
 3. Download COVID-19 data from: http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/
    Click [Table: Community Case/Death], then click [Download this table]
    Click [Table: Community Testing], then click [Download this table]
    Store these two csv files in the [Data] folder
 4. Set up date1, date2, date3 as "mm-dd" format
 5. Run through the scirpt. The result will be stored in the [Risk Score] folder.

Link to the dashboard: https://risk-score-la.herokuapp.com
