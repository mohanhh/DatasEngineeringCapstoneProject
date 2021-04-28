# Data Engineering Capstone Project

### Project Summary
This project analyzes visitor information to United States from i94 immigration data for 1996. This i94 immigration 
data is enhanced using Country codes which also conmtains the region the conuntry belongs to and US state province 
data. US state province data contains total population of state as well as total foreigner population in each state. 
The data will be arranged in fact and dimension tables with the fact table being i94 immigration data and dimensions 
being countries visitors came from, their intended destination state, date of arrival and US port of entry.

#### Data Set
 i94_data : i94 Visitor Data provided Udacity in paraquet format 
 Country_codes.csv: Country Codes and country names extracted from i94 Visitor data dictionary, enhanced by adding region
 us-cities-demographics.csv: This data comes from OpenSoft. Provided by UDACITY
 port_of_entry_codes.csv: Airport Code Table. This is a  table of airport codes, corresponding cities and states

### Project Scope
#### The data is organized to answer following queries.
1.) Count of visitors from each country in 1996
2.) Count of visitors from each country each quarter of 1996
3.) Count of visitors from each region,  per quarter or per month
4.) Count of visitors to each state and comparison of visitors to percentage of foreigners in the state
5.) Count of visitors entering various port of entries in US
6.) 

Spark is chosen to analyze the data. Spark is well suited to analyze big data sets and is extremely scalable. It includes 
SQL Engine Spark-SQL and a query optimizer in catalyst optimizer. 

### Data is cleaned, analyzed and copied to AWS Redshift to run further analysis.

### Cleaning Steps
Data Frame schema is updated to remove spaces. Each data set is analyzed for duplicate values using SQL Queries. For i94 data,rows which have arrdate and gender are null are removed. i94cit, i94res, cicid, i94bir are cast to integers. Data in us-cities-demographics file are cleaned and aggregated per state to have approximate  population of each state, approximate number of foreigners in each state and percentage of foreigners in each state.



![Entity Relationship Diagram](i94DW.png)

#### Loading Data
i94_visitor data is loaded through parquet files. These files were originally in SAS format. They have been converted 
and kept in i94_data folder
Country_codes Country Codes and country names extracted from i94 Visitor data dictionary, enhanced by adding region saved as csv file. 
us-cities-demographics.csv: us-cities-demographics loaded as csv file.
port_of_entry_codes.csv: Airport Code Table. This is a simple table of airport codes, corresponding cities and states, loaded as csv file.







