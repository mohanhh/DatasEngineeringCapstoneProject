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
 port_of_entry_codes.csv: Airport Code Table. This is a simple table of airport codes, corresponding cities and states

### Project Scope
#### The data is organized to answer following queries.
1.) Count of visitors from each country in 1996
2.) Count of visitors from each country each quarter of 1996
3.) Count of visitors from each region, each region per quarter or per month
4.) Count of visitors to each state
5.) Count of visitors entering various port of entries in US

Spark is chosen to analyze the data. Spark is well suited to analyze big data sets and is extremely scalable. It includes 
SQL Engine Spark-SQL and a query optimizer in catalyst optimizer. 

### Cleaning Steps
Data Frame schema is updated to remove spaces. Each data set is analyzed for duplicate values using SQL Queries.  

### Dimension Tables
##### Countries:  List of countries with id, name of the country, region it belongs to and country code extracted from 
#### I94_SAS_Labels_Descriptions.SAS
#### time: Time dimension, created by converting arrival and departure dates in i94 immigration date and enriched by 
#### adding month, year, day and quarter for each entry. id generated by Spark is the surrogate key.
#### states: Information about each state, name of the state, state_province_code, total population of the state, total 
#### foreign born population and foreign born population as the percentage of total population of state. 
#### port_of_entry : Dimension table to track which airport visitors use to enter US. Has code, location and state
#### i94_

### Fact Table: 
#### i94_data
###### The main fact table contains following fields
###### id : Surrogate id generated by Spark
###### cicid: Degenerative Dimension from i94 data
###### visitor_birth_year,
###### gender : Visitor gender    
###### i94mode_id : i94 modes  Land, Air, Sea, link to i94_modes dimension
###### insnum : INS Assigned number
###### airline : Airline visitor came in
###### admnum : Visitor admission number 
###### fltno : Visitor Flight number 
###### dtadfile
###### i94visa : Type of visa
###### visapost_1 : Consulate/Embassy issuing visa
###### occup : Occupation as declared by visitor
###### entdepa : 
###### entdepd,
###### entdepu,
###### matflag,
###### arrival_date_id : Arrival date linking to time dimensionn
###### departure_date_id : Departure date id linking to time dimension 
###### country_of_citizenship_id : Link to Country dimension
###### country_of_residence_id : Link to country dimension
###### port_of_entry_id : Link to port of entry dimension
###### destination_state_id : Link to state dimension

![Entity Relationship Diagram](i94DW.png)

#### Loading Data
i94_visitor data is loaded through parquet files. These files were originally in SAS format. They have been converted 
and kept in i94_data folder
Country_codes Country Codes and country names extracted from i94 Visitor data dictionary, enhanced by adding region saved as csv file. 
us-cities-demographics.csv: us-cities-demographics loaded as csv file.
port_of_entry_codes.csv: Airport Code Table. This is a simple table of airport codes, corresponding cities and states, loaded as csv file.







