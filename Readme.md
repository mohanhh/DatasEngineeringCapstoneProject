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

##### Rationale for the choice of tools and technologies for the project:
I am choosing Spark to load and analyze the data. Spark is a popular opensource Analytical Tool supported on all major cloud platforms. Spark partitions the data and assigns it to individual worker nodes.  Any growth in data could be easily handled by adding more executors to Spark cluster. Spark also has in buit SQL Engine which makes it easy to analyze the data using familiar SQL syntax. After analysis, the data is being saved in Redshift.  Users can use variety of front end tools like Tableu or Power BI to generate reports and graphs from this data. 
#### Propose how often the data should be updated and why.
I propose that the data could be updated monthly. This will allow one to analyze visitor data to the country on a monthly and quarterly basis. 

#### Write a description of how you would approach the problem differently under the following scenarios:
#### The data was increased by 100x : 
I would still use Spark to analyze the data. But perhaps we can reduce the time between updates to weekly to reduce time taken to analyze the data. As the final data is saved in Redshift which can handle peta bytes of data, the solution would still work. 

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
I would propose that we use Airflow to schedule following jobs
a.) Kick off Cloud formation scripts to create an EMR cluster
b.) Add Spark as one of the custom step in the EMR and run the code
c.) Once the Spark job ends the data is available in Redshift, now kick off job to update the Dashboard.



#### The database needed to be accessed by 100+ people.
Data is being stored in Redshift, which can easily scale to handle 100+ people accessing the data. We can always add more compute nodes to allow simultaneous access to more people





