select_visitors_by_country = "select  c1.country_name, c1.region, count(*) as visitor_count from i94_data join countries c1 on i94_data.country_of_residence_id = c1.id group by c1.country_name, c1.region order by count(*) desc"

select_visitors_by_region="select c1.region, count(*) as visitor_count from i94_data join countries c1 on i94_data.country_of_residence_id = c1.id group by c1.region order by count(*) desc"

select_visitors_to_states = "select  s1.state, s1.foreign_population, s2.visitor_count from (Select s1.STATE, count(*) as visitor_count from i94_data join states s1 on i94_data.destination_state_id = s1.id group by s1.STATE order by count(*) desc) s2 join all_states s1 on s1.state = s2.state order by s2.visitor_count desc"

select_visitors_by_region_per_quarter = "select c1.region, t1.quarter, count(*) as visitor_count from i94_data join countries c1 on i94_data.country_of_residence_id = c1.id join time t1 on t1.id = i94_data.arrival_date_id group by c1.region, t1.quarter"

select_visitors_by_port_of_entry = "select p1.location,  count(*) as visitor_count from i94_data join port_of_entry p1 on i94_data.port_of_entry_id = p1.id join time t1 on t1.id = i94_data.arrival_date_id group by p1.location, t1.quarter order by 2 desc"

clean_states_dim = "select State, state_code, sum(Total_population) as state_population, sum(Foreign_born) as foreign_population from (select trim(State) as state,city, Total_population, Foreign_born, trim(State_code) as state_code from all_states group by state, city, Total_population, Foreign_born, State_code) s1 group by State, state_code"

calculate_state_foreign_population_ration = "select State, state_code as state_province_code, state_population, foreign_population, foreign_population*100.0/state_population as percentage_foreign_population from all_states"

create_time_dim = "select (monotonically_increasing_id() + 1) as id, i94_date, month(date) as month, year(date) as year, dayofmonth(date) as day, quarter(date) as quarter from time_dim"

create_fact_query = "select i94.id, i94.cicid, cast(i94.biryear as numeric) as visitor_birth_year, i94.gender, i94_modes.id as i94_entry_mode_id, i94.insnum, i94.airline, cast (i94.admnum as numeric), i94.fltno,\
    i94.dtadfile, i94.i94visa, case when i94.visapost is null then 'Unknown' else i94.visapost end visapost_1, case when i94.occup is null then 'Unknown' else i94.occup end occup_1, i94.entdepa, i94.entdepd,  i94.matflag, t1.id as arrival_date_id,\
    t2.id as departure_date_id, c1.id as country_of_citizenship_id, c2.id as country_of_residence_id, \
    p1.id as port_of_entry_id,\
    s1.id as destination_state_id from i94_data i94 left join countries c1 on int(i94.i94res_cleaned)=int(c1.country_code) left join countries c2 \
    on i94.i94cit_cleaned=trim(c2.country_code) left join states s1 on i94.i94addr = s1.state_province_code left join time t1 on i94.arrdate=t1.i94_date \
    left join time t2 on i94.depdate=t2.i94_date left join port_of_entry p1 on p1.code=i94.i94port_cleaned left join i94_modes on i94_modes.entry_type_code = i94.I94MODE"

clean_country_data = "select id, trim(Region) as Region, trim(Country_Code) as Country_code, trim(Country_Name) as Country_Name from (select *, row_number() over (partition by Country_Name order by id) as row_number from all_countries) as rows where row_number = 1"

clean_i94_data = "select i94.cicid,i94.i94yr,i94.i94mon,case when c1.country_code is null then 9999 else c1.country_code end i94cit_cleaned, case when c2.country_code is null then 9999 else i94.i94res end i94res_cleaned, case when p1.code is null then 'xxx' else i94.i94port end i94port_cleaned, i94.i94port,i94.arrdate,i94.i94mode,i94.i94addr,i94.depdate,i94.i94bir,i94.i94visa,i94.count,i94.dtadfile,i94.visapost,i94.occup,i94.entdepa,i94.entdepd,i94.entdepu,i94.matflag,i94.biryear,i94.dtaddto,i94.gender,i94.insnum,i94.airline,i94.admnum,i94.fltno,i94.visatype from i94_data_clean i94 left join port_of_entry p1 on i94.i94port=p1.code left join countries c1 on i94.i94cit = c1.Country_Code left join countries c2 on i94.i94res = c2.country_code"

create_i94_fact_table = "create table if not exists i94_data(id bigint PRIMARY KEY, cicid numeric, visitor_birth_year numeric, gender varchar(10), i94_entry_mode_id bigint, insnum varchar(32), airline text, admnum numeric, fltno text, i94visa float8, visapost_1 varchar(256), occup_1 varchar(256),dtadfile text, entdepa text, entdepd text, matflag text, arrival_date_id bigint, country_of_citizenship_id bigint , country_of_residence_id bigint, port_of_entry_id bigint, destination_state_id bigint, departure_date_id bigint)" 

create_states_dim_table = "create table if not exists states_dim(id bigint PRIMARY KEY, state_province_code varchar(4), state_name varchar(32), total_population numeric, foreign_population numeric, percentage_foreign_population decimal (10, 2))"

create_port_of_entry_dim_table = "create table if not exists port_of_entry_dim(id bigint PRIMARY KEY, code varchar(16), location varchar(16), state varchar(16))"

create_time_dim_table = "create table if not exists time_dim(id bigint PRIMARY KEY, date DATE, month numeric, quarter numeric, day numeric)"  

create_country_dim_table = "create table if not exists country_dim(id bigint PRIMARY KEY, Region varchar(16), Country_Code numeric, Country_Name varchar(32))"

create_i94_modes_dim = "create table if not exists i94_modes_dim (id bigint PRIMARY KEY, visa_type varchar(16), visa_code numeric)"

create_table_queries = [create_i94_fact_table]

drop_table_queries = [ "drop table if exists i94_time_dim", "drop table if exists i94_enter_modes_dim", "drop table if exists visa_categories_dim", "drop table if exists states_dim", "drop table if exists country_dim", "drop table if exists port_of_entry_dim"]
delete_i94_data = "delete from i94_data"