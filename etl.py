from pyspark.sql import SparkSession
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F 
import pyspark.sql.column as F1
import pyspark.sql.types as F2
from clean_data import *
import sql_queries as sql
import configparser
import psycopg2



def create_spark_session(config):
    '''
    create spark session an add capability to read saas data and capability to write to S3 buckets
    Creates Spark session and loads hadoop-aws.jar to work with S3'''
  
    print(config['CLUSTER']['AWS_ACCESS_KEY'])
    print(config['CLUSTER']['AWS_SECRET_KEY'])
    
    
    spark = SparkSession.builder.\
    master("local[*]").\
    config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
    .enableHiveSupport().getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['CLUSTER']['AWS_ACCESS_KEY'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['CLUSTER']['AWS_SECRET_KEY'])
    
    return spark


def readi94_data(spark,filepath):
    '''read all of i94 data for 2016. Create a dataframe and return the frame back
    spark: Spark session
    filepath: where to look for all the files
    '''    
    print(filepath)
    df_i94_data = spark.read.option("mergeSchema", "true").format('parquet').load(filepath)
    df_i94_data = clean_schema(spark, df_i94_data, False)
    df_i94_data.show()
   
   
    
    return df_i94_data


def clean_i94Data(spark, df_i94_data, df_port_of_entry_dim, df_country_dim):
    '''
     i94_data remove any record with arrdate as null, arrdate < 0, depdate < 0
     setup i94cit and i94res as Integers
    '''    
    print("i94 data size")
    print(df_i94_data.count())
 
    # remove if arrival date is null 
    df_bad_i94_data = df_i94_data.filter("arrdate <= 0").filter("depdate <= 0").filter("arrdate is null").filter("gender is null")
    print("Bad i94 data")
    df_bad_i94_data.show(100)
    
    
    df_i94_data = df_i94_data.filter("arrdate is not null and arrdate >= 0").filter("depdate >= 0").filter("gender is not null")
    # cast i94cit and i94 res as integer 
    df_i94_data = df_i94_data.withColumn("i94cit", F.col("i94cit").cast(F2.IntegerType())).withColumn("i94res", F.col("i94res").cast(F2.IntegerType())).withColumn("cicid", F.col("cicid").cast(F2.IntegerType())).withColumn("i94bir", F.col("i94bir").cast(F2.IntegerType()))
    # clean port of entries. If any port of entry code is not found in out poty of entry dim, 
    # set it to XXX
    df_i94_data.createOrReplaceTempView("i94_data_clean")
    df_port_of_entry_dim.createOrReplaceTempView("port_of_entry")
    df_country_dim.createOrReplaceTempView("countries")
    
    
    
    df_i94_data = spark.sql(sql.clean_i94_data) 

    # Add ids to our fact table
    df_i94_data = df_i94_data.withColumn("id", (F.monotonically_increasing_id() + 1))
    return df_i94_data


def setup_fact_table(spark, df_i94_data, df_port_of_entry_dim,df_countries_dim,df_states_dim, df_time_dim, df_i94_modes_dim, df_visa_categories_dim):
    '''
    main etl logic. Link i94 data and arriving airport, i94 country of residence, time and states. 
    df_i94_data -> Fact table
    df_time_dim : time dimension table
    df_states_dim : states dimension tables
    df_countries_dim : country dimension table.
    df_i94_modes_dim: i94 Modes
    df_visa_categories_dim: visa categories
    '''
    df_i94_data.createOrReplaceTempView("i94_data")
    df_port_of_entry_dim.createOrReplaceTempView("port_of_entry")
    df_countries_dim.createOrReplaceTempView("countries")
    df_states_dim.createOrReplaceTempView("states")
    df_time_dim.createOrReplaceTempView("time")
    df_i94_modes_dim.createOrReplaceTempView("i94_modes")
#cicid   i94yr  i94mon  i94cit  i94res  i94port  arrdate  i94mode  i94addr  depdate  i94bir  i94visa  count  
#dtadfile  visapost  occup  entdepa  entdepd  entdepu  matflag  biryear   dtaddto  gender  insnum  airline           
#admnum  fltno  visatype  arrival_date  departure_date   id  
    df_final_data = spark.sql(sql.create_fact_query)  
    return df_final_data


    

def create_time_dim(spark, df_i94_data):
    '''
    create time dimension frame by extrtacting columns in i94_data
    '''
    df_time_arrival_data = df_i94_data.select("arrdate").distinct().withColumnRenamed("arrdate", "i94_date")
    df_time_departure_data = df_i94_data.select("depdate").distinct().withColumnRenamed("depdate", "i94_date")
    df_time = df_time_arrival_data.union(df_time_departure_data).distinct()
    print("Negative dates")
    df_time.filter("i94_date < 0").show()
    df_time = df_time.withColumn("date", F.expr("date_add('1960-01-01', cast(i94_date as int))"))
    df_time_dim = df_time.withColumn("id", (F.monotonically_increasing_id() + 1))
    # now add month, year, quarter to time data
    df_time_dim.createOrReplaceTempView("time_dim")
    df_time_dim = spark.sql(sql.create_time_dim)
     # Add ids 
    df_time_dim = df_time_dim.withColumn ("id", (F.monotonically_increasing_id() + 1)) 
    df_time_dim.show()
    return df_time_dim



def load_csv_data(spark, filepath, delimiter=","):
    '''
    Load csv file from filepath, clean it and pass the data frame back 
    '''    
    df_frame = spark.read.format("csv").option("delimiter", delimiter).option("header", "true").option("inferschema", "true").load(filepath)
    df_frame = clean_schema(spark, df_frame)
      # Add ids 
    df_frame = df_frame.withColumn ("id", (F.monotonically_increasing_id() + 1))      
    df_frame.show(10)
    return df_frame

def setup_i94_visa_caregories_dim(spark):
    '''
    setup dimension table for i94 entry categories and return the data frame
    '''
    # i94 visa categories 
     # 	1 = 'Business'
     # 2 = 'Pleasure' #3 = 'Student';   
    visa_category_schema = StructType([StructField("visa_type", StringType(), True), StructField("visa_code", IntegerType(),True)])
    visa_categories = [("Business", 1), ("Pleasure", 2), ("Student", 3)]
    df_visa_categories = spark.createDataFrame(visa_categories, visa_category_schema)
    df_frame = df_visa_categories.withColumn ("id", (F.monotonically_increasing_id() + 1))      
    return df_frame



def setup_i94_entry_modes_dim(spark):
    '''
    setup dimension table for i94 entry modes and return the data frame
    '''
    # i94 mode 
     # 	1 = 'Air'
     # 2 = 'Sea' #3 = 'Land' #9 = 'Not reported' ;   
    entry_mode_schema = StructType([StructField("entry_type", StringType(), True), StructField("entry_type_code", IntegerType(),True)])
    entry_modes = [("Air", 1), ("Sea", 2), ("Land", 3), ("Not Reported", 9)]
    df_i94_modes = spark.createDataFrame(entry_modes, entry_mode_schema)
    df_frame = df_i94_modes.withColumn ("id", (F.monotonically_increasing_id() + 1))      
    return df_frame

def setup_states_dim(spark):
    '''
    clean and setup states data to join with immigration data
    intent of states dimension table is to check whether sum of visitors to each state calculated by using i94_addr field 
    matches that of foreigner percentage in that state
    parameter: spark: spark session
    '''    
    df_states_dim=load_csv_data(spark, "us-cities-demographics.csv", ";")
   # aggregate state's population per state and total foreign born population for each state
    df_states_dim.createOrReplaceTempView("all_states")
    # remove duplicates from the data, use city_name, state name together when eliminating duplicates.
    df_states_dim = spark.sql(sql.clean_states_dim)
    df_states_dim.createOrReplaceTempView("all_states")
    # Add % foreign population
    df_states_dim = spark.sql(sql.calculate_state_foreign_population_ration)
    df_states_dim.show()
    return df_states_dim

def run_quality_check(spark, df_final_data):
    '''
    check whether our fact table contains data
    Parameters:
    spark: Spark session
    df_final_data: Dataframe representing Fact table 
    '''
    df_final_data.createOrReplaceTempView("i94_data")
    
    print(df_final_data.schema)
    
    # run our intented queries and check whether it contains any records
    frame1 = spark.sql(sql.select_visitors_by_country)
    if(frame1.count() <= 0):
        print("QUALITY CHECK FAILED FOR VISITORS BY COUNTRY")
        raise Exception("QUALITY CHECK FAILED FOR VISITORS BY COUNTRY")
        
    frame1.show()    
  
    frame2 = spark.sql(sql.select_visitors_by_region)
    if(frame2.count() <= 0):
        print("QUALITY CHECK FAILED FOR VISITORS BY REGION")
        raise Exception("QUALITY CHECK FAILED FOR VISITORS BY REGION")
    frame2.show() 
    
    frame3 = spark.sql(sql.select_visitors_to_states)
    
    if(frame3.count() <= 0):
        print("QUALITY CHECK FAILED FOR VISITORS TO STATE")
        raise Exception("QUALITY CHECK FAILED FOR VISITORS TO STATE")
    frame3.show() 
   
    
    
    frame4 = spark.sql(sql.select_visitors_by_region_per_quarter)
    
    if(frame4.count() <= 0):
        print("QUALITY CHECK FAILED FOR VISITORS by region per quarter")
        raise Exception("QUALITY CHECK FAILED FOR VISITORS by region per quarter")
    frame4.show() 
    
    frame5 = spark.sql(sql.select_visitors_by_port_of_entry)
    if(frame5.count() <= 0):
        print("QUALITY CHECK FAILED FOR VISITORS BY PORT OF ENTRY")
        raise Exception("QUALITY CHECK FAILED FOR VISITORS BY PORT OF ENTRY")
    frame5.show() 
    
def drop_redshift_tables(cursor, connection):
    '''
    drop redshift tables
    '''
    
    for query in sql.drop_table_queries:
        print(query)
        cursor.execute(query)
        connection.commit()    
    # delete from i94 table
    cursor.execute(sql.delete_i94_data)
    connection.commit()

def create_redshift_tables(cursor, connection):
    '''
    create redshift tables
    '''
   
    for query in sql.create_table_queries:
        cursor.execute(query)
        connection.commit()
        
    
def copy_data_to_redshift(spark, df_i94_data, df_port_of_entry_dim,df_countries_dim,df_states_dim, df_time_dim, df_i94_modes, df_visa_categories_dim, config, cursor, connection):
    
    '''
    Copy data to redshift to connect with BI tools
    cursor: redshift connected cursor
    spark: Spark Session
    df_i94_data: i94 fact table data data frame
    df_port_of_entry_dim: port of entry dim data frame 
    df_countries_dim: countries dim data frame
    df_states_dim: states dim data frame
    df_time_dim: time dim data frame
    df_i94_modes: i94 modes data frame
    df_visa_categories_dim: visa categories dim data frame
    '''
    

    
    
    
    jdbc_config = "jdbc:postgresql://{}/{}?user={}&password={}".format(*config['CLUSTER'].values())
   
    print(jdbc_config)
    s3_bucket = config['S3']['SAVE_BUCKET_WITHOUT_AUTH']
    print(s3_bucket)
    df_i94_data.coalesce(1).write.mode("overwrite").format("parquet").save(s3_bucket)
    qry = """
   copy i94_data from '{}'
    credentials 'aws_iam_role={}'
    FORMAT AS PARQUET;
""".format(config['S3']['SAVE_BUCKET_S3'], config['IAM_ROLE']['ARN'])
    print(qry)
    cursor.execute(qry)
    connection.commit()
    df_port_of_entry_dim.write \
  .format("jdbc") \
  .option("url", jdbc_config) \
  .option("dbtable", "port_of_entry_dim") \
  .option("tempdir", "s3a://mohansbucket/port_of_entry_dim") \
  .save()
    
    
    
   
    
    
    df_countries_dim.write \
  .format("jdbc") \
  .option("url", jdbc_config) \
  .option("dbtable", "country_dim") \
  .option("tempdir", "s3a://mohansbucket/country_dim") \
  .save()
    
    df_states_dim.write \
  .format("jdbc") \
  .option("url", jdbc_config) \
  .option("dbtable", "states_dim") \
  .option("tempdir", "s3a://mohansbucket/states_dim") \
  .save()
    
    df_time_dim.write \
  .format("jdbc") \
  .option("url", jdbc_config) \
  .option("dbtable", "i94_time_dim") \
  .option("tempdir", "s3a://mohansbucket/i94_time_dim") \
  .save()

    df_i94_modes.write \
  .format("jdbc") \
  .option("url", jdbc_config) \
  .option("dbtable", "i94_enter_modes_dim") \
  .option("tempdir", "s3a://mohansbucket/i94_enter_modes_dim") \
  .save()
    
    df_visa_categories_dim.write \
  .format("jdbc") \
  .option("url", jdbc_config) \
  .option("dbtable", "visa_categories_dim") \
  .option("tempdir", "s3a://mohansbucket/visa_categories_dim") \
  .save()
   
    
    
    
#    df_i94_data.write \
#  .format("jdbc") \
#  .option("url", jdbc_config) \
#  .option("dbtable", "i94_data") \
#  .option("tempdir", "s3a://mohansbucket/i94_data") \
#  .save()
    



def main():
    config = configparser.ConfigParser()
    
    config.read('dwh.cfg')
    os.environ['AWS_ACCESS_KEY_ID']=config['CLUSTER']['AWS_ACCESS_KEY']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['CLUSTER']['AWS_SECRET_KEY']

    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    drop_redshift_tables(cur, conn)
    #create_redshift_tables(cur, conn)
    
    spark = create_spark_session(config)
   
    
    df_i94_data = readi94_data(spark, './i94_data/')
    
    df_states_dim = setup_states_dim(spark)
        # read in port of entry codes
    df_port_of_entry_dim = load_csv_data(spark, "port_of_entry_codes.csv", ",")
    
    # create visa_type_dimension
    df_visa_type_dim = df_i94_data.select("visatype").distinct()
    
    # Read all the country codes from labelled country_codes.csv file
    df_countries_dim = load_csv_data(spark, "Country_codes.csv", ",")
      # Select distinct country names
    df_countries_dim.createOrReplaceTempView("all_countries")
  
    df_countries_dim = spark.sql(sql.clean_country_data)
   
    
    # read in port of entry codes
    df_port_of_entry_dim = load_csv_data(spark, "port_of_entry_codes.csv")
      # Add ids to states dim
    df_states_dim = df_states_dim.withColumn ("id", (F.monotonically_increasing_id() + 1))
    
    df_i94_modes = setup_i94_entry_modes_dim(spark)
    df_visa_categories_dim = setup_i94_visa_caregories_dim(spark)
      # clean i94_data
    df_i94_data = clean_i94Data(spark, df_i94_data, df_port_of_entry_dim, df_countries_dim)
    # time dimension tables
    df_time_dim = create_time_dim(spark, df_i94_data)
   
   
    
   
    df_final_data = setup_fact_table(spark, df_i94_data, df_port_of_entry_dim,df_countries_dim,df_states_dim, df_time_dim, df_i94_modes, df_visa_categories_dim)
    print(df_final_data.schema)
#    copy to redshift
    copy_data_to_redshift(spark, df_final_data, df_port_of_entry_dim,df_countries_dim,df_states_dim, df_time_dim, df_i94_modes, df_visa_categories_dim, config, cur, conn)
    # run checks 
    run_quality_check(spark, df_final_data)
    
    

if __name__ == "__main__":
    main()

