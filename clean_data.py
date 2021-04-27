from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as sqlFunctions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

import pyspark.sql.column as F1

'''
Parse dataFrame's schema and update the schema to contain no spaces. Strips of leading and trailing spaces. If there is a space or -
in between words, uses _ to replace them. 
'''
def clean_schema(spark, dataFrame, cleanna=True):
   # Remove any space in schema names
    if(cleanna):
        dataFrame = dataFrame.na.drop()
    newSchema = []
    for s in dataFrame.schema:
        name=s.name.strip().replace(" ", "_")
        name=name.replace("-", "_")
        newSchema.append(StructField(name, s.dataType, s.nullable, s.metadata))
        
    newFrame = spark.createDataFrame(dataFrame.rdd, StructType(newSchema))
    return newFrame

 
