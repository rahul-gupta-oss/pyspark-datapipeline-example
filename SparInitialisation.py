##Initiate spark session
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Helloworld").getOrCreate()

##Intsall psycopg2 to connect to postgresql
##import data as pandas data frame and conver it into spark dataframe

import pandas as pd
import psycopg2
con=psycopg2.connect(host="localhost",
                     database="Trainning",
                    user="postgres",
                    password="postgres",
                    port=5432)
cur=con.cursor()
df1=pd.read_sql("select * from disease1",con)
sdf=spark.createDataFrame(df1)
sdf.printSchema()