

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys
APP_NAME = "data_clear"

def main(spark,filenm):
    dframe=spark.read.csv(filenm,header=True);
    data_Req=dframe.select('#DATE','BOROUGH','ZIP CODE','NUMBER OF PERSONS INJURED','NUMBER OF PERSONS KILLED','NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED','NUMBER OF CYCLIST INJURED','NUMBER OF CYCLIST KILLED','NUMBER OF MOTORIST INJURED','NUMBER OF MOTORIST KILLED','VEHICLE TYPE CODE 1')
    data_final=data_Req.filter(col("#DATE").isNotNull() & col("BOROUGH").isNotNull() & col("ZIP CODE").isNotNull() & col("NUMBER OF PERSONS INJURED").isNotNull() & col('NUMBER OF PERSONS KILLED').isNotNull() & col("NUMBER OF PEDESTRIANS INJURED").isNotNull() & col("NUMBER OF PEDESTRIANS KILLED").isNotNull() & col("NUMBER OF CYCLIST INJURED").isNotNull() & col("NUMBER OF CYCLIST KILLED").isNotNull() & col("NUMBER OF MOTORIST INJURED").isNotNull() & col("NUMBER OF MOTORIST KILLED").isNotNull() & col("VEHICLE TYPE CODE 1").isNotNull())
    data_final.write.format("csv").save("/user/samparma/SparkCleanData")
    
    

if __name__ == "__main__":
   # Configure Spark
   spark=SparkSession.builder.master("local").appName(APP_NAME).getOrCreate()
   filenm = sys.argv[1]
   # Execute Main functionality
   main(spark, filenm)
