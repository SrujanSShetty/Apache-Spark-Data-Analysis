import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, month, weekofyear, avg, to_date

if __name__=="__main__":
    if len(sys.argv)!=2:
        print("Usage: module-05:py <file>", file=sys.stderr)
        sys.exit(-1)

    #Building the Spark Session using the SparkSession APIs
    spark = (SparkSession
         .builder
         .appname("SF Fire Calls Analysis")
         .getOrCreate())
    
    #Get the SF fire calls dataset path from the Command line.
    file_path = sys.argv[1]
    
    #Reading the file into Spark DataFrame using the inferred method having CSV format
    df = (spark.read.csv("csv")
          .option("header","true")
          .option("inferSchema","true")
          .load(file_path))
    
    #Convert CallDate Column to Date format
    df=df.withcolumn("CallDate", to_date(col("CallDate","MM/dd/yyyy")))