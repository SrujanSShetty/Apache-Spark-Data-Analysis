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
         .appName("SF Fire Calls Analysis")
         .getOrCreate())
    
    #Get the SF fire calls dataset path from the Command line.
    file_path = sys.argv[1]
    
    #Reading the file into Spark DataFrame using the inferred method having CSV format
    df = (spark.read
          .option("header","true")
          .option("inferSchema","true")
          .csv(file_path))
    
    #Convert CallDate Column to Date format
    df= df.withColumn("CallDate", to_date(col("CallDate"),"MM/dd/yyyy"))
    
    # Drop rows with null CallDate
    df = df.dropna(subset=["CallDate"])
    
    #Answering all 7 questions in order
    
    #1.What were all the different types of fire calls in 2018?
    print("\n 1.What were all the different types of fire calls in 2018?:")
    fire_calltype_2018 = df.filter(year("CallDate")== 2018).select("CallType").distinct()
    fire_calltype_2018.show()
    
    
    #2.What months within the year 2018 saw the highest number of fire calls?
    print("\n 2.What months within the year 2018 saw the highest number of fire calls?:")
    highest_firecalls_by_monthof2018 = df.filter(year("CallDate")==2018).groupBy(month("CallDate").alias("Month")).count().orderBy(col("count").desc())
    highest_firecalls_by_monthof2018.show()
    
                     
    #3.Which neighborhood in San Francisco generated the most fire calls in 2018?
    print("\n 3.Which neighborhood in San Francisco generated the most fire calls in 2018?:")
    highest_firecalls_by_neighborhood2018 = df.filter(year("CallDate")==2018).groupBy("Neighborhood").count().orderBy(col("count").desc())
    highest_firecalls_by_neighborhood2018.show()
    
    
    #4.Which neighborhoods had the worst response times to fire calls in 2018?
    print("\n 4.Which neighborhoods had the worst response times to fire calls in 2018?:")
    worst_responsetime_neighborhood2018 = df.filter(year("CallDate")==2018).groupBy("Neighborhood").agg(avg("Delay").alias("Avg_response_time")).orderBy(col("Avg_response_time").desc())
    worst_responsetime_neighborhood2018.show()
    
    
    #5.Which week in the year in 2018 had the most fire calls?
    print("\n 5.Which week in the year in 2018 had the most fire calls?:")
    weekintheyear2018_most_firecalls = df.filter(year("CallDate")==2018).groupBy(weekofyear("CallDate").alias("Week")).count().orderBy(col("count").desc())
    weekintheyear2018_most_firecalls.show()
    
    
    #6.Is there a correlation between neighborhood, zip code, and number of fire calls?
    print("\n 6.Is there a correlation between neighborhood, zip code, and number of fire calls?:")
    correlation = df.filter(year("CallDate")==2018).groupBy("Neighborhood","Zipcode").count().orderBy(col("count").desc())
    correlation.show(10)#Top 10 count in descending
    
    
    #7.How can we use Parquet files or SQL tables to store this data and read it back?
    print("\n 7. Storing and reading data using Parquet:")
    df.write.format("parquet").mode("overwrite").save("sf-fire-calls.parquet")
    parquet_df=spark.read.parquet("sf-fire-calls.parquet")
    parquet_df.show(6)
    
    #Stop Spark Session
    spark.stop()
    