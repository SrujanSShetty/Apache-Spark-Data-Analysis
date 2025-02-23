

#Overview

This project demonstrates the use of Apache Spark SQL and PySpark DataFrame APIs to process flight delay data. The dataset used is departuredelays.csv, and operations include SQL queries, temp views, schema enforcement, and data storage in multiple formats.

##Features
	•	Spark SQL Queries: Conversion of SQL queries into DataFrame APIs.
	•	Temp Views & Catalog: Creating temporary views and querying Spark catalog.
	•	Data Transformation: Applying schema, filtering, and transforming data.
	•	Data Storage: Writing outputs as JSON, LZ4-compressed JSON, and Parquet.
	•	ORD Flight Extraction: Filtering flights originating from Chicago O’Hare (ORD).

##Setup & Execution
	1.	Clone the Repository:

        git clone 


	2.	Run the Script:

        spark-submit --conf spark.sql.catalogImplementation=hive FlightDelayAnalysis.py ./departuredelays.csv


	3.	Output Files:
	•	output/departuredelays.json
	•	output/departuredelays_lz4.json
	•	output/departuredelays.parquet
	•	output/orddeparturedelays.parquet

##Technologies Used
	•	Apache Spark
	•	PySpark
	•	Hadoop (Hive Catalog)
	•	Parquet, JSON Storage

##Author

Srujan Shekar Shetty 🚀