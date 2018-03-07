./bin/spark-shell

//spark datasets
case class Airport (ident:String, category:String, name:String, latitude_deg:Double, longitude_deg:Double,elevation_ft:Integer, continent:String, iso_country:String, iso_region:String, municipality:String,gps_code:String, iata_code:String, local_code:String)

val airports = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/airport_codes.csv").as[Airport]

val airportsCA = airports.filter(_.iso_region == "US-CA")
airportsCA.show(5)

val heliportsCA = airports.filter(_.iso_region == "US-CA").filter(_.category == "heliport").filter(_.elevation_ft > 1000)
heliportsCA.show(5)

airports.CA.persist

//--------------- spark dataframe


case class Airport (ident:String, category:String, name:String, latitude_deg:Double, longitude_deg:Double,elevation_ft:Integer, continent:String, iso_country:String, iso_region:String, municipality:String,gps_code:String, iata_code:String, local_code:String)

val airportsDF = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/airport_codes.csv").as[Airport].toDF
airportsDF.show(5)
val airportsDF1 = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/airport_codes.csv")
airportsDF1.show(5)

val airportsCA = airportsDF.filter($"iso_region" === "US-CA")
airportsCA.show(5)

val heliportsCA = airportsDF.filter($"iso_region" === "US-CA").filter($"category" === "heliport").filter($"elevation_ft" > 1000)
heliportsCA.show(5)

val highAirportsCA = airportsDF.filter($"iso_region" === "US-CA").groupBy($"iso_region").avg("elevation_ft")
highAirportsCA.show

val heliportsCAProjection = airportsDF.select($"ident", $"name").filter($"iso_region" === "US-CA").filter($"category" === "heliport").filter($"elevation_ft" > 1000)
heliportsCAProjection.show(5)

val otherAirportData = spark.read.json("file:///root/data/airports.jsonl")
//val otherAirportData = spark.read.json("data/airports.jsonl")
otherAirportData.printSchema

val joinedAirportData = airportsDF.join(otherAirportData, airportsDF("name") === otherAirportData("name")).select($"ident",airportsDF("name"),$"tz")
joinedAirportData.show(5)

airportsDF.createOrReplaceTempView("airports")
otherAirportData.createOrReplaceTempView("other_airports")

spark.sql("SELECT * FROM airports WHERE iso_region = 'US-CA'").show(3)
spark.sql("SELECT a.ident, a.name, o.tz FROM airports a, other_airports o WHERE a.name = o.name").show(5)
spark.sql("SELECT AVG(elevation_ft) FROM airports WHERE iso_region = 'US-CA'").show

//----
val suppliersDF = spark.read.option("sep", "|").csv("/data/supplier/suppliers.csv")
val rows = spark.read.option("multiLine", true).json("/data/rows.json")
//look into parquet files looks like python tool are pyarrow and fastparquet for reads in pandas
// also parquet-python
//https://arrow.apache.org/docs/python/parquet.html

//jdbc and spark
//bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
import java.util.Properties
val connectionProperties = new Properties()
connectionProperties.put("user", "jmategko")
connectionProperties.put("password", "password123")
val jdbcDF = spark.read.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

//Hive
spark.sql("SELECT id, date, price from home_data limit 10").show
spark.sql("show tables").show()
// myDf.createOrReplaceTempView("mytempTable") 
// sqlContext.sql("create table mytable as select * from mytempTable");

//val homedf = homedataframe.withColumn("Date", (col("Date").cast("date")))
//https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/Dataset.html
//https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/Row.html
//https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/RelationalGroupedDataset.html
val homedataframe = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/home_data.csv")
homedataframe.show(5)

//How many houses sold were built prior to 1979?
val before1979 = homedataframe.filter($"yr_built" < 1979).count
print(before1979)
// 11991

//The most expensive zipcode in the data set, defined as highest average sales price?
val zipcode_avg_sale = homedataframe.groupBy($"zipcode").avg("price")
zipcode_avg_sale.show(5)
val max_avg_priceDF = zipcode_avg_sale.agg(max("avg(price)"))
val firstrow = max_avg_priceDF.head()
val maxvalue = firstrow(0)
val most_expensive_zipcode = zipcode_avg_sale.filter($"avg(price)" === maxvalue)
most_expensive_zipcode.show(5)
/*
+-------+----------+
|zipcode|avg(price)|
+-------+----------+
|  98039| 2160606.6|
+-------+----------+
*/

//Number of unique zipcodes in data set?
val zipcodes = homedataframe.groupBy($"zipcode").count()
val zipcodeCount = zipcodes.count()
print(zipcodeCount)
//70

//Drop the “sqft_living15” and “sqft_lot15” columns from dataset.
val dfdrop = homedataframe.drop("sqft_living15", "sqft_lot15")
dfdrop.printSchema
/* 
root
 |-- id: long (nullable = true)
 |-- date: string (nullable = true)
 |-- price: integer (nullable = true)
 |-- bedrooms: integer (nullable = true)
 |-- bathrooms: double (nullable = true)
 |-- sqft_living: integer (nullable = true)
 |-- sqft_lot: integer (nullable = true)
 |-- floors: double (nullable = true)
 |-- waterfront: integer (nullable = true)
 |-- view: integer (nullable = true)
 |-- condition: integer (nullable = true)
 |-- grade: integer (nullable = true)
 |-- sqft_above: integer (nullable = true)
 |-- sqft_basement: integer (nullable = true)
 |-- yr_built: integer (nullable = true)
 |-- yr_renovated: integer (nullable = true)
 |-- zipcode: integer (nullable = true)
 |-- lat: double (nullable = true)
 |-- long: double (nullable = true)
*/

//Access the zipcode table stored in Hive and join that data with a DataFrame created from home_data.csv.

spark.sql("SELECT id, date, price from home_data limit 10").show
spark.sql("SELECT * from wa_zipcodes limit 10").show
val wa_zipcodes = spark.sql("SELECT * from wa_zipcodes")
val joinedHomeZipData = homedataframe.join(wa_zipcodes, homedataframe("zipcode") === wa_zipcodes("zipcode")).select($"id", $"date", $"price", wa_zipcodes("zipcode"), wa_zipcodes("city"))
joinedHomeZipData.show(5)
/*
+----------+---------------+------+-------+---------+
|        id|           date| price|zipcode|     city|
+----------+---------------+------+-------+---------+
|7129300520|20141013T000000|221900|  98178|  Seattle|
|6414100192|20141209T000000|538000|  98125|  Seattle|
|5631500400|20150225T000000|180000|  98028|  Kenmore|
|2487200875|20141209T000000|604000|  98136|  Seattle|
|1954400510|20150218T000000|510000|  98074|Sammamish|
+----------+---------------+------+-------+---------+
*/

//project time
val littlestocks = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/WIKI-PRICES_10k.csv")
val stocks = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/default_stock_codes_data.csv")
val bigstocks = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/stocks/WIKI_PRICES_212b326a081eacca455e13140d7bb9db.csv")
//count=15,343,013

// avg open close report
//mvp test with just dataframe
//after 51
val withdates = bigstocks.withColumn("year_month", date_format(col("date"), "YYYY-MM")).filter($"date" >= "2017-01-01"and $"date" <= "2017-06-30").filter(($"ticker" isin ("GOOGL","MSFT","COF")))
val openavg = withdates.groupBy($"ticker", $"year_month").avg("open", "close").orderBy($"ticker", $"year_month")
//MBP test with sql
//After 79
bigstocks.createOrReplaceTempView("bigstocks2")
val openavgsql = spark.sql("SELECT ticker, TRUNC(date, 'MM'), AVG(open), AVG(close) FROM bigstocks2 WHERE ticker IN ('GOOGL', 'MSFT', 'COF') AND (date BETWEEN '2017-01-01' AND '2017-06-30') GROUP BY ticker, TRUNC(date, 'MM') ORDER BY ticker, TRUNC(date, 'MM')")
openavgsql.repartition(1).write.json("file:///root/data/open_close_rpt_df.json")
openavg.repartition(1).write.json("file:///root/data/open_close_rpt_sql.json")
//in another life insert into another db with JDBC

//for now just use hive
spark.sql("show tables").show()
spark.sql("create table open_close_rpt_H12017 as select * from bigstocks2");
spark.sql("show tables").show()

spark.sql("show tables").show()
stocks.createOrReplaceTempView("mytempstocks")
spark.sql("create table mystock as select * from mytempstocks");
spark.sql("show tables").show()

//https://medium.com/@mrpowers/working-with-dates-and-times-in-spark-491a9747a1d2
//http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
//https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2458071/Date+Functions+and+Properties+Spark+SQL
//https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/sql/functions.html
//http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@trunc(date:org.apache.spark.sql.Column,format:String):org.apache.spark.sql.Column
