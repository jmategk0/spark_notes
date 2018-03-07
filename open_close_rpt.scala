//import stuff for tests
import org.apache.spark.sql.SparkSession
val ss = SparkSession.
builder().
master("local").
appName("Spark in Motion Example").
config("spark.config.option", "some-value").
enableHiveSupport().
getOrCreate()

import ss.implicits._
import org.apache.spark.sql.functions._
import java.util.Date;

//Test 1. Spark with dataframe API 
val operationStart = new Date();

//Import the dataset from CSV 
val bigstocks = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/stocks/WIKI_PRICES_212b326a081eacca455e13140d7bb9db.csv")

//Create Report in two steps
//1. Add new col for YYYY-MM from date, filter by date range and three stock codes
//2. Group By stock code, year/mo then get avg for open price and close price
val withdates = bigstocks.withColumn("year_month", date_format(col("date"), "YYYY-MM")).filter($"date" >= "2017-01-01"and $"date" <= "2017-06-30").filter(($"ticker" isin ("GOOGL","MSFT","COF")))
val openavg = withdates.groupBy($"ticker", $"year_month").avg("open", "close").orderBy($"ticker", $"year_month")

//print the results
openavg.show(20)

val operationEnd = new Date();
val timeDiff = Math.abs(operationStart.getTime() - operationEnd.getTime());
val timeInSeconds = timeDiff / (1000)
print(timeInSeconds)

//Test 2. Spark SQL 
val operationStart = new Date();

//Import the dataset from CSV 
val bigstocks = spark.read.option("inferSchema", "true").option("header","true").csv("file:///root/data/stocks/WIKI_PRICES_212b326a081eacca455e13140d7bb9db.csv")

//Create Report in two steps
//1. Create Temp Table
//2. Run SQL QRY

bigstocks.createOrReplaceTempView("bigstocks2")
val openavgsql = spark.sql("SELECT ticker, TRUNC(date, 'MM'), AVG(open), AVG(close) FROM bigstocks2 WHERE ticker IN ('GOOGL', 'MSFT', 'COF') AND (date BETWEEN '2017-01-01' AND '2017-06-30') GROUP BY ticker, TRUNC(date, 'MM') ORDER BY ticker, TRUNC(date, 'MM')")

//print the results
openavgsql.show(20)

val operationEnd = new Date();
val timeDiff = Math.abs(operationStart.getTime() - operationEnd.getTime());
val timeInSeconds = timeDiff / (1000)
print(timeInSeconds)

//save results in hive; as new table // Works fine in the spark shell but throws a "please enable hive support error in notebook".

spark.sql("show tables").show()
openavgsql.createOrReplaceTempView("tempopenavgsql")
spark.sql("create table openclosesql_3_5_18 as select * from tempopenavgsql");
spark.sql("show tables").show()