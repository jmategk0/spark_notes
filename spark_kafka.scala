
val spark = SparkSession
 .builder
 .appName("Spark-Kafka-Integration")
 .master("local")
 .getOrCreate()
 import spark.implicits._

// define data schema of the airline routes dataset
 val mySchema = StructType(Array(
 StructField("id", IntegerType),
 StructField("airline", StringType),
 StructField("airline_id", IntegerType),
 StructField("source_airport", StringType),
 StructField("source_airport_id", IntegerType),
 StructField("destination_airport", StringType),
 StructField("destination_airport_id", IntegerType),
 StructField("stops", IntegerType),
 StructField("equipment", StringType),
))

//import data from csv file, I had to add a pk/id value for each row
// when you drop csv files into the directory they will automatically appended in the streaming dataframe 
val streamingDataFrame = spark.readStream.schema(mySchema).csv("data/routes_new.csv")

// Once we have streamingDataFrame created we can publish to kafka 
// defining out airline id/pk value as the kafka key and casting the row value to json as the kafka value
//In this case I named the topic airline routes 
 streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
  writeStream
  .format("kafka")
  .option("topic", "airline_routes")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("checkpointLocation", "data/checkpoints/airline_routes/offsets")
  .start()

// to subscribe to the kafka stream we create a readSteamDataframe 
// we then select and cast data from the read stream converting the value from json based on
// our predefined schema, and casting out timestamp
  val readSteamDataframe = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "airline_routes")
  .load()

  val df1 = readSteamDataframe.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
  .select(from_json($"value", mySchema).as("data"), $"timestamp")
  .select("data.*", "timestamp")

// If we want to see the results in the console then we can use writeStream
df1.writeStream
    .format("console")
    .option("truncate","false")
    .start()
    .awaitTermination()

