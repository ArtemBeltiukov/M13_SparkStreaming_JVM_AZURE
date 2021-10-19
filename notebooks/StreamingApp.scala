// Databricks notebook source

import org.apache.spark
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.jline.keymap.KeyMap.display


val inputPath = "abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather"

// define schema
val jsonSchema = new StructType()
  .add("address", StringType)
  .add("avg_tmpr_c", DoubleType)
  .add("avg_tmpr_f", DoubleType)
  .add("city", StringType)
  .add("country", StringType)
  .add("geoHash", StringType)
  .add("id", StringType)
  .add("latitude", DoubleType)
  .add("longitude", DoubleType)
  .add("name", StringType)
  .add("wthr_date", StringType)
  .add("wthr_year", StringType)
  .add("wthr_month", StringType)
  .add("wthr_day", StringType)

// get static data
val staticInputDF =
  spark
    .read
    .format("parquet")
    .schema(jsonSchema)
    .load(inputPath)
// display static data
display(staticInputDF)

// COMMAND ----------
// create streaming DF
val streamingInputDF =
spark
  .readStream
  .schema(jsonSchema)
  .option("maxFilesPerTrigger", 1)
  .format("parquet")
  .load(inputPath)
// define pipeline of data aggregations
val streamingCountsDF =
  streamingInputDF
    .groupBy($"wthr_date", $"city")
    .agg(avg("avg_tmpr_c").alias("average"),
      max("avg_tmpr_c").alias("maximum"),
      min("avg_tmpr_c").alias("minimum"),
      count("id").alias("hotel_count"))


streamingCountsDF.isStreaming

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")
// execute pipeline
val query =
  streamingCountsDF
    .writeStream
    .format("memory")
    .queryName("counts")
    .outputMode("complete")
    .start()


// COMMAND ----------

// MAGIC %sql
// MAGIC -- number of hotels for each city and avg temperature
// MAGIC select * from counts order by hotel_count desc

// COMMAND ----------

// MAGIC %sql
// MAGIC -- top 10 biggest cities and average temperature
// MAGIC select * from counts order by hotel_count desc LIMIT 10

// COMMAND ----------


