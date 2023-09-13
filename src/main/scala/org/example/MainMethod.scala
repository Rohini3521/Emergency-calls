package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, date_format, regexp_replace, unix_timestamp}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}

object MainMethod {

 def main(args:Array[String]): Unit ={
   Logger.getLogger("org").setLevel(Level.ERROR)
   val spark = SparkSession
     .builder
     .master("local")
     .appName("Emergency")
     .getOrCreate()

   val schema = StructType(Array(
     StructField("latitute", DoubleType),
     StructField("longitude", DoubleType),
     StructField("description", StringType),
     StructField("zip", IntegerType),
     StructField("title", StringType),
     StructField("timestamp", StringType),
     StructField("township", StringType),
     StructField("address", StringType),
     StructField("e", StringType)
   )
   )

   val input_path ="D:/911.csv"

   var df = spark.read
     .option("header", "true")
     .format("csv")
     .schema(schema)
     .load(input_path)


   // Task 1
   val cols_to_drop = Array("description","e","timestamp","title")
   df = df.withColumn("timestamp", unix_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm").cast(TimestampType))
     .withColumn("year", date_format(col("timestamp"), "yyyy"))
     .withColumn("month", date_format(col("timestamp"), "MM"))
     .withColumn("day", date_format(col("timestamp"), "dd"))
     .withColumn("hour", date_format(col("timestamp"), "HH"))
     .withColumn("type", functions.split(col("title"), ":").getItem(0))
     .withColumn("subtype", functions.split(col("title"), ":").getItem(1))
     .withColumn("subtype", regexp_replace(col("subtype"), "-", ""))
     .drop(cols_to_drop: _*)

   val grp_type = df.groupBy("type", "subtype").count()
     .orderBy(col("type").asc, col("count").desc)
     .select("type", "subtype", "count")

   val cardiac_df = df.filter(col("subtype").like("%CARDIAC%"))
     .groupBy(col("township")).count()
     .orderBy(col("count").desc)

   df=df.withColumn("hour", col("hour").cast(IntegerType))
   df.createOrReplaceTempView("incidents")
   val night_incidents = spark
     .sql("SELECT subtype, COUNT(*) AS count FROM incidents WHERE  hour >= 0 AND hour < 6 GROUP BY subtype")

   night_incidents.show()
   night_incidents.coalesce(1).write.mode("overwrite")
     .parquet("D:/incidents.parquet")




 }

}
