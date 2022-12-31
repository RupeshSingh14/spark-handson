package dataset

import org.antlr.v4.runtime.atn.SemanticContext.OR
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy.LEGACY
import org.apache.spark.sql.sources.Or

object ComplexTypes extends App {

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //Dates
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")) //conversion
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // timestamp
    .withColumn("Movie_Age", - (datediff(col("Today"), col("Actual_Release"))/365)) //date diff

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)//.show()

  // reading stocks.csv and parse the dates
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF.withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))

  //stocksDFWithDates.show()

  //structures ~ structs in spark, are groups of columns aggregated in one. tuples composed as a value
  //with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit")).show()

  //with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross").show


  //Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Array_of_title_words"))

  moviesWithWords.select(
    col("Title"),
    expr("Array_of_title_words[0]"),
    size(col("Array_of_title_words")),
    array_contains(col("Array_of_title_words"),  "Love")
  ).show()



}
