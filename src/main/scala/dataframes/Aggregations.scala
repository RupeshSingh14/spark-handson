package dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  /*
  Aggregations and Grouping are wide transformations ie.. they cause data in data frames which are spread across different nodes in cluster
  to shuffle between nodes and get the result. This is called shuffling where
  one/more input partitions create one/more output partitions
  This cause big performance impact on spark jobs
  Such actions should be called at last most step in data pipelines on bare minimum data
   */

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //counting
  val generesCountDF = moviesDF.select(count(col("Major_Genre"))) //count of values other than nulls
  //generesCountDF.show()

  //moviesDF.selectExpr("count(Major_Genre)").show()

  //counting all
  //moviesDF.select(count("*")).show() //counts all the row and includes nulls

  //counting distinct
  //moviesDF.select(countDistinct(col("Major_Genre"))).show()

  //approximate count
  //moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  //min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  //moviesDF.selectExpr("min(IMDB_Rating)").show()

  //sum
  moviesDF.select(sum("US_Gross"))
  //moviesDF.selectExpr("sum(US_Gross)").show()

  //average
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  //moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  //data-science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")), // this is the median rating provided
    stddev(col("Rotten_Tomatoes_Rating")) // this is standard deviation of the rating, a less value like 2,4,6 means less deviation from
    //mean, so data is not widespread and higher value means data varies and is widespread.
  )//.show()

  //Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) //includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  //countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  //avgRatingByGenreDF.show()

  val aggregationsByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(col("Avg_Rating"))

  //aggregationsByGenreDF.show()

  /*
  1. Sum up all the profits of all movies in the DF
  2. Count how many distinct directors are there
  3. Show the mean and standard deviation of US gross revenue for the movies
  4. Compute the average IMDB rating and the average US gross revenue per Director
   */

  val sumOFProfitsDF = moviesDF.
    select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))

  //sumOFProfitsDF.show()

  val countDirectors = moviesDF.
    select(countDistinct(col("Director")))

 // countDirectors.show()

  moviesDF.select(
    mean("US_Gross")as("US_Gross_Mean"),
    stddev("US_Gross")as("US_Gross_Standard_Deviation")
  )//.show()

  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

}
