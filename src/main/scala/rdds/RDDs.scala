package rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

/*
RDD VS DF or DS
DataFrame == DataSet[Row]

Common
collection API - map, flatmap, filter, take, reduce
union, count, distinct
groupBy, sortBy

RDDs over DS
partition control : repartition, coalesce, partitioner, mapPartitions, zipPartitions
operation control : checkpoint, isCheckpointed, localCheckpoint, cache
storage control : cache, getStorageLevel, persist

DS over RDDs
select and join

Distributed typed collections of JVM objects.
Partitioning can be controlled
order of elements can be controlled
order of operations matters for performance
 */
object RDDs extends App{

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("RDDs hands on")
    .config("spark.master", "local")
    .getOrCreate()

  //Spark context is entry point instance for using RDDs in spark
  val sc = spark.sparkContext

  //Distribute a local Scala collection to form an RDD
  val numbers = 1 to 100000
  val numbersRDD = sc.parallelize(numbers)

  //reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // another way
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // read from DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksRDD3 = stocksDF.rdd // this conversion looses data type

  import spark.implicits._
  val stockDS = stocksDF.as[StockValue]
  val stocksRDD4 = stockDS.rdd // this conversion holds data type

  // RDD to DF
  val numbersDF = numbersRDD.toDF("numbers")

  // RDD to DS
  val numbersDS = spark.createDataset(numbersRDD)

  // transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT")  // lazy transformation
  val msftCount = msftRDD.count()  // eager ACTION
  println(msftCount)

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct()  // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =  Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  //implicit val stockOrdering1 =  Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  //implicit val stockOrdering2 =  Ordering.fromLessThan((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() //action
  println(minMsft)

  //reduce
  println(numbersRDD.reduce(_+_))

  //grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)  // its expensive as it causes shuffling
  println(groupedStocksRDD)

  //Partitioning
  /*
  Repartitioning is Expensive as it involves shuffling
  best practise is to partition early, then process that.
  Ideal Size of partition 10MB- 100MB
   */

  println(stocksRDD.getNumPartitions)
  val repartitionedStocksRDD = stocksRDD.repartition(10)
  repartitionedStocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks10")

  //coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(5)  // shuffling is by default false(off)
  coalescedRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks5")

  /*
  1. Read the movies.json as an RDD
  2. show the distinct generes as an RDD
  3. Select all the movies in the drama genre with IMDB rating > 6
  4. show the average rating of movies by genre
   */

  case class Movie(title: String, genre: String, rating: Double)

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //1
  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  //2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  //3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  moviesRDD.toDF().show()
  genresRDD.toDF().show()
  goodDramasRDD.toDF().show()

  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF().show()
  moviesRDD.toDF().groupBy(col("genre")).avg("rating").show()

}

