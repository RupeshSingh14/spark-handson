package dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App{

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //adding a plain value
  moviesDF.select(col("Title"), lit(47).as("plain_value"))//.show()

  //Booleans
  moviesDF.select("Title").where(col("Major_Genre") === "Drama")
  moviesDF.select("Title").where(col("Major_Genre") equalTo "Drama")//.show(5)

  //extracting filters in val
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  //multiple ways of filtering
  moviesDF.select("Title").where(dramaFilter)
  moviesDF.select("Title").filter(preferredFilter)//.show(5)

  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  //filter on a boolean column
  moviesWithGoodnessFlagDF.where("good_movie")//.show(5) // where(col("good_movie") === "true")

  //negations and dropped the column at end
  moviesWithGoodnessFlagDF.where(not(col("good_movie"))).drop("good_movie")//.show(5)

  //numbers
  //maths operators can directly be used and operated on numeric columns only
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)//.show()

  //correlation - coefficient association of 2 values having value between -1 t0 1, value closer to -1 means a negative correlation ie.. more the
  //one variable increases, more the other variable decreases, whereas value closer to 1 means a positive strong correlation. if value is
  //around -0.3 or 0.3, that means values are not correlated at all.

  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) //corr in an action

  //Strings

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //carsDF.select(initcap(col("Name"))).show() //Returns a new string column by converting the first letter of each word to uppercase
  //carsDF.select(lower(col("Name"))).show // similarly upper can be used

  //contains
  carsDF.select("*").where(col("Name").contains("Volkswagen"))

  //regex ~ better way of doing contains
  val regexString = "volkswagen|vw"

  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")//.show()

  //replace
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's car").as("regex_replace")
  ).drop("Name")//.show()

  /*
  filter the carsDF by a list of car names obtained by an API call
   */
  def getCarNames: List[String] = List("Volkswagen", "Chevrolet", "Ford")

  //version 1
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // produces "volkswagen|chevrolet|ford"
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract").show()

  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarFilter) => combinedFilter or newCarFilter)
  carsDF.filter(bigFilter)show()

}
