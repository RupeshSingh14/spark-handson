package dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

/*
Data sets are typed data frames - distributed collection of JVM objects.
 */
object DataSet extends App {

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DataSets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  /*
  applying language(scala here) level logics(transformations) and making the structured data strongly typed so that it can be checked
  at compile time for errors and be out of bugs in production, we use data sets over data frame. Every dataframe is a collection of DataSet[Row]
  DataSet is collection of JVM objects which we define and feed to DFs via case classes. Most useful when -
  - we want to maintain type information
  - we want clean concise code
  - our filters/transformations are hard to express in DF or SQL
   */

  //convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS = numbersDF.as[Int]

  numbersDS.filter(_ < 100)   // Data set operation in scala

  //dataset of a complex type
  // 1. define the case class
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: String,
                Origin: String
                )

  //2. read the DF from the file
  def readDF(filename : String) = spark
    .read.option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

 //val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  //3. define an encoder (importing the implicits)
  import spark.implicits._
  val carsDF = readDF("cars.json")

  //4. convert the DF to DS
  val carsDS = carsDF.as[Car]

  //DS collections functions
  numbersDS.filter(_ < 100).show()

  // functions like map, flatmap, fold, reduce, for comprehensions can be used
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  carNamesDS.show()

  /*
  1. counting how many care are there
  2. counting powerful cars (HP > 140)
  3. Average HP for the entire dataset
   */


  println(carsDS.count())

  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140))

  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_+_) / carsDS.count())

  //we can use data frame api's too on Data set
  carsDS.select(avg(col("Horsepower"))).show

  //joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandDS = readDF("bands.json").as[Band]

  // type will be tuple of both the data sets
  val guitarPlayerBandDS = guitarPlayersDS.joinWith(bandDS, guitarPlayersDS.col("band") === bandDS.col("id"), "inner")

  guitarPlayerBandDS.show()

  // join the guitarDS and guitarPlayerDS, in an outer join

  guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  //grouping DS
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  //joins and groups are wide transformations which involves SHUFFLE operations



}
