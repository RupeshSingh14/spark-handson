package dataframes

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DF columns and Expressions")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //carsDF.show()

  //columns
  val firstColumn = carsDF.col("Name")

  //selecting (also referred as projections)
  val carNameDF = carsDF.select(firstColumn)

  //carNameDF.show()

  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // scala symbol, auto converted to column
    $"Horsepower", // fancier interpolated string, returns a column object
    expr("Origin") // expression
  )//.show()

  //select with plain column names
  carsDF.select("Name", "Year")

  /*
  While selecting from a DF, we cannot mix plain column names and columns.
  Only selecting is a narrow transformation, where selected column data is selected across all nodes in
  cluster as a new df and no shuffling happens.
   */

  //Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2").as("Weight_in_kg_2")
  )
  //carsWithWeightDF.show()

  //selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 "
  )

  //DF processing

  //adding a new column
  val carsWithKg3 = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  //carsWithKg3.show()

  //renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  //carsWithColumnRenamed.show()

  /*
  For plain string column names without '-', '_', or any such escaping sequences, selecting such column can not correctly be inferred by compiler
   and will throw error. Work around is to use backticks(`) for selecting such namings as below
   */
  //careful with column names
  //carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()

  //removing a column
  //carsWithColumnRenamed.drop("Cylinders", "Displacement").show()

  //filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA") //for using equal operator, use (===)
  //europeanCarsDF.show(2)

  //filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  //americanCarsDF.show(2)

  //chain filters
  //normal filter chaining
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)

  //using AND operator
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))

  //using AND in scala way
  val americanPowerfulCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  //americanPowerfulCarsDF3.show(2)

  //using AND with expression strings
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //unions ~ adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  //works if both the DFs have the same schema
  val allCarsDF = carsDF.union(moreCarsDF)

  //distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  //allCountriesDF.show()

  /*
  1. Read the movies DF and select 2 columns of choice
  2. Create another column summing up the total profit of the movies - US_Gross + Worldwide_Gross + DVD_sales
  3. Select all COMEDY movies with IMDB rating above 6
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //moviesDF.printSchema()
  //moviesDF.show(5)

  //moviesDF.select("Title", "Release_Date").show(5)
  moviesDF.selectExpr(
    "Title",
    "Release_Date")//.show(5)

  val totalCollectionDF = moviesDF.withColumn("Total_Collection", col("US_Gross") + col("Worldwide_Gross"))

  val totalCollectionDF0 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross").withColumn("Total_Collection", col("US_Gross") + col("Worldwide_Gross"))

  val totalCollectionDF2 = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Collection")
  )

  val totalCollectionDF3 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  //totalCollectionDF.show(5)
  //totalCollectionDF2.show(5)
  //totalCollectionDF3.show(5)
  //totalCollectionDF0.show(5)

  val goodRatedComedyMovies = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val goodRatedComedyMovies1 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  val goodRatedComedyMovies2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  goodRatedComedyMovies.show(5)
  goodRatedComedyMovies1.show(5)
  goodRatedComedyMovies2.show(5)








}
