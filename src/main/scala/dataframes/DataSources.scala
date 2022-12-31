package dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App{

  val spark = SparkSession.builder()
    .appName("Data sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    //StructField("Year", StringType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  Reading a DF:
  - format
  - schema or inferSchema = true
  - path
  - zero or mode options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carSchema)
    .option("mode", "failFast") // dropMalformed ~ drops the illegal argument row, permissive (default) ~ puts null for all the row values
    .option("path", "src/main/resources/data/cars.json" )
    //.load("src/main/resources/data/cars.json")
    .load()

  //alternative reading using a map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()

  /*
  //writing Dfs
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  - path
  - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    //.option("path", "src/main/resources/data/cars_dup.json")
    //.save()
    .save("src/main/resources/data/cars_dup.json")

  //JSON flags
  spark.read
    .format("json")
    .schema(carSchema)
    .option("dateFormat", "YYYY-MM-dd") //couple with schema; if spark fails parsing, it will put null. Timestamp format also available
    .option("allowSingleQuotes", true) //a flag to allow json values with single quotes
    .option("compression", "uncompressed") //default type; other types - bzip, gzip, lz4, snappy, deflate
    .load("src/main/resources/data/cars.json")
    .show(5)

  /*
   instead of using format and load options, we can use one option ~ json("path") at end
   spark.read
    .schema(carSchema)
    .option("dateFormat", "YYYY-MM-dd")
    .option("allowSingleQuotes", true)
    .option("compression", "uncompressed")
    .json("src/main/resources/data/cars.json")
   */

  //CSV flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM d yyyy") //for dates data format
    .option("header", true) // for ignoring the first row headers and using the given schema, since csv may or may not have headers
    .option("sep", ",") //for values separated in a row
    .option("nullValue", "") // since there is no notion of null in csv, such nulls will be handled
    .load("src/main/resources/data/stocks.csv")
    .show(5)

  /*
    spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", true)
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")
   */

 // Parquet - is an open-source compressed binary data storage format optimized for fast reading of columns
 // It is default storage format for data frames.

  carsDF.write
    .mode(SaveMode.Overwrite)
   // .parquet("src/main/resources/data/cars.parquet")
    .save("src/main/resources/data/cars.parquet") // save can be used since parquet is default storage.

  //Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/classicmodels"


  //Reading data from DB
  val employeesDf = spark.read
    .format("jdbc")
    //.option("driver", "com.mysql.jdbc.Driver")
    //.option("url", "jdbc:mysql://localhost:3306/classicmodels")
    .option("driver", driver)
    .option("url", url)
    .option("user", "root")
    .option("password", "admin")
    .option("dbtable", "classicmodels.employees")
    .load()

  employeesDf.show(10)

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  //writing to tab separated csv
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // writing to parquet file
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  //writing to DB
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", "root")
    .option("password", "admin")
    .option("dbtable", "classicmodels.movies")
    .save()















}
