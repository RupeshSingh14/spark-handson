package loadData

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

import java.io.File

object LoadAuditData extends App {

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  //initialising and creating the spark session
  val spark = SparkSession.builder()
    .appName("Audit Data Load")
    .config("spark.master", "local")
    .getOrCreate()


// function to get all the csv formatted files available in a defined directory
  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath).toList
  }

  // list of the files available in directory
  val list = getListOfFiles("src/main/resources/test") // TO DO - put this path to load from a configuration/property file
  println(list)

  //Iterating through the list of available CSV files and loading the data in spark dataframes and performing the logic
  // of adding unique job id and sequence number and saving it.
  list.foreach(file => {
    val loadedDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(file)

    if(!loadedDF.isEmpty) {
      println(loadedDF.count())
      val modifiedDF = loadedDF.withColumn("job_id", expr("uuid()")).withColumn("seq_id", monotonically_increasing_id + 1)

      modifiedDF.show(10)

      modifiedDF.write
        .mode(SaveMode.Append)
        .format("csv")
        .option("header", "true")
        .option("sep", ",")
        .save("src/main/resources/output") // writing the csv to a directory.
    }
  }
  )

}