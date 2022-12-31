package dataframes

import dataframes.DataSources.{driver, spark, url}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

  /*
  Combines data from multiple data frames and thus is a wide transformation
  one(or more) column from table1(left) is compared with one(or more) column from table2(right)
  if condition passes rows are combined and non matching rows are discarded
   */

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)


  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  //inner joins
  val guitaristsBandDF = guitaristsDF.join(bandsDF, guitaristsDF.col("band") === bandsDF.col("id"), "inner")
  guitaristsBandDF//.show()

  //extracting join condition as a val to reuse elsewhere
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")

  //outer joins
  //left outer - everything in the inner join plus all rows in the left table with nulls for wherever data is missing
  //left    join    right      condition        join type (default is inner if not passed as argument)
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")//.show()

  //right outer - everything in the inner join plus all rows in the right table with nulls for wherever data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")//.show()

  //outer - everything in the inner join plus all rows in the BOTH table with nulls for wherever data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")//.show()

  //semi-joins -> left-semi ~ everything in the left DF for which there is NO row in the right DF satisfying the condition
  // inner join minus right table data
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")//.show()

  //anti-join -> left-anti ~ everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")//.show()

  //while applying joins the join column gets duplicated in the resulting DF. If we try selecting any column which has same name in other table
  //without mentioning the table name, program will crash
  // guitaristsBandDF.select("id", "band").show() this crashes with error : Reference 'id' is ambiguous, could be: id, id.

  //option 1 - rename the column on which we are joining, this will also let this column appear only once in resulting DF
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")//.show

  //option 2 - drop the duplicate column
  guitaristsBandDF.drop(bandsDF.col("id"))

  //option 3 - rename the offending column and keep the data
  val bandModifiedDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandModifiedDF, guitaristsDF.col("band") === bandModifiedDF.col("bandId"))

  //option 4 - using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /*
  Read employees database created in DB and perform following :
  1. show all employees and their max salary
  2. show all employees who were never managers
  3. find the job titles of best paid 10 employees in the company
   */

  //Reading data from DB
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/employees"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    //.option("driver", "com.mysql.jdbc.Driver")
    //.option("url", "jdbc:mysql://localhost:3306/employees")
    .option("driver", driver)
    .option("url", url)
    .option("user", "root")
    .option("password", "admin")
    .option("dbtable", s"employees.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagerDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("max_salary"))
    .orderBy(col("emp_no"))
  //maxSalariesPerEmpNoDF.show()

  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  //employeesSalariesDF.show(10)
  //employeesSalariesDF.orderBy("emp_no").show(10)

  val nonManagerialEmployees = employeesDF
    .join(deptManagerDF, employeesDF.col("emp_no") === deptManagerDF.col("emp_no"), "left_anti")

  System.out.println(nonManagerialEmployees.count())
  nonManagerialEmployees.show()

  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  mostRecentJobTitlesDF.show()

  val bestPaidEmployees = employeesSalariesDF.orderBy(col("max_salary").desc).limit(10)
  bestPaidEmployees.show()

  val bestPaidJobsDF = bestPaidEmployees.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()
}
