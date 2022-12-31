package sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object SparkSQL extends App{

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Spark SQL hands-on")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
  //  .config("spark.sql.legacy.allowCreatingManagedTableUsingNonEmptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")//.show()

  //using spark sql
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  //americanCarsDF.show()

  // this creates a file directory bases data base in the project under directory named spark-warehouse
  // this default creation is managed by a spark session config option as above
  spark.sql("create database rupesh")
  spark.sql("use rupesh")

  val databasesDF = spark.sql("show databases")

  //databasesDF.show()

  //transfer tables from a DB to spark tables

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

/*  val employeesDF = readTable("employees")
  employeesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("employees")*/

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
    //overwrite is not working for current spark versions
    //need to check on it
  }

  transferTables(List("employees", "departments", "dept_manager", "salaries", "titles", "dept_emp"))

  //read the DF from warehouse
  //val employeesDF2 = spark.read.table("employees")

  /*
  1. Read the movies DF and store it is as a spark table in the rupesh database
  2. Count number of employees were hired in between Jan 1999 to Jan 2000
  3. Show the average salaries for the employee hired in between those dates, grouped by department
  4. Show the name of best paying department hired in between those dates
   */

  //1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

/*
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")
*/

  //2
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  )//.show()

  //3
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, salaries s, dept_emp de
      |where e.emp_no = s.emp_no
      |and e.emp_no = de.emp_no
      |and hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |group by de.dept_no
      |""".stripMargin
  )//.show()

  //4
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, salaries s, dept_emp de, departments d
      |where e.emp_no = s.emp_no
      |and e.emp_no = de.emp_no
      |and de.dept_no = d.dept_no
      |and hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin
  ).show()


}
