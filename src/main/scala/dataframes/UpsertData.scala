package dataframes

import dataframes.UpsertData.connectionProperties
import dataset.CommonTypes.regexString
import dataset.DataSet.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace}
import sparksql.SparkSQL.spark

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object UpsertData extends App {

  //for avoiding all detail logging in console
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Upsert Data in MySQL")
    .config("spark.master", "local")
    .getOrCreate()

  //Reading data from DB
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/employees"

  val connectionProperties = new Properties
  connectionProperties.put("jdbcDriver", driver)
  connectionProperties.put("jdbcUrl", url)
  connectionProperties.put("user", "root")
  connectionProperties.put("password", "admin")
  connectionProperties.put("dbname", "employees")


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

  case class employees(empNo: Long, birthDate: String, firstName: String, lastName: String, gender: String, hireDate: String)

  val employeesDF = readTable("employees")

  val employeesDF1 = employeesDF.filter("gender == 'F'")
  employeesDF.printSchema()

  val employeesDF2 = employeesDF1.select(
    col("emp_no"),
    col("birth_date"),
    col("first_name"),
    col("last_name"),
    regexp_replace(col("gender"), "F", "M").as("gender"),
    col("hire_date"))

  //employeesDF1.show(10)
  //employeesDF2.show(10)

/*
  import spark.implicits._
  employeesDF1.rdd.coalesce(10).mapPartitions((employees) => Iterator(employees)).foreach { batch =>
    val dbc: Connection = DriverManager.getConnection(url, "root", "admin")
    val st: PreparedStatement = dbc.prepareStatement("UPDATE employees.employees SET gender=? where empNo=?")

    batch.grouped(1000).foreach { session =>
      session.foreach { x =>
        st.setDouble(1, x.getDouble(1))
        st.addBatch()
      }
      st.executeBatch()
    }
    dbc.close()
  }
*/
  val sc = spark.sparkContext

  val brConnect = sc.broadcast(connectionProperties)

    //browse through each partition data
  employeesDF2.rdd.coalesce(10).foreachPartition(partition =>
  {
    val connectionProperties = brConnect.value
    val jdbcUrl = connectionProperties.getProperty("jdbcUrl")
    val user = connectionProperties.getProperty("user")
    val pwd = connectionProperties.getProperty("password")

    Class.forName(driver)

    val dbc: Connection = DriverManager.getConnection(jdbcUrl, user, pwd)
    val dbBatchSize = 100
    var st: PreparedStatement = null


    //employeesDF1.foreachPartition(f)
    //val f: Iterator[Row] => Unit = (iterator: Iterator[Row]) => {
    partition.grouped(dbBatchSize).foreach(batch => {
      batch.foreach { row => {
          val empNoIndex = row.fieldIndex("emp_no")
          val empNo = row.getInt(empNoIndex)

          val birthDateIndex = row.fieldIndex("birth_date")
          val birthDate = row.getDate(birthDateIndex)

          val firstNameIndex = row.fieldIndex("first_name")
          val firstName = row.getString(firstNameIndex)

          val lastNameIndex = row.fieldIndex("last_name")
          val lastName = row.getString(lastNameIndex)

          val genderIndex = row.fieldIndex("gender")
          val gender = row.getString(genderIndex)

          val hireDateIndex = row.fieldIndex("hire_date")
          val hireDate = row.getDate(hireDateIndex)

          val updateSQLString = "UPDATE employees.employees SET birthDate=?, firstName=?, lastName=?, gender=?, hireDate=? WHERE empNo=?"
          st = dbc.prepareStatement(updateSQLString)
          st.setDate(1, birthDate)
          st.setString(2, firstName)
          st.setString(3, lastName)
          st.setString(4, gender)
          st.setDate(5, hireDate)
          st.setInt(6, empNo)

          st.addBatch()

          println(" Emp no ====== "  + " ====== " + updateSQLString)

        }
          st.executeBatch()
        }
      })

      dbc.close()

  })

}

