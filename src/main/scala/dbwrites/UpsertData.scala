package dbwrites

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}

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

  //case class employees(empNo: Long, birthDate: String, firstName: String, lastName: String, gender: String, hireDate: String)

  val employeesDF = readTable("employees")

  val employeesTmpDF = readTable("employees_tmp")

  val employeesDF1 = employeesDF.filter("gender == 'F'")
  //employeesDF.printSchema()

  val employeesDF2 = employeesDF1.select(
    col("emp_no"),
    col("birth_date"),
    col("first_name"),
    col("last_name"),
    regexp_replace(col("gender"), "F", "M").as("gender"),
    col("hire_date")).limit(100)

  val employeesDF3 = employeesTmpDF.union(employeesDF2)

  //employeesDF1.show(10)
  employeesDF3.show(10)

  val sc = spark.sparkContext

  val brConnect = sc.broadcast(connectionProperties)

  //browse through each partition data
  employeesDF3.rdd.coalesce(5).foreachPartition(partition =>
  {
    val connectionProperties = brConnect.value
    val jdbcUrl = connectionProperties.getProperty("jdbcUrl")
    val user = connectionProperties.getProperty("user")
    val pwd = connectionProperties.getProperty("password")

    Class.forName(driver)

    val dbc: Connection = DriverManager.getConnection(jdbcUrl, user, pwd)
    val dbBatchSize = 20
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

        //To check if the DF record exists
        val whereCol: List[String] = List("emp_no")
        val sqlString = "SELECT * FROM employees.employees where emp_no=?"
        val ps: PreparedStatement = dbc.prepareStatement(sqlString)
        ps.setInt(1, empNo)

        val rs = ps.executeQuery()
        var count: Int = 0

        while(rs.next()) {
          count = 1
        }

        //applying switch for insert and update
        var operation = "NULL"
        if(count > 0)
          operation = "UPDATE"
        else
          operation = "INSERT"

        if(operation == "UPDATE") {

          val updateSQLString = "UPDATE employees.employees SET birth_date=?, first_name=?, last_name=?, gender=?, hire_date=? WHERE emp_no=?"
          st = dbc.prepareStatement(updateSQLString)
          st.setDate(1, birthDate)
          st.setString(2, firstName)
          st.setString(3, lastName)
          st.setString(4, gender)
          st.setDate(5, hireDate)
          st.setInt(6, empNo)

        }else if( operation == "INSERT") {
          val insertSQLString = "INSERT INTO employees.employees(emp_no, birth_date, first_name, last_name, gender, hire_date) VALUES (?,?,?,?,?,?)"
          st = dbc.prepareStatement(insertSQLString)
          st.setInt(1, empNo)
          st.setDate(2, birthDate)
          st.setString(3, firstName)
          st.setString(4, lastName)
          st.setString(5, gender)
          st.setDate(6, hireDate)
        }

        // add the records to batch action
        st.addBatch()

        println( operation + " => " + empNo +  " => " + st.toString)

      }
        st.executeBatch()
      }
    })

    dbc.close()

  })

}


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