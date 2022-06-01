import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.{Level, Logger}

object Q2 extends App {

  def printPhysicalPlan[_](ds: Dataset[_]): Unit = {
    println(ds.queryExecution.executedPlan.treeString)

  }

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

    //===========================//
    //creating SparkSession
    //===========================//
    val spark_posgreSQlReader = SparkSession.builder()
      .appName("Postgresql_Connector_Spark")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://localhost:5432/posgreplayground?user=docker&password=docker"

    val dfSalaries = spark_posgreSQlReader
      .read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "salaries")
      .load()

    println("++++++++printPhysicalPlan: numPartitions=1 +++++++++++++++++++++++++++++++++++++++++++++++")
    println(dfSalaries.queryExecution.toString())
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    println("++++++++printPhysicalPlan: numPartitions=1 +++++++++++++++++++++++++++++++++++++++++++++++")
    printPhysicalPlan(dfSalaries.select())
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


    dfSalaries.show()
    println("Count dfSalaries: " + dfSalaries.count())
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    val dfSalariesParallel = spark_posgreSQlReader
      .read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "salaries")
      .option("numPartitions", 10)
      .option("partitionColumn", "emp_no")
      .option("lowerBound", 10010)
      .option("upperBound", 499990)
      .load()

    /* something like
    SELECT * FROM pets WHERE owner_id >= 1 and owner_id < 1000
    SELECT * FROM pets WHERE owner_id >= 1000 and owner_id < 2000
    SELECT * FROM pets WHERE owner_id >= 2000 and owner_id < 3000
    */

    println("++++++++printPhysicalPlan: numPartitions=10 ++++++++++++++++++++++++++++")
    println(dfSalariesParallel.queryExecution.toString())
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    println("++++++++printPhysicalPlan: numPartitions=10 ++++++++++++++++++++++++++++")
    printPhysicalPlan(dfSalariesParallel.select())
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    dfSalariesParallel.show()

}
