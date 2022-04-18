import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scala.reflect.io.Directory
import java.io.File

object Q5 extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  def printPhysicalPlan[_](ds: Dataset[_]): Unit = {
    println(ds.queryExecution.executedPlan.treeString)

  }

    //===========================//
    //creating SparkSession
    //===========================//
    val spark = SparkSession.builder()
      .appName("Q5")
      .config("spark.master","local[*]")
      .getOrCreate()

    // set smaller number of partitions so they can fit the screen
    //spark.conf.set("spark.sql.shuffle.partitions", 8)
    // disable broadcast join to see the shuffle
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    // reading a DF

    val dimApplicationDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/DimApplication.json")

    val FactTableDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/FactTable.json")

    FactTableDF.show()
    dimApplicationDF.show()

    println("+++++++ 5 +++++++++++++++++++++++++++++++++++++++++++++")
    val resultJoin_5 = FactTableDF.join(dimApplicationDF, Seq( "application_id" )  , "left")
    printPhysicalPlan(resultJoin_5)

    println(FactTableDF.count())
    println(dimApplicationDF.count())
    println("++++++++++++++++++++++++++++++++++++ WRITE +++++++++++++++++++++++++++++++++++++++++++++++")
    val directory = new Directory(new File("src/main/resources/question_5/"))
    directory.deleteRecursively()
    resultJoin_5.write.parquet("src/main/resources/question_5")

}
