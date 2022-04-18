import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf,asc,_}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.io.Directory


object Q7 extends App {


  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)


  def printPhysicalPlan[_](ds: Dataset[_]): Unit = {
    println(ds.queryExecution.executedPlan.treeString)

  }


  def getRandom(start: Int, end: Int): Int = {
    val r = scala.util.Random
    start + r.nextInt(end)
  }

  // UDF
  val randomNumberUdf: UserDefinedFunction = udf(getRandom(_:Int, _:Int): Int)



    //===========================//
    //creating SparkSession
    //===========================//
    val spark = SparkSession.builder()
      .appName("Data_Skew")
      .config("spark.master","local[*]")
      .getOrCreate()


    // set smaller number of partitions so they can fit the screen
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    // disable broadcast join to see the shuffle
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

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


    println("+++++++ 7 +++++++++++++++++++++++++++++++++++++++++++++")
    val resultJoin = FactTableDF.join(dimApplicationDF, Seq( "application_id" )  , "left")
    resultJoin.explain()

    resultJoin
      .groupBy("application_id")
      .count()
      .orderBy(asc("application_id"))
      .show()

    //Methods
    //1.Repartition data по другому ключу (имеющемуся или синтетическому)

    // get number of shuffle partitions
    val parts = spark.conf.get("spark.sql.shuffle.partitions").toInt
    println("SALT")
    println(parts)

    val saltedResultDF = resultJoin
      .withColumn("Salt", randomNumberUdf(lit(1), lit(parts)))
    saltedResultDF.groupBy("Salt").count().orderBy(col("Salt").desc).show()

    //2.Broadcast smaller dataframe
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1)
    val resultBroadcastJoin = FactTableDF.join(broadcast(dimApplicationDF), Seq("application_id"), "left")
    resultBroadcastJoin.explain()

    resultBroadcastJoin
      .groupBy("application_id")
      .count()
      .orderBy(asc("application_id"))
      .show()


  }
