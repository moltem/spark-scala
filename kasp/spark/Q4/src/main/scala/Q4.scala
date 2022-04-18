import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.sql.functions.{lit, _}

object Q4 extends App {

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

    println("+++++++ 4 STRAT BroadcastNestedLoopJoin+++++++++++++++++++++++++++++++++++++++++++++")
    val dfLeftJoinBJN = FactTableDF.join(dimApplicationDF, FactTableDF( "application_id" ) >=  dimApplicationDF( "application_id" ) , "left")
    dfLeftJoinBJN.explain()
    println("++++++++ 4 END +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    println("+++++++ 4 STRAT SortMergeJoin ++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    val dfLeftJoinSM = FactTableDF.join(dimApplicationDF, FactTableDF( "application_id" ) ===  dimApplicationDF( "application_id" ) , "left")
    dfLeftJoinSM.explain()
    println("++++++++ 4 END SortMergeJoin +++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    println("+++++++ 4 STRAT BroadcastHashJoin ++++++++++++++++++++++++++++++++++++++++++++++++++")
    val dfLeftJoinBJ = FactTableDF.join( broadcast(dimApplicationDF), FactTableDF( "application_id" ) ===  dimApplicationDF( "application_id" ) , "left")
    dfLeftJoinBJ.explain()
    println("++++++++ 4 END BroadcastHashJoin +++++++++++++++++++++++++++++++++++++++++++++++++++")

}
