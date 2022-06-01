import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit, upper}

object Q3 extends App {


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
    .appName("DFbasics")
    .config("spark.master","local[*]")
    .getOrCreate()

  //====================================//
  // Create data for DataFrame  //
  //====================================//
  val structureData_rfj = Seq(
    Row(Row("James","","Smith"),"36636","M",3100),
    Row(Row("Michael","Rose",""),"40288","M",4300),
    Row(Row("Robert","","Williams"),"42114","M",1400),
    Row(Row("Maria","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  //=================================//
  // Create default STRUCTURE SCHEMA //
  //=================================//
  val structureSchema_rfj = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)

  //=================================//
  // 3. read Schema from json frile  //
  // Create default Data Frame //
  //=================================//
  val df_default_rfj = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData_rfj),structureSchema_rfj
  )
  println("+++++++SHOW DF++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  df_default_rfj.show()
  println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

  println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  df_default_rfj.filter(
    upper(col("status")) == "APPLIED"
  )
    .withColumn("period", lit(1))
    .show(100)
  println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  df_default_rfj
    .withColumn("period", lit(1))
    .filter(
      upper(col("status")).isin("APPLIED")
    )
    .show(100)
}
