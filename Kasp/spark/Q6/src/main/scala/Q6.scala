import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, collect_list, skewness}
import org.apache.spark.sql.{Dataset, SparkSession, functions}

object Q6 extends App{

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

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()

  //avg
  println("avg: "+ df.select(avg("salary")).collect()(0)(0))
  printPhysicalPlan(df.select(avg("salary")))

  //collect_list
  df.select(collect_list("salary")).show(false)
  printPhysicalPlan( df.select(collect_list("salary")))

  //skewness
  df.select(skewness("salary")).show(false)
  printPhysicalPlan(df.select(skewness("salary")))

  //sum
  df.select(functions.sum("salary")).show(false)
  printPhysicalPlan(df.select(functions.sum("salary")))

}
