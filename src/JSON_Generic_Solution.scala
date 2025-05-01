//package test
import org.apache.spark._
import org.apache.spark.sql._
import java.util.Properties
import java.io.FileInputStream

object JSON_Generic_Solution {

  def main(args: Array[String])  {
    var prop: Properties = new Properties()
   // val inputFile = args(0)
   val inputFile = "G:\\TechnoGeeks\\Module-5\\Spark\\JSONGenericSolution\\products.json"
     //val outputPath = args(1)
    val outputPath = "G:\\TechnoGeeks\\Module-5\\Spark\\JSONGenericSolution\\output1"

    val conf = new SparkConf().setMaster("local").setAppName("jsonToCsv")
    val sc = new SparkContext(conf)
    
    //val acc =sc.accumulator(1)
    //acc.
    val sqlContext = new SQLContext(sc)

    try {
      val JsonDataFrame = sqlContext.read.json(inputFile)
     // val prop_file = args(2)
      val prop_file = "G:\\TechnoGeeks\\Module-5\\Spark\\JSONGenericSolution\\fields.txt"
      prop.load(new FileInputStream(prop_file))

      val jsonColumnsList = prop.getProperty("product_fields")
      if(jsonColumnsList == null){
        println("Error: jsonColumnsList not found..")
      }
      println("JSON Columns:"+jsonColumnsList)

      val jsonParse = JsonDataFrame.selectExpr(jsonColumnsList.split(","): _*).repartition(1)
        jsonParse.write.option("header", "true").csv(outputPath)
   
    }
    catch{
      case ex: NullPointerException => {
        println("Error: jsonColumnsList  are not populated..")
        System.exit(1)
      }
    }
    println("Execution completed..")
  }

}