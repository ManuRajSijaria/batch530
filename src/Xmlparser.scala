import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Xmlparser {
  def main(args:Array[String]){
     val spark = SparkSession
      .builder.appName("abc").master("local[*]")
      .config("spark.sql.warehouse.dir", "G:\\TechnoGeeks\\Module-5\\Spark\\SparkSql\\temp")
      .getOrCreate()
    val products = spark.read.format("com.databricks.spark.xml").option("rowTag","product").
    load("F:\\TechnoGeeks\\Module-5\\Spark\\XMLParser\\products.xml")

products.show(2)
val test = products.select("Description","createdDate")
test.show(2)
                          test.persist()
                          test.cache()
products.createOrReplaceTempView("prod")
val output = spark.sql("select * from prod")
//val output = spark.sql("select Description,createdDate from prod")
output.show()
  }
}