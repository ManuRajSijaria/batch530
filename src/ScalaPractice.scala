





import org.apache.spark.SparkContext
object ScalaPractice {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")
    val input = sc.textFile("F:\\TechnoGeeks\\Module-5\\Spark\\TotalSpentByCustomer\\customer-orders.csv")
    input.foreach(println)
  }
}