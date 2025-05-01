import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object SparkSQL {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.OFF)
  
    val spark = SparkSession
      .builder.appName("abc").master("local[*]")
      .config("spark.sql.warehouse.dir", "F:\\TechnoGeeks\\Module-5\\Spark\\SparkSql\\temp")
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("F:\\TechnoGeeks\\Module-5\\Spark\\SparkSql\\fakefriends.csv")
    val people = lines.map(mapper)
    
    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = people.toDS
    
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")
    
    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
    
  }
  
}
