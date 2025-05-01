

object poc {
  
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext._

 
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("G:\\TechnoGeeks\\Module-5\\Spark\\a.txt")
    
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
    
    // Count up the occurrences of each word
    val wordCounts = words.countByValue()
    
    // Print the results.
    //wordCounts.foreach(println)
    
    val filter = words.filter(x =>x.contains("#"))
    filter.foreach(println)
    
   //(filter.first().charAt(0))
    val f1 =filter.filter(x=>x.charAt(0) == '#' && x.charAt(x.length() - 1) == '#')
    println("*--------------------------------------")
    f1.foreach(println)
    println("*--------------------------------------")
    
  }
  

}
