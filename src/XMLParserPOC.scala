

										import org.apache.spark.SparkContext
										import org.apache.spark.sql.SparkSession

										object XMLParserPOC {

										  def main(args: Array[String]) {
											import org.apache.log4j._
											Logger.getLogger("org").setLevel(Level.OFF)

											val spark1 = SparkSession.builder.appName("XMLParser").master("local[2]").
											  config("spark.sql.warehouse.dir", "/user/hive/warehouse").
											  config("hive.metastore.uris", "thrift://127.0.0.1:9083").enableHiveSupport().getOrCreate()

											val customers = spark1.read.format("com.databricks.spark.xml").option("rowTag", "customer").load("/home/cloudera/POC3/XML/cust*")

											val test = customers.select("custno", "firstname", "lastname", "gender", "age", "profession", "contactno", "emailid", "city", "state", "isactive", "createddate", "updateddate")

											test.createOrReplaceTempView("view_customers")

											spark1.sql("insert into poc3_xml.customers select * from view_customers")
										  }
										}