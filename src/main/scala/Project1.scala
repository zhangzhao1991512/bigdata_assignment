import org.apache.spark.sql.SparkSession
//import spark.implicits._

object Project1 {
  def main(args: Array[String]) {
    val appleFile = "/p1/Apple.xml" // Should be some file on your system
    val spark = SparkSession.builder.appName("Project1").getOrCreate()
  
    import spark.implicits._

    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(appleFile)

    df.select("title").show()
	df.select("revision.text").show()
	//val selectedData = df.select("author", "_id")
    //selectedData.write
      //.format("com.databricks.spark.xml")
      //.option("rootTag", "books")
      //.option("rowTag", "book")
      //.save("newbooks.xml") 
	df.printSchema()

    spark.stop()
  }
}
