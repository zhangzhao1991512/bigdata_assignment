import org.apache.spark.sql.SparkSession
//import spark.implicits._

//function with input string "text" that returns a list of strings
def getLinks (textIn: String) : List[String] = textIn match {
	case null => {List[String]()}
	case text => {
		val linkPattern = "\\[\\[(.*?)\\]\\]".r
		val matchList = linkPattern.findAllIn(text).toList //create a list of all elements that fit in between [[   ]]

		val blank = "\\[\\[\\s*\\]\\]".r //used to filter out blank entries
		val first_link_empty = "\\[\\[\\s*\\|+.*\\]\\]".r //used to filter out entries that are blank before the pipe
		val extra = "\\|.*".r //used to remove any entries after the pipe

		val category = "Category:".r

		//filter out all invalid cases
		val matchList = for {
			link <- matchList
			if(blank.findFirstIn(link) == None && first_link_empty.findFirstIn(link) == None 
						   && !link.contains("#") && (!link.contains(":") || category.findFirstIn(link) != None))
		} yield link.replaceAll("[", "").replaceAll("]", "").replaceAll(extra, "").toLowerCase
		
		//remove all empty elements from the list
		val newListNoEmpty = for {
			link <- newList
			if link.nonEmpty
		} yield link
		return newListNoEmpty
	}
}


object Project1 {
  def main(args: Array[String]) {
    val appleFile = "hdfs://clnode163.clemson.cloudlab.us:8020/p1/Apple.xml" // Should be some file on your system
    val spark = SparkSession.builder.appName("Project1").getOrCreate()
  
    import spark.implicits._

    val df0 = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load(appleFile)

	//val selectedData = df.select("author", "_id")
    //selectedData.write
      //.format("com.databricks.spark.xml")
      //.option("rootTag", "books")
      //.option("rowTag", "book")
      //.save("newbooks.xml") 
	df.printSchema()


    df1 =  df0.select("title","revision.text")
    df1.show()

    spark.stop()
  }
}
