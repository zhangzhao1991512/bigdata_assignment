import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.DataFrame
//import spark.implicits._

object Project1 {

	//function with input string "text" that returns a list of strings
	def getLinks (textIn: String) : List[String] = textIn match {
		case null => {List[String]()}
		case text => {
			val linkPattern = "\\[\\[(.*?)\\]\\]".r
			val linkList = linkPattern.findAllIn(text).toList //create a list of all elements that fit in between [[   ]]

			val blank = "\\[\\[\\s*\\]\\]".r //used to filter out blank entries
			val first_link_empty = "\\[\\[\\s*\\|+.*\\]\\]".r //used to filter out entries that are blank before the pipe
			val extra = "\\|.*" //used to remove any entries after the pipe

			val category = "\\[\\[Category:".r

			//filter out all invalid cases
			val linkListFiltered = for {
				link <- linkList
				if(blank.findFirstIn(link) == None && first_link_empty.findFirstIn(link) == None 
						   && !link.contains("#") && (!link.contains(":") || category.findFirstIn(link) != None))
			} yield link.replace("[", "").replace("]", "").replaceAll(extra, "").toLowerCase
		
			//remove all empty elements from the list
			val linkListNoEmpty = for {
				link <- linkListFiltered
				if !link.isEmpty
			} yield link
			return linkListNoEmpty
		}
	}	


	val udfGetLinks = udf[List[String], String](getLinks)

	def main(args: Array[String]) {
		var startTime = System.nanoTime

		//val appleFile = "hdfs://clnode163.clemson.cloudlab.us:8020/p1/Apple.xml" // Should be some file on your system
		//val appleFile = "hdfs://clnode163.clemson.cloudlab.us:8020/p1/enwiki-20110115-pages-articles1.xml" // Should be some file on your system
		//val appleFile = "hdfs://clnode163.clemson.cloudlab.us:8020/p1/q4/enwiki-20110115-pages-articles1.xml" // Should be some file on your system	
		//val appleFile = "/users/jackzh/bigdata_assignment/enwiki-20110115-pages-articles1.xml"

		val appleFile = "hdfs://clnode163.clemson.cloudlab.us:8020/p1/wholeRep2/enwiki-20110115-pages-articles_whole.xml" // Should be some file on your system


//		val conf = new SparkConf()
//		.setMaster("local[2]")
//		.setExecutor.memory("5g")


		val spark = SparkSession.builder.appName("Project1").getOrCreate()
  
		import spark.implicits._

		val df0 = spark.read
      		.format("com.databricks.spark.xml")
     		.option("rowTag", "page")
                .option("excludeAttribute", true)
		.load(appleFile)
	
		//df0.printSchema()


		val df1 =  df0.select("title","revision.text")
		//df1.show()

		val df2 = df1.withColumn("linkList", udfGetLinks('text))
		//df2.show()
		//df2.select("linkList").show()
		//df2.printSchema()	
		val df3 = df2.withColumn("links",explode(df2("linkList")))
		//df3.show()

                val df4 = df3.select("title", "links")
		df4.write.option("sep","\t").csv("hdfs://clnode163.clemson.cloudlab.us:8020/p1rep2/out_dir6_whole")
	//	df4.write
        //        .format("com.databricks.spark.csv")
	//	.option("delimiter","\t")
	//	.option("path","hdfs://clnode163.clemson.cloudlab.us:8020/p1")
	//	.save("clusterLinks3.csv")

		var timeElapsed = System.nanoTime - startTime
		printf("Time elapsed = %d\n", timeElapsed)
	
		spark.stop()
	}
}
