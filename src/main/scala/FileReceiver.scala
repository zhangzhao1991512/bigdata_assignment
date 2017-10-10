// The following example executes a streaming query over CSV files
// CSV format requires a schema before you can start the query

// You could build your schema manually
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Column 

object FileReceiver {

def getTitle(textIn: String) : String = textIn match {
    case null => ""
    case text => {
     
      val extra = "\\,.*" //used to remove any entries after comma
      val parenthesis = "("
      val textNew = text.substring(1).replaceAll(extra, "")
      return textNew;
  }
} 

def getRank(textIn: String) : Double = textIn match {
    case null => 0
    case text => {
      val before = ".*\\,"
      val rankString = text.replace(")","").replaceAll(before, "")
      return rankString.toDouble
    }
}

  val udfGetTitles = udf[String, String](getTitle)
  val udfGetRanks = udf[Double, String](getRank)

  def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("FileReceiver").getOrCreate()

      import spark.implicits._
      
      val defaultSchema = new StructType().add("value","string")

      val rank = spark.
        readStream.
	schema(defaultSchema).
        text("hdfs://clnode140.clemson.cloudlab.us:8020/p1/q11ranksFinalFinal/")
    
    val rankWithTitles = rank.withColumn("title", udfGetTitles('value))
    val titleAndRanks = rankWithTitles.withColumn("rank", udfGetRanks('value))

    val filterRanks = titleAndRanks.filter("rank > 0.5")

    val query = filterRanks.select("title","rank").writeStream
        .outputMode("append")
        .format("csv")
	.option("sep","\t")
        .option("checkpointLocation", "hdfs://clnode140.clemson.cloudlab.us:8020/p1/checpointTest/")
	.option("path", "hdfs://clnode140.clemson.cloudlab.us:8020/p1/q11outputFinalFinal/")
	.start()

     query.awaitTermination()
  }
}
