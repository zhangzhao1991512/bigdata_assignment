// The following example executes a streaming query over CSV files
// CSV format requires a schema before you can start the query

// You could build your schema manually
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object FileReceiver {
  def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("FileReceiver").getOrCreate()


      val defaultSchema = new StructType().add("value","string")

      val rank = spark.
        readStream.
	schema(defaultSchema).
        text("hdfs://clnode140.clemson.cloudlab.us:8020/p1/q10input/")

      
 //     val rank = spark.
 //       read.
 //       text("hdfs://clnode140.clemson.cloudlab.us:8020/p1/q10input/")

	rank.printSchema()
	
// val largeRanks = rank.filter("rank <= 0.5")

    println(rank.isStreaming)

     val query = rank.writeStream
        .outputMode("append")
        .format("console")
        .start()

     query.awaitTermination()

  }
}
