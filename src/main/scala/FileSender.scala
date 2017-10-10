import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object FileSender {


  def main(args: Array[String]) {

      val spark = SparkSession.builder.appName("FileSender").getOrCreate()

      import spark.implicits._

      val defaultSchema = new StructType().add("value","string")

      val rank = spark.
        readStream.
        text("hdfs://clnode140.clemson.cloudlab.us:8020/p1/outputRanks11/")

    val query = rank.writeStream
        .outputMode("append")
        .format("text")
        .option("checkpointLocation", "hdfs://clnode140.clemson.cloudlab.us:8020/p1/checpointTest/")
        .option("path", "hdfs://clnode140.clemson.cloudlab.us:8020/p1/q11ranksFinalFinal/")
        .start()

     query.awaitTermination()
  }
}
  
