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
    //val schema = StructType(
      //StructField("article", StringType, false) ::
      //StructField("rank", DoubleType, false) :: Nil)

//	val sparkConf = new SparkConf().setAppName("FileReceiver")
//	val spark = new SparkContext(sparkConf)
	val spark = SparkSession.builder.appName("FileReceiver").getOrCreate()


    // Use the business object that describes the dataset
    //case class Rank(article: String, rank: Double)

    //import org.apache.spark.sql.Encoders
    //schema = Encoders.product[Rank].schema

      val userSchema = new StructType().add("article", "string").add("rank", "double")

      val rank = spark.
	readStream.
        schema(userSchema).
	text("hdfs://clnode140.clemson.cloudlab.us:8020/p1/q10input/")
//      textFile("hdfs://clnode140.clemson.cloudlab.us:8020/p1/q10input/")

//    val record = rank.flatMap(_.split(","))
//    val large_ranks = record.filter("rank <= 0.5")

//    println(rank.isStreaming)

  }
}
