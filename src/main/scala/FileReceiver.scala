// The following example executes a streaming query over CSV files
// CSV format requires a schema before you can start the query

// You could build your schema manually
import org.apache.spark.sql.types._
object FileReceiver {
  def main(args: Array[String]) {
    val schema = StructType(
      StructField("article", StringType, false) ::
      StructField("rank", DoubleType, false) :: Nil)



    // ...but is error-prone and time-consuming, isn't it?

    // Use the business object that describes the dataset
    case class Rank(article: String, rank: Double)

    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[Rank].schema

    val rank = spark.
      readStream.
      schema(schema).
      text("path/to/input").
      as[Rank]

    val record = rank.flatMap(_.split("\t"))

    // ...but it is still a Dataset.
    // (Almost) any Dataset operation is available
    // val population = people.
    //   groupBy('city).
    //   agg(count('city) as "population")

    val large_ranks = record.filter("rank <= 0.5")

    println(rank.isStreaming)


    // Start the streaming query
    // Write the result using console format, i.e. print to the console
    // Only Complete output mode supported by groupBy
    // import scala.concurrent.duration._
    // import org.apache.spark.sql.streaming.{OutputMode, Trigger}
    // val populationStream = population.
    //   writeStream.
    //   format("console").
    //   trigger(Trigger.ProcessingTime(30.seconds)).
    //   outputMode(OutputMode.Complete).
    //   queryName("textStream").
    //   start
  }
}
