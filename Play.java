// $example on:programmatic_schema$
import java.util.ArrayList;
import java.util.List;
// $example off:programmatic_schema$
// $example on:create_ds$
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;
// $example off:create_ds$

// $example on:schema_inferring$
// $example on:programmatic_schema$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
// $example off:programmatic_schema$
// $example on:create_ds$
import org.apache.spark.api.java.function.MapFunction;
// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:programmatic_schema$
// $example off:create_df$
// $example off:run_sql$
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
// $example off:create_ds$
// $example off:schema_inferring$
import org.apache.spark.sql.RowFactory;
// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:init_session$
// $example on:programmatic_schema$
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off:programmatic_schema$
import org.apache.spark.sql.AnalysisException;

// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;
// $example off:untyped_ops$

import org.apache.spark.SparkContext;  //?????????????????????
import org.apache.spark.sql.*;
import com.databricks.spark.xml.XmlOptions;


public class Play {


	String file = "/users/jackzh/Apple.xml";
    String booksFileTag = "book";

	public static class Page implements Serializable {
		String title;
		String text;

		public String getTitle() {
	      return title;
	    }

	    public void setTitle(String title) {
	      this.title = title;
	    }

	    public String getText() {
	      return text;
	    }

	    public void setTitle(String title) {
	      this.title = title;
	    }
	}


	public static void main(String[] args) {

		// $example on:init_session$
	    SparkSession spark = SparkSession
	      .builder()
	      .appName("Java Test")
	      .getOrCreate();
	    // $example off:init_session$
	    findLinks(spark);


	    // runBasicDataFrameExample(spark);
	    // runDatasetCreationExample(spark);
	    // runInferSchemaExample(spark);
	    // runProgrammaticSchemaExample(spark);

	    spark.stop();
	}

	private static void findLinks(SparkSession spark) throws AnalysisException {
	
		// HashMap<String, String> options = new HashMap<String, String>();
  //       options.put("rowTag", booksFileTag);
  //       options.put("path", booksFile);

  //       //options.put("rowTag", Page)

  //       Dataset df = spark.read("com.databricks.spark.xml", options);





        DataFrame df = spark.read()
        .format("com.databricks.spark.xml")
	    .option("rowTag", "page")
	    .load(File);

		// df.select("author", "_id").write()
	 //    	.format("com.databricks.spark.xml")
	 //    	.option("rootTag", "books")
	 //    	.option("rowTag", "book")
	 //    	.save("newbooks.xml");
	    df.show();

	    df.printSchema();







	}

}
