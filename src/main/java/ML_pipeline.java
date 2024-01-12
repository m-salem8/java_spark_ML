import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ML_pipeline {
    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("ML_pipeline")
                .master("local")
                .getOrCreate();

        // Initiate Spark context from Spark session
        SparkContext sc_raw = spark.sparkContext();

        // Create Java Spark context from Spark context
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sc_raw);

        // Load the data into JavaRDD
        JavaRDD<String> raw_rdd = sc.textFile("src/main/java/housing.data");

        Dataset<Row> df = RddToDataFrameConvertor.convertRDDToDataFrame(raw_rdd);
        df.show();


    }
}
