import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
;

public class ml {

    public static void main(String[] args) {

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

        // Check the data and print the first 10 lines;
        raw_rdd.take(10).forEach(line -> System.out.println(line));

        // Removing the space at the beginning and separate by comma
        JavaRDD<String[]> rdd = raw_rdd.map(line -> line.substring(1).split("\\s+"));
        
        // Printing the the first 10 arrays of the list in the rdd
        rdd.take(10).forEach(line->System.out.println(Arrays.toString(line)));

        // Finding the length of the arrays:

        // int col_len = rdd.take(1).get(0).length;
        // OR
        int col_num = rdd.first().length;

        // Convert JavaRDD<String[]> to JavaRDD<Row>
        //JavaRDD<Row> data = rdd.map(parts -> RowFactory.create(parts[0], parts[1], parts[3]));

        JavaRDD<Row> data = rdd.map(parts -> {
            
            String[] values = new String[col_num];
            for (int i = 0; i < col_num; i++){
                    values[i] = parts[i];
            }
            return RowFactory.create(values);
            
        });
        


        // Define the schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("crim", DataTypes.StringType, true),
                DataTypes.createStructField("zn", DataTypes.StringType, true),
                DataTypes.createStructField("indus", DataTypes.StringType, true),
                DataTypes.createStructField("chas", DataTypes.StringType, true),
                DataTypes.createStructField("nox", DataTypes.StringType, true),
                DataTypes.createStructField("rm", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true),
                DataTypes.createStructField("dis", DataTypes.StringType, true),
                DataTypes.createStructField("rad", DataTypes.StringType, true),
                DataTypes.createStructField("tax", DataTypes.StringType, true),
                DataTypes.createStructField("ptratio", DataTypes.StringType, true),
                DataTypes.createStructField("b_1000", DataTypes.StringType, true),
                DataTypes.createStructField("lstat", DataTypes.StringType, true),
                DataTypes.createStructField("medv", DataTypes.StringType, true),
                
        });

        // Create DataFrame
        Dataset<Row> df = spark.createDataFrame(data, schema);
        df = df.select(col("crim").cast("double"),
                       col("zn").cast("double"),
                       col("indus").cast("double"),
                       col("chas").cast("double"),
                       col("nox").cast("double"),
                       col("rm").cast("double"),
                       col("age").cast("double"),
                       col("dis").cast("double"),
                       col("rad").cast("double"),
                       col("tax").cast("double"),
                       col("ptratio").cast("double"),
                       col("b_1000").cast("double"),
                       col("lstat").cast("double"),
                       col("medv").cast("double"));

        // Print DataFrame schema

        
        df.printSchema();
        df.show();

        //count the number of cities near the river and those not near the river:
        df.groupBy("chas").count().show();

        //count by number of occurrences the average number of rooms per dwelling:
        df.groupBy("rm").count().sort(desc("count")).show();

        //display the different modalities of the variable RAD:
        df.select("rad").distinct().show();



         
        
    }
}