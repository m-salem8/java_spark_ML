import static org.apache.spark.sql.functions.substring;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;




public class RddToDataFrameConvertor {
    public static Dataset<Row> convertRDDToDataFrame(JavaRDD<String> rawRDD){
        JavaRDD<String[]> rdd = rawRDD.map(line -> line.substring(1).split("\\s+"));

        // integer represents the length of the fitst raw
        int col_num = rdd.first().length;

        
        // Convert JavaRDD<String[]> to JavaRDD<Row>
        //JavaRDD<Row> data = rdd.map(parts -> RowFactory.create(parts[0], parts[1], parts[3]));

        JavaRDD<Row> data = rdd.map(lines -> {
            String[] values = new String[col_num];
            for (int i=0; i<col_num;i++){
                values[i] = lines[i];
            }
            return RowFactory.create(values);
        });


        //Schema Definition
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
        SparkSession spark = SparkSession.builder()
                                         .appName("convertRDDToDataFrame")
                                         .getOrCreate();

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
        return df;

    }
    
}
