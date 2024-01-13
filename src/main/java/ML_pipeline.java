import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;



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
        // df.show();

        /*//count the number of cities near the river and those not near the river:
        df.groupBy("chas").count().show();

        //count by number of occurrences the average number of rooms per dwelling:
        df.groupBy("rm").count().sort(desc("count")).show();

        //display the different modalities of the variable RAD:
        df.select("rad").distinct().show();*/

        /*String[] colNames = df.columns();
        System.out.println(Arrays.toString(colNames)); */

     

        String[] colNames = df.columns();
        ArrayList<String> assembledCols = new ArrayList<>();

        for (String colName : colNames) {
            if (!colName.equals("medv")) {
                assembledCols.add(colName);
            }
        };
        String[] colsToAssembly = new String[assembledCols.size()];
        for (int i = 0;i<assembledCols.size();i++) {
            colsToAssembly[i] = assembledCols.get(i);
        }

        df = df.withColumnRenamed("medv", "label");
        

        //                          STEP BY STEP

        /*VectorAssembler assembler = new VectorAssembler()
                .setInputCols(colsToAssembly)
                .setOutputCol("features");
        
        Dataset<Row> df_assembled = assembler.transform(df);
        
        Dataset<Row>[] data_split = df_assembled.randomSplit(new double[] {0.8, 0.2}, 12345);
        Dataset<Row> trainData = data_split[0];
        Dataset<Row> testData = data_split[1];

        
        LinearRegression lr = new LinearRegression()
                                  .setLabelCol("label")
                                  .setFeaturesCol("features");

        LinearRegressionModel lrModel = lr.fit(trainData);
        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
        
        Dataset<Row> predictions = lrModel.transform(testData);
        predictions.select("features", "label", "prediction").show();*/

        
        //                              ML Pipeline

        // Definition of the Vector Assembler
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(colsToAssembly)
                .setOutputCol("features");

        // Definition of data standarizer
        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithMean(false)
                .setWithStd(true);

        // Definition of the regression model
        LinearRegression lr = new LinearRegression()
                                  .setLabelCol("label")
                                  .setFeaturesCol("scaledFeatures");

        // Creating the pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{assembler, scaler, lr});

        // Data Split
        Dataset<Row>[] data_split = df.randomSplit(new double[] {0.8, 0.2}, 12345);
        Dataset<Row> trainData = data_split[0];
        Dataset<Row> testData = data_split[1];

        // Define a parameter grid for the linear regression
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(lr.regParam(), new double[]{0.1, 0.01})
                .build();

        // Define the evaluator
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        // Create a CrossValidator
        CrossValidator crossValidator = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(3); // Specify the number of folds

        // Fit the CrossValidator to the training data
        CrossValidatorModel cvModel = crossValidator.fit(trainData);

        // Make predictions on the test data
        Dataset<Row> predictions = cvModel.transform(testData);

         // Show the predictions
        predictions.select("features", "label", "prediction").show();


        //                   Evaluation of models
        
        
        // Access the average metrics from the CrossValidatorModel
        double[] avgMetrics = cvModel.avgMetrics();

        // Print the metrics
        System.out.println("Average Metrics: " + Arrays.toString(avgMetrics));

        // Assuming you want to print the RMSE for each fold
        for (int i = 0; i < avgMetrics.length; i++) {
        System.out.println("Fold " + (i + 1) + " RMSE: " + avgMetrics[i]);
}

        // Alternatively, you can also directly use the evaluator to get the metric on the test data
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
        System.out.println(Arrays.toString(cvModel.avgMetrics()));


        /* IF WE WANT TO RAIN TH PIPE LINE WITHOUT THE CROSS EVALUATOR
        // Fit the pipeline on the training data
        PipelineModel pipelineModel = pipeline.fit(trainData);

        // Make predictions on the test data
        Dataset<Row> predictions = pipelineModel.transform(testData);*/

        // Show the predictions
        //predictions.select("features", "label", "prediction").show();


        


        
        // PipelineModel model = Pipeline.fit(trainData);
        // Dataset<Row> preditions = model.transform(testData);

        // preditions.show();

        spark.stop();
    }
}
