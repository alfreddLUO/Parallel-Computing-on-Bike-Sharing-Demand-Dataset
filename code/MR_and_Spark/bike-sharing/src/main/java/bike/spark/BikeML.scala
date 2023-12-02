package bike.spark

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * @author Joe
 * @since 2023/11/19 13:57
 */
object BikeML {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BikeML")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(Array(
      StructField("datetime", StringType, nullable = true),
      StructField("season", IntegerType, nullable = true),
      StructField("holiday", IntegerType, nullable = true),
      StructField("workingday", IntegerType, nullable = true),
      StructField("weather", IntegerType, nullable = true),
      StructField("temp", DoubleType, nullable = true),
      StructField("atemp", DoubleType, nullable = true),
      StructField("humidity", IntegerType, nullable = true),
      StructField("windspeed", StringType, nullable = true),
      StructField("casual", IntegerType, nullable = true),
      StructField("registered", IntegerType, nullable = true),
      StructField("count", StringType, nullable = true)
    ))

    val train = spark.read.format("csv")
      .schema(schema)
      .load("hdfs://master:9000/output/train/part-r-00000")

    val test = spark.read.format("csv")
      .schema(schema)
      .load("hdfs://master:9000/output/test/part-r-00000")

    val trainDF = train.withColumn("count", split(col("count"), "\t").getItem(0))
      .withColumn("count", col("count").cast(IntegerType))
      .withColumn("date", to_date(col("datetime")))
      .withColumn("hour", hour(col("datetime")))
      .withColumn("year", year(col("datetime")))
      .withColumn("weekday", dayofweek(col("datetime")))
      .withColumn("month", month(col("datetime")))
      .withColumn("windspeed", col("windspeed").cast(DoubleType))
      .select("date", "hour", "year", "month", "weekday", "season", "holiday", "workingday", "weather", "temp", "atemp", "humidity", "windspeed", "casual", "registered", "count")

    val testDF = test.withColumn("windspeed", split(col("windspeed"), "\t").getItem(0))
      .withColumn("windspeed", col("windspeed").cast(DoubleType))
      .withColumn("date", to_date(col("datetime")))
      .withColumn("hour", hour(col("datetime")))
      .withColumn("year", year(col("datetime")))
      .withColumn("weekday", dayofweek(col("datetime")))
      .withColumn("month", month(col("datetime")))
      .withColumn("count", lit(0))
      .withColumn("casual", lit(0))
      .withColumn("registered", lit(0))
      .select("date", "hour", "year", "month", "weekday", "season", "holiday", "workingday", "weather", "temp", "atemp", "humidity", "windspeed", "casual", "registered", "count")

    trainDF.write.mode("overwrite")
      .format("hive")
      .option("header", "true")
      .option("path", "hdfs://master:9000/user/hive/warehouse/bike/train")
      .option("dbtable", "default.bike_train") // Hive 表的名称，格式为 "database.table"
      .saveAsTable("bike_train")

    testDF.write.mode("overwrite")
      .format("hive")
      .option("header", "true")
      .option("path", "hdfs://master:9000/user/hive/warehouse/bike/test")
      .option("dbtable", "default.bike_test") // Hive 表的名称，格式为 "database.table"
      .saveAsTable("bike_test")

    //Linear Regression Model
    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month", "hour", "weekday", "season", "holiday", "workingday", "weather", "temp", "atemp", "humidity", "windspeed"))
      .setOutputCol("features")

    val trainDataVectorized = assembler.transform(trainDF)
    val testDataVectorized = assembler.transform(testDF)

    //Feature Correlation
    val correlation: Matrix = Correlation.corr(trainDataVectorized, "features").head().getAs[Matrix](0)
    println(s"Correlation matrix: \n $correlation")

    // Linear Regression ========== Start
    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("count")
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lrModel = lr.fit(trainDataVectorized)
    val lrPredictions = lrModel.transform(testDataVectorized)

    val lrEvaluator = new RegressionEvaluator()
      .setLabelCol("count")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val lrRmse = lrEvaluator.evaluate(lrPredictions)
    println(s"Linear Regression R2 = $lrRmse")

    // Save to Hive
    lrPredictions.select("date", "count", "prediction")
      .groupBy("date")
      .agg(sum("count").as("count"), sum("prediction").as("prediction"))
      .orderBy("date")
      .write.mode("overwrite")
      .format("hive")
      .option("header", "true")
      .option("path", "hdfs://master:9000/user/hive/warehouse/bike/predict/lr")
      .option("dbtable", "default.bike_lr_predict")
      .saveAsTable("bike_lr_predict")
    // Linear Regression ========== End

    // Gradient Boost Tree ========== Start
    val gbt = new GBTRegressor()
      .setLabelCol("count")
      .setFeaturesCol("features")
      .setMaxIter(10)
    val gbtModel = gbt.fit(trainDataVectorized)
    val gbtPredictions = gbtModel.transform(testDataVectorized)

    val gbtEvaluator = new RegressionEvaluator()
      .setLabelCol("count")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val gbtRmse = gbtEvaluator.evaluate(gbtPredictions)
    println(s"Gradient Boost Tree RMSE = $gbtRmse")

    // save to Hive
    gbtPredictions.select("date", "count", "prediction")
      .groupBy("date")
      .agg(sum("count").as("count"), sum("prediction").as("prediction"))
      .orderBy("date")
      .write.mode("overwrite")
      .format("hive")
      .option("header", "true")
      .option("path", "hdfs://master:9000/user/hive/warehouse/bike/predict/gbt")
      .option("dbtable", "default.bike_gbt_predict")
      .saveAsTable("bike_gbt_predict")
    // Gradient Boost Tree ========== End

    // Random Forest ========== Start
    val rf = new RandomForestRegressor()
      .setLabelCol("count")
      .setFeaturesCol("features")
    val rfModel = rf.fit(trainDataVectorized)
    val rfPredictions = rfModel.transform(testDataVectorized)

    val rfEvaluator = new RegressionEvaluator()
      .setLabelCol("count")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rdRmse = rfEvaluator.evaluate(rfPredictions)
    println(s"Random Forest RMSE = $rdRmse")

    //Save to Hive
    rfPredictions.select("date", "count", "prediction")
      .groupBy("date")
      .agg(sum("count").as("count"), sum("prediction").as("prediction"))
      .orderBy("date")
      .write.mode("overwrite")
      .format("hive")
      .option("header", "true")
      .option("path", "hdfs://master:9000/user/hive/warehouse/bike/predict/rf")
      .option("dbtable", "default.bike_rf_predict")
      .saveAsTable("bike_rf_predict")
    // Random Forest ========== End

  }
}
