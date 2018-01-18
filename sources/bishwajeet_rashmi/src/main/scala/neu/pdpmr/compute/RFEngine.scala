package neu.pdpmr.compute

import org.apache.spark.ml.feature.{Imputer, RFormula, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql._
import SongColnames._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.sql.functions.col

/**
  * @author deyb(dey.b@husky.neu.edu)
  */
object RFEngine {

   val schemaType: StructType = (new StructType)
    .add(SongColnames.NORM_TITLE, StringType)
    .add(SongColnames.NORM_ARTISTNAME, StringType)
    .add(SongColnames.MEAN_PRICE, DoubleType)
    .add(SongColnames.NORM_SONG_HOTNESS, DoubleType)

    .add(SongColnames.ARTIST_HOTNESS, DoubleType)
    .add(SongColnames.ARTIST_FAMILIARITY, DoubleType)
    .add(SongColnames.LOUDNESS, DoubleType)

    .add(SongColnames.DOW_CONF, DoubleType)
    .add(SongColnames.BIN_PLAYS, IntegerType)


  def main(args: Array[String]): Unit = {
    val dfFile = args(0)
    val master = args(1)
    val output = args(2)

    val spark = SparkSession.builder
      .master(master)
      .appName("RFEngine")
      //.config("spark.driver.memory", "4g")
      //.config("spark.executor.memory", "4g")
      .getOrCreate

    for (depth <- Array(10, 15, 20, 25)) {
      for (numTrees <- Array(100, 150, 200, 250, 300)) {
        println(s"Starting numTrees=$numTrees, depth=$depth")

        val df = read(dfFile, spark)

        val formula = new RFormula()
          .setFormula(s"${DOW_CONF} ~ ${MEAN_PRICE} + ${NORM_SONG_HOTNESS} + ${ARTIST_HOTNESS} + ${BIN_PLAYS} + ${ARTIST_FAMILIARITY} + ${LOUDNESS}")
          .setFeaturesCol("features")
          .setLabelCol("label")

        val cleanedDF = df.filter(df.col(DOW_CONF).isNotNull)

        val featureDF = formula.fit(cleanedDF).transform(cleanedDF)

        val Array(trainingData, testData) = featureDF.randomSplit(Array(0.9, 0.1))

        val rfRegressor = new RandomForestRegressor()
          //.setMaxBins(32)
          .setFeatureSubsetStrategy("auto")
          .setImpurity("variance")
          .setMaxDepth(depth)
          .setNumTrees(numTrees)
          .setFeaturesCol("features")
          .setLabelCol("label")


        val evaluator = new RegressionEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          //     "rmse" (default): root mean squared error
          //     "mse": mean squared error
          //     "r2": R2 metric
          //     "mae": mean absolute error
          .setMetricName("rmse")


        val model = rfRegressor.fit(trainingData) // train: DataFrame

        val metricsTrain = regressionMetrics(trainingData, model)
        val metricsTest = regressionMetrics(testData, model)
        val outputMetricsRDD = spark.sparkContext.parallelize(Seq(("numTrees", numTrees),
                                 ("depth", depth),

                                 ("train-variance", metricsTrain.explainedVariance),
                                 ("train-rmse", metricsTrain.rootMeanSquaredError),
                                 ("train-mse", metricsTrain.meanSquaredError),
                                 ("train-variance%", 1.0 - metricsTrain.meanSquaredError / metricsTrain.explainedVariance),

                                 ("test-variance", metricsTest.explainedVariance),
                                 ("test-rmse", metricsTest.rootMeanSquaredError),
                                 ("test-mse", metricsTest.meanSquaredError),
                                 ("test-variance%", 1.0 - metricsTest.meanSquaredError / metricsTest.explainedVariance)
                               ))
        val curOutputDir = s"$output/$numTrees-$depth"

        outputMetricsRDD.saveAsTextFile(s"$curOutputDir/metrics")
        model.save(s"$curOutputDir/model")

        println(s"Completed numTrees=$numTrees, depth=$depth")
      }

    }

  }

  private def regressionMetrics(trainingData: Dataset[Row], model: RandomForestRegressionModel): RegressionMetrics = {
    val predictions = model.transform(trainingData)

    val predictionAndLabels = predictions
      .select(predictions("prediction"), predictions("label"))
      .rdd
      .map { case Row(prediction: Double, label: Double) => (prediction, label) }

    new RegressionMetrics(predictionAndLabels)
  }

  def predict(model: RandomForestRegressionModel, spark: SparkSession, row: Row): Int = {
    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schemaType)

    val formula = new RFormula()
      .setFormula(s"${DOW_CONF} ~ ${MEAN_PRICE} + ${NORM_SONG_HOTNESS} + ${ARTIST_HOTNESS} + ${BIN_PLAYS} + ${ARTIST_FAMILIARITY} + ${LOUDNESS}")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val testFeatureDF = formula.fit(testDF).transform(testDF)

    val predictions = model.transform(testFeatureDF)

    val predictedDownload = predictions.select(predictions("prediction"))
      .rdd
      .map{case Row(prediction: Double) => prediction}
      .take(1)(0)

    predictedDownload.floor.toInt
  }

  private def read(dfFile: String, spark: SparkSession): DataFrame = {

    val reader = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .schema(schemaType)
      //.option("inferSchema", true)
      //.option("mode", "DROPMALFORMED")

      .load(dfFile)

    reader.describe().show()

    reader.printSchema()

    reader

  }
}
