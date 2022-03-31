package com.hongstudy.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._


object LinearRegressionDataFrameDataset {

  case class RegressionSchema(label: Double, features_raw: Double)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("LinearRegressionDF")
      .master("local[*]")
      .getOrCreate()

    val regressionSchema = new StructType()
      .add("label", DoubleType, nullable = true)
      .add("features_raw", DoubleType, nullable = true)

    import spark.implicits._

    val dsRaw = spark.read
      .option("sep", ",")
      .schema(regressionSchema)
      .csv("data/regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler()
      .setInputCols(Array("features_raw"))
      .setOutputCol("features")
    val df = assembler.transform(dsRaw)
      .select("label", "features")

    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainDF = trainTest(0)
    val testDF = trainTest(1)

    var lir = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(100)
      .setTol(1E-6)

    val model = lir.fit(trainDF)

    val fullPrediction = model.transform(testDF).cache()

    val predictionAndLabel = fullPrediction.select("prediction", "label").collect()

    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    spark.stop()
  }

}
