package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession

object RealEstate {

  case class RegressionSchema(No:Int, TransactionDate:Double, HouseAge:Double, DistanceToMRT:Double,
                              NumberConvenienceStores:Int, Latitude:Double, Longitude:Double, PriceOfUnitArea:Double)

  def main(args:Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("RealEstate")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dsRaw = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/realestate.csv")
      .as[RegressionSchema]

    val assembler = new VectorAssembler()
      .setInputCols(Array("HouseAge", "DistanceToMRT", "NumberConvenienceStores"))
      .setOutputCol("features")

    val df = assembler.transform(dsRaw)
      .select("PriceOfUnitArea", "features")

    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainDF = trainTest(0)
    val testDF = trainTest(1)

    val dtr = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")

    val model = dtr.fit(trainDF)

    val fullPredictions = model.transform(testDF).cache()

    val predictionAndLabel = fullPredictions.select("prediction", "PriceOfUnitArea")

    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
    spark.stop()
  }


}
