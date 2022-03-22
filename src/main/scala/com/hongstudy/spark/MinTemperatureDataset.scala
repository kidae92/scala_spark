package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types.{StringType, StructType, FloatType, IntegerType}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._


object MinTemperatureDataset {

  case class Temperature(StationID:String, date:Int, measure_type:String, temperature:Float)

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MinTemperature")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temperature", FloatType, nullable = true)


    import spark.implicits._
    val ds = spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[Temperature]

    val minTemps = ds.filter($"measure_type" === "TMIN")
    val stationTemps = minTemps.select("stationID", "temperature")
    val minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
    val minTempsBtStationF = minTempsByStation.withColumn("temperature", functions.round($"min(temperature)" * 0.1f * (9.0f / 5.0f)+ 32.0f, 2))
      .select("stationID", "temperature").sort("temperature")

    val results = minTempsBtStationF.collect()

    for (result <- results) {
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimun temperature: $formattedTemp")
    }

  }

}
