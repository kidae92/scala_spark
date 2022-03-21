package com.hongstudy.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.min

object MinTemperatures {

  def parseLine(line:String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  def main(arg: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Mintemperatures")

    val lines = sc.textFile("data/1800.csv")

    val parsedLines = lines.map(parseLine)

    val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    val stationTemps = minTemps.map( x => (x._1, x._3.toFloat))

    val minTempsByStation = stationTemps.reduceByKey((x,y) => math.min(x,y))

    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"

      println(s"$station minimum temperature: $formattedTemp")
    }
  }

}
