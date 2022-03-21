package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {
  def parseline(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val numfriends = fields(3).toInt

    (age, numfriends)
  }

  def main(arg: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    val rdd = lines.map(parseline)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val averageByAge = totalsByAge.mapValues(x => x._1/x._2)

    val results = averageByAge.collect()

    results.sorted.foreach(println)
  }
}
