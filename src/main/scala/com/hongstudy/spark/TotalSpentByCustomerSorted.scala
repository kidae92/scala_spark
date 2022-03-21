package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalSpentByCustomerSorted {

  def extractCustomerPricePairs(line:String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)

  }

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentByCustomerSorted")

    val input = sc.textFile("data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val totalByCustomer = mappedInput.reduceByKey((x,y) => x+y)

    val flipped = totalByCustomer.map(x => (x._2, x._1))

    val totalByCustomerSorted = flipped.sortByKey()

    val results = totalByCustomerSorted.collect()

    val test = results.map(x => (x._2, x._1))

    test.foreach(println)

  }

}
