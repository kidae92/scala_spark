package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalSpentByCustomer {

  def extractCustomerPricePairs(line:String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)

  }

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")

    val input = sc.textFile("data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val totalByCustomer = mappedInput.reduceByKey((x,y) => x+y)

    val results = totalByCustomer.collect()

    results.foreach(println)

  }

}
