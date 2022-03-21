package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCountBetter {

  def main(arg: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCountBetter")

    val input = sc.textFile("data/book.txt")

    val words = input.flatMap(x => x.split("\\W+"))

    val lowercaseWords = words.map(x => x.toLowerCase())

    val wordCounts = lowercaseWords.countByValue()

    wordCounts.foreach(println)
  }

}
