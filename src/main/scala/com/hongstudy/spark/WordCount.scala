package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {

  def main(arg: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val input = sc.textFile("data/book.txt")

    val words = input.flatMap(x => x.split(' '))

    val wordCounts = words.countByValue()

    wordCounts.foreach(println)

  }

}
