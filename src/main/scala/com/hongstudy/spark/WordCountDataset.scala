package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

object WordCountDataset {

  case class Book(value: String)

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    val words = input.select(explode(split($"value", "\\W+")).alias("word")).filter($"word" =!= "")

    val wordCounts = words.groupBy("word").count()

    wordCounts.show(wordCounts.count.toInt)


  }

}
