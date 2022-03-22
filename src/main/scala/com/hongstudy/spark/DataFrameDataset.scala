package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    people.printSchema()

    people.select("name").show()

    people.groupBy("age").count().show()

    people.filter(people("age")<21).show()

    people.select(people("name"), people("age") + 10).show()

    spark.stop()

  }

}
