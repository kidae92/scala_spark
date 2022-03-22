package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, round}
import org.apache.spark.sql.{SparkSession, functions}

object FriendsByAgeDataset {

  case class People(id:Int, name:String, age:Int, friends:Int)

  def main(arg: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv("data/fakefriends.csv")
      .as[People]

    val friendsByAge = ds.select("age", "friends")

    friendsByAge.groupBy("age").avg("friends").sort("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"), 2)).sort("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"),2).alias("friends_avg")).sort("age").show()




  }

}
