package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MostNotPopularSuperheroDataset {

  case class SuperheroNames(id:Int, name: String)
  case class Superhero(value:String)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MostNotPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperheroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[Superhero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) -1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val minConnectionCount = connections.agg(min("connections")).first().getLong(0)

    val minConnections = connections.filter($"connections" === minConnectionCount)

    val minConnectionWithNames = minConnections.join(names, usingColumn = "id")

    println("The following characters have only " + minConnectionCount + " connection(s):")
    minConnectionWithNames.select("name").show()
  }
}
