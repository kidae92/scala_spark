package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

import java.sql.Struct
import scala.io.{Codec, Source}

object PopularMoviesNicerDataset {

  case class Movies(userID:Int, movieID:Int, rating:Int, timestamp:Long)

  /** Load up a Map of movie IDs to movie names. */

  def loadMovieNames(): Map[Int, String] = {

    implicit val codec: Codec = Codec("ISO-8859-1")

    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("data/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("PopularMoviesNicer")
      .master("local[*]")
      .getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
      val movies = spark.read
        .option("sep", "\t")
        .schema(moviesSchema)
        .csv("data/ml-100k/u.data")
        .as[Movies]

    val movieCounts = movies.groupBy("movieID").count()

    val lookupName : Int => String = (movieID: Int) => (nameDict.value(movieID))

    val lookupNameUDF = udf(lookupName)

    val moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(col("movieID")))

    val sortedMovieWithNames = moviesWithNames.sort("count")

    sortedMovieWithNames.show(sortedMovieWithNames.count.toInt, truncate = false)



    }




}
