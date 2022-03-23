package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object PopularMoviesDataset {

  final case class Movie(movieID: Int)

  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val movieDS = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movie]

    val topMovieIDs = movieDS.groupBy("movieID").count().orderBy(desc("count"))

    topMovieIDs.show(10)

    spark.stop()
  }

}
