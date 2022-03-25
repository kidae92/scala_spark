package com.hongstudy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.functions._

object MovieSimilaritiesDataset {

  case class Movies(userid:Int, movieID:Int, rating:Int, timestamp:Long)
  case class MoviesNames(movieID:Int, movieTitle:String)
  case class MoviePairs(movie1:Int, movie2:Int, rating1:Int, rating2:Int)
  case class MovieParisSimilarity(movie1:Int, movie2:Int, score:Double, numPairs: Long)

  def computeCosineSimilarity(spark: SparkSession, data: Dataset[MoviePairs]): Dataset[MovieParisSimilarity] = {
    val pairScore = data
      .withColumn("xx", col("rating1")*col("rating1"))
      .withColumn("yy", col("rating2")*col("rating2"))
      .withColumn("xy", col("rating1")*col("rating2"))

    val calculateSimilarity = pairScore
      .groupBy("movie1", "movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx")))*sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn("score", when(col("denominator")=!=0, col("numerator")/col("denominator")).otherwise(null))
      .select("movie1", "movie2", "score", "numPairs").as[MovieParisSimilarity]

    result
  }
  def getMovieName(movieNames: Dataset[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(col("movieID") === movieId)
      .select("movieTitle").collect()(0)

    result(0).toString
  }



  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    val moviesNameSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("\nLoading movie names...")

    import spark.implicits._

    val movieNames = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNameSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val ratings = movies.select("userID","movieID", "rating")

    val moviesParis = ratings.as("ratings1")
      .join(ratings.as("rating2"), $"ratings1.userId" === $"ratings2.userId" && $"rating1.movieId" < "rating2.movieId")
      .select($"rating1.movieId".alias("movie1"),
        $"rating2.movieId".alias("movie2"),
        $"rating1.rating".alias("rating1"),
        $"rating2.rating".alias("rating2")
      ).as[MoviePairs]

    val moviePairSimilarities = computeCosineSimilarity(spark, moviesParis).cache()

    if (args.length>0){
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      val filteredResult = moviePairSimilarities.filter(
        (col("movie1") === movieID || col("movie2") === movieID) &&
          col("score") > scoreThreshold && col("numPairs")> coOccurrenceThreshold)

      val results = filteredResult.sort(col("score").desc).take(10)

      println("\nTop 10 similar movies for " + getMovieName(movieNames, movieID))
      for (result <- results){
        var similarMovieID = result.movie1
        if (similarMovieID == movieID){
          similarMovieID = result.movie2
        }
        println(getMovieName(movieNames, similarMovieID) + "\tscore: " + result.score + "\tstrength: " + result.numPairs)
      }
    }
  }
}
