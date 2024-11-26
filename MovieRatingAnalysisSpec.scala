package edu.neu.coe.csye7200.csv

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

class MovieRatingAnalysisSpec extends AnyFlatSpec {

  val spark = SparkSession.builder()
    .appName("TestMovieRatingAnalysis")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  "calculateStats" should "return correct mean and standard deviation" in {
    val testData = Seq(
      (1, 5.0), (1, 3.0), (1, 4.0),
      (2, 2.0), (2, 4.0),
      (10, 3.0), (10, 3.0), (10, 3.0)
    ).toDF("movieId", "rating")

    val result = MovieRatingAnalysis.calculateStats(testData).collect()

    assert(result.contains(Row(1, 4.0, 1.0)))
    assert(result.contains(Row(2, 3.0, 1.4142135623730951)))
    assert(result.contains(Row(10, 3.0, 0.0)))
  }
}

