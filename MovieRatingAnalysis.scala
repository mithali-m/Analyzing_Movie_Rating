package edu.neu.coe.csye7200.csv

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object MovieRatingAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Movie Rating Analysis")
      .master("local[*]")
      .getOrCreate()

    val filePath = "ratings.csv"

    val movieDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    val resultDF = calculateStats(movieDF)

    resultDF.show()

    spark.stop()
  }

  def calculateStats(df: DataFrame): DataFrame = {
    df.groupBy("movieId")
      .agg(
        avg("rating").alias("mean_rating"),
        stddev("rating").alias("stddev_rating")
      )
  }
}