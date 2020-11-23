package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exo_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val rdd = sparkSession.sparkContext.textFile("data/donnees_civiles.csv")

    val df: DataFrame  = sparkSession.read.option("inferSchema", true).option("header", true).csv( path = "data/data.csv")

    df.printSchema()
    df.show
  }
}
