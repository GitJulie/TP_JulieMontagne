package exercises

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exo_6 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val df: DataFrame  = sparkSession.read.option("inferSchema", true).option("header", true).csv( path = "data/data.csv")

    val transformField = udf((date: String) => {
      val formatDate = new SimpleDateFormat("yyyy-MM-dd")
      formatDate.format(new Date(date))
    })

    //Usage:
    val transformedDateDf = df.withColumn("Order Date", transformField(col("Order Date")))

    transformedDateDf.show

  }
}
