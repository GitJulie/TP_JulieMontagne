package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Exo_5 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val df: DataFrame  = sparkSession.read.option("inferSchema", true).option("header", true).csv( path = "data/data.csv")

    val columnsList = df.columns
    val columnsWithtoutSpaces: Array[String] = columnsList.map(elem => elem.replaceAll(" ", "_"))
    //Array("a", "b", "c") ==> "a", "b", "c"

    val dfWithRightColumnNames = df.toDF(columnsWithtoutSpaces:_*)
    //dfWithRightColumnNames.show()
    dfWithRightColumnNames.write.partitionBy("Sales_Channel").mode(SaveMode.Overwrite).parquet("result")
    val parquetDF = sparkSession.read.parquet("result")
    parquetDF.printSchema()
    parquetDF.show

  }
}
