package exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exo_4 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val df: DataFrame  = sparkSession.read.option("inferSchema", true).option("header", true).csv( path = "data/data.csv")

    val question2: DataFrame = df.filter(col(colName = "Unit Price") >= 500).filter(col(colName = "Units Sold") >= 3000)
    print("Il y a " + question2.count() + " produits ayant un prix unitaire supérieur à 500 et plus de 3000 unités vendues")

    val question3: DataFrame = df.filter(col(colName = "Unit Price") >= 500).groupBy( col1 = "Unit Price").sum( colNames = "Units Sold")
    question3.show

    val question4: DataFrame = df.groupBy( col1 = "Item Type").mean( colNames = "Unit Price")
    question4.show

    val question5: DataFrame = df.withColumn( colName = "total_revenue",
      col(colName = "Unit Price") * col(colName = "Units Sold"))

    val question6: DataFrame = question5.withColumn( colName = "total_cost",
      col(colName = "Unit Cost") * col(colName = "Units Sold"))

    val question7: DataFrame = question6.withColumn( colName = "total_profit",
      col(colName = "total_revenue") - col(colName = "total_cost"))

    val question8: DataFrame = question7.withColumn("unit_price_discount", when(col("Units Sold") > 3000, col("Unit Price") * 0.7).otherwise(col("Unit Price") * 0.9))

    val question9: DataFrame = question8.withColumn("total_revenue", col("Units Sold") * col("unit_price_discount"))
    val question9bis: DataFrame = question9.withColumn("total_profit", when(col("Units Sold") > 3000, col("Units Sold") * (col("Unit Price") - col("Unit Cost"))))
    question9bis.show



  }
}
