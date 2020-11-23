package tp1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Ex_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/films.csv")

    //Question 2
    val renamed_df: DataFrame = df.withColumnRenamed("_c0", "nom_film")
      .withColumnRenamed("_c1", "nombre_vues")
      .withColumnRenamed("_c2", "note_film")
      .withColumnRenamed("_c3", "acteur_principal")

    //Question 3.2
    val films_ldc: DataFrame = renamed_df.filter(renamed_df("acteur_principal") === "Di Caprio")
    print("Il y a " + films_ldc.count() + " films de Leonardo Di Caprio")

    //Question 3.3
    val moy_notes_films_ldc: DataFrame = films_ldc.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moy_notes_films_ldc.show

    //Question 3.4
    val total_vues_films  = renamed_df.agg(sum("nombre_vues")).first.get(0).toString.toDouble
    val total_vues_films_ldc = films_ldc.agg(sum("nombre_vues")).first.get(0).toString.toDouble

    val pourcentage_vues_ldc: Double = total_vues_films_ldc / total_vues_films * 100
    print("Le pourcentage des vues des films de Di Caprio est de " + pourcentage_vues_ldc)

    //Question 3.5
    val moy_notes_par_acteur = renamed_df.groupBy( col1 = "acteur_principal").mean( colNames = "nombre_vues")
    moy_notes_par_acteur.show

    val moy_vues_par_acteur = renamed_df.groupBy( col1 = "acteur_principal").mean( colNames = "note_film")
    moy_vues_par_acteur.show

    //Question 4
    val pourcentage_vues = renamed_df.withColumn(colName = "pourcentage_de_vues", col(colName = "nombre_vues") / total_vues_films * 100)
    pourcentage_vues.show

  }
}
