package tp1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Ex_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    println("Hello world")

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1
    val rdd = sparkSession.sparkContext.textFile("data/films.csv")

    //Question 2
    val films_ldc = rdd.filter(elem => elem.contains("Di Caprio"))
    print("Il y a " + films_ldc.count() + " films de Leonardo Di Caprio")

    print("\n")

    //Question 3
    val counts = films_ldc.map(item => (item.split(";")(2).toDouble) )
    val moyenne = counts.sum() / counts.count()
    print("La moyenne des films de Di Caprio est de " + moyenne)

    print("\n")

    //Question 4
    val total_vues_film_ldc = films_ldc.map(item => (item.split(";")(1).toDouble))
    val total_vues_film = rdd.map(item => (item.split(";")(1).toDouble))
    val pourcentage_vues_ldc = total_vues_film_ldc.sum() / total_vues_film.sum() * 100
    print("Le pourcentage des vues des films de Di Caprio est de " + pourcentage_vues_ldc)
  }
}
