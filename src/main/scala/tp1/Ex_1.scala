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
    
    //Question 5
    //Moyenne des notes par acteur
    val count: RDD[(String, Double)] = rdd.map(item => ((item.split(";")(3)), (item.split(";")(2).toDouble)))
    val withValue = count.mapValues(e => (1.0, e))
    val countSums = withValue.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    keyMeans.foreach(println)

    //Moyenne des vues par acteur
    val count2: RDD[(String, Double)] = rdd.map(item => ((item.split(";")(3)), (item.split(";")(1).toDouble)))
    val withValue2 = count2.mapValues(e => (1.0, e))
    val countSums2 = withValue2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans2 = countSums2.mapValues(avgCount => avgCount._2 / avgCount._1)
    keyMeans2.foreach(println)
  }
}
