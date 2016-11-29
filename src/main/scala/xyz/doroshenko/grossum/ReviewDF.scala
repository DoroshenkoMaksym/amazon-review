package xyz.doroshenko.grossum

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Maksym on 11/28/2016.
  */
object ReviewDF {

  def getStats() = {
    val spark = getSparkSession()
    val inputDf = spark.read.option("header", "true").csv(getClass.getResource("/Reviews.csv").getPath).cache()
    val mostActiveUsers = inputDf.groupBy("ProfileName").count().orderBy(desc("count")).show(100)
    val mostCommentedItems = inputDf.groupBy("ProductId").count().orderBy(desc("count")).show(100)
    val mostUsedWords = inputDf.select("Text").rdd.map(r => r.getAs[String](0)).flatMap(line => line.split("\\W+")).map(word => (word.toLowerCase, 1)).reduceByKey(_ + _).takeOrdered(100)(Ordering[Int].reverse.on(x => x._2))
    mostUsedWords.foreach(x => println(s"""Word "${x._1}" used ${x._2}"""))
  }

  private def getSparkSession() = {
    SparkSession
      .builder()
      .appName("Amazon review analysis")
      .master("local[4]")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    getStats
  }

}
