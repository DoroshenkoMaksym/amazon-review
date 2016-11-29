package xyz.doroshenko.grossum

import java.util.logging.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Maksym on 11/27/2016.
  */
case class Review(ProductId: String, ProfileName: String, Text: String)

object Review {
  private val masterName = "local"
  private val applicationName = "Review statistic"
  private val itemsNumber = 100

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def getStats() = {
    val sc = initSparkContext(masterName, applicationName)
    val inputPath = getClass.getResource("/Reviews.csv").getPath
    val inputFile = sc.textFile(inputPath)
    val reviews = inputFile.map { line =>
      val splitted = line.split(",")
      Review(splitted(1), splitted(3), splitted(9))
    }
    val mostActiveUsers = reviews.map(review => (review.ProfileName, 1)).reduceByKey(_ + _).takeOrdered(itemsNumber)(Ordering[Int].reverse.on(x => x._2))
    val mostCommentedItems = reviews.map(review => (review.ProductId, 1)).reduceByKey(_ + _).takeOrdered(itemsNumber)(Ordering[Int].reverse.on(x => x._2))

    mostActiveUsers.foreach(x => println(s"User - ${x._1} : ${x._2} comments"))
    mostCommentedItems.foreach(x => println(s"Product id - ${x._1} : ${x._2} comments"))
  }

  private def initSparkContext(master: String, appName: String): SparkContext = {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    new SparkContext(conf)
  }

}
