package spark.job1

import org.apache.spark.rdd.RDD
import spark.SessionSpark
import spark.spark.{Beers, Breweries, Reviews}

object TopBreweries extends SessionSpark {

  def executeJob(): Unit = {
    val beers = sparkSession.sparkContext.textFile("")
    val reviews = sparkSession.sparkContext.textFile("")
    val breweries = sparkSession.sparkContext.textFile("")

    val beersRDD = removeFirstRow(beers).map(Beers.extract)
    val breweriesRDD = removeFirstRow(breweries).map(Breweries.extract)
    val reviewsRDD = removeFirstRow(reviews).map(Reviews.extract)


  }

  private def removeFirstRow(rdd: RDD[String]): RDD[String] = {
    val firstRow = rdd.first()
    rdd.filter(!_.equals(firstRow))
  }
}
