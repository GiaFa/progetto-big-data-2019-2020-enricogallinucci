package spark.job2

import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.commonmethod.Common
import spark.{Beers, Breweries, Reviews, SessionSpark}

object TopBeers extends SessionSpark{

  private val LOW_SCORE = 0.1
  private val MEDIUM_SCORE = 0.3
  private val HIGH_SCORE = 0.8
  private val RESULT_PATH: String = "giovannim/dataset/output/datasetprogetto/spark/job2"

  def toPrint(values:(Int,(String,Int,Int,Int,Double))): String = {
    values._2._1.concat(" Quantità Birre Qualità Bassa : ").concat(values._2._2.toString)
      .concat(" Quantità Birre Qualità Media : ").concat(values._2._3.toString)
      .concat(" Quantità Birre Qualità Alta : ").concat(values._2._4.toString)
      .concat(" Per uno score di : ").concat(values._2._5.toString)
  }


  def executeJob(sparkSession: SparkSession, RANGE_SCORE_B: Broadcast[(Int,Int,Int)], nBirrerie:Int, minRecensioni:Int): Unit = {
    Common.verifyDirectory(sparkSession,new Path(RESULT_PATH))
    val (beers,reviews,breweries) = readFile(sparkSession)
    val beersRDD = removeFirstRow(beers).map(Beers.extract).keyBy(_.brewery_id).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val breweriesRDD = removeFirstRow(breweries).map(Breweries.extract).keyBy(_.id).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val reviewsRDD = removeFirstRow(reviews).map(Reviews.extract).keyBy(_.beer_id).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val reviewsRDDAveraged =   Common.filterAndAvgReviews(reviewsRDD,minRecensioni)
    val reviewsAverageClass = createClassAvg(reviewsRDDAveraged,RANGE_SCORE_B)
    val beersAndBreweriesJoin = filterBreweries(beersRDD,breweriesRDD)
    val rddFinal =  countBeerPerBreweries(beersAndBreweriesJoin,reviewsAverageClass,RANGE_SCORE_B)
    val rddWithName = searchNameBreweries(rddFinal,breweriesRDD)
    computeScore(rddWithName).zipWithIndex().filter(_._2 < nBirrerie).map(x => x._1).map(toPrint).saveAsTextFile(RESULT_PATH)
  }

  def removeFirstRow(rdd: RDD[String]): RDD[String] = {
    val firstRow = rdd.first()
    rdd.filter(!_.equals(firstRow))
  }

  def filterBreweries(rdd: RDD[(Int,Beers)],breweriesRDD: RDD[(Int,Breweries)]): RDD[(Int,(Beers,Breweries))] =
    rdd.join(breweriesRDD).map(x => (x._2._1.id,x._2))

  def createClassAvg(rdd: RDD[(Int, Double)],RANGE_SCORE_B: Broadcast[(Int,Int,Int)]): RDD[(Int, (Double, Int))] ={
      rdd.mapValues{
        case d if d <= RANGE_SCORE_B.value._1 => (d,RANGE_SCORE_B.value._1)
        case d if d <= RANGE_SCORE_B.value._2 => (d,RANGE_SCORE_B.value._2)
        case d => (d,RANGE_SCORE_B.value._3)
      }
  }

  def countBeerPerBreweries(beersAndBreweriesJoin: RDD[(Int, (Beers, Breweries))], reviewsAverageClass: RDD[(Int, (Double, Int))],RANGE_SCORE_B: Broadcast[(Int,Int,Int)]): RDD[(Int, (Int, Int, Int))] = {
    beersAndBreweriesJoin.join(reviewsAverageClass).map(x =>  (x._2._1._2.id, (x._2._1._1,x._2._1._2,x._2._2._2)))
      .aggregateByKey((0,0,0))({
        case (avg,count) if count._3==RANGE_SCORE_B.value._1 => (avg._1+1,avg._2,avg._3)
        case (avg,count) if count._3==RANGE_SCORE_B.value._2 => (avg._1,avg._2+1,avg._3)
        case (avg,count) if count._3==RANGE_SCORE_B.value._3 => (avg._1,avg._2,avg._3+1)
      },(temp,actual) => (temp._1+actual._1,temp._2+actual._2,temp._3+actual._3))

  }
  def searchNameBreweries(rddFinal:RDD[(Int, (Int, Int, Int))], breweries: RDD[(Int, Breweries)]): RDD[(Int, (String, Int, Int, Int))] ={
    rddFinal.join(breweries).mapValues(x=>(x._2.name,x._1._1,x._1._2,x._1._3))
  }


  def computeScore(rdd:  RDD[(Int, (String, Int, Int, Int))]):  RDD[(Int, (String, Int, Int, Int,Double))] = {
    val x = rdd.coalesce(1)
    val maxLow:Double = x.map(_._2._2).max()
    val maxMiddle:Double = x.map(_._2._3).max()
    val maxHigh:Double = x.map(_._2._4).max()

    x.mapValues(x => (x._1,x._2,x._3,x._4,getScore(x._2/maxLow,x._3/maxMiddle,x._4/maxHigh))).sortBy(x => (x._2._5,x._2._4,x._2._3,x._2._2),ascending = false )
  }

  def getScore(lowNormalized: Double, middleNormalized: Double, highNormalize:Double): Double ={
    (lowNormalized * LOW_SCORE) + (middleNormalized * MEDIUM_SCORE) + (highNormalize * HIGH_SCORE)
  }

}
