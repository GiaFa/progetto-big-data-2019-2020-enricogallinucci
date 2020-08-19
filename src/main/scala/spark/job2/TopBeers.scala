package spark.job2

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import spark.{Beers, Breweries, Reviews, SessionSpark}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spark.commonmethod.Common

object TopBeers extends SessionSpark{
  def toPrint(values:(Int,(String,Int,Int,Int))): String = {
    values._2._1.concat(" Quantità Birre Qualità Bassa : ").concat(values._2._2.toString)
      .concat(" Quantità Birre Qualità Media : ").concat(values._2._3.toString)
      .concat(" Quantità Birre Qualità Alta : ").concat(values._2._4.toString)
  }

  def executeJob(sparkSession: SparkSession,RANGE_SCORE_B: Broadcast[(Int,Int,Int)], nBirrerie: Int = 20, minRecensioni: Int = 50): Unit = {
    val (beers,reviews,breweries) = readFile(sparkSession)
    val beersRDD = removeFirstRow(beers).map(Beers.extract).keyBy(_.brewery_id).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val breweriesRDD = removeFirstRow(breweries).map(Breweries.extract).keyBy(_.id).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val reviewsRDD = removeFirstRow(reviews).map(Reviews.extract).keyBy(_.beer_id).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val reviewsRDDAveraged =   Common.filterAndAvgReviews(reviewsRDD,minRecensioni)
    val reviewsAverageClass = createClassAvg(reviewsRDDAveraged,RANGE_SCORE_B)
    val beersAndBreweriesJoin = filterBreweries(beersRDD,breweriesRDD)
    val rddFinal =  countBeerPerBreweries(beersAndBreweriesJoin,reviewsAverageClass,RANGE_SCORE_B)
    searchNameBreweries(rddFinal,breweriesRDD)./*.zipWithIndex().filter(_._2 < nBirrerie).map(x => x._1).*/map(toPrint).saveAsTextFile("faspeeencina/datasets/output/project/spark/")
  }

  def removeFirstRow(rdd: RDD[String]): RDD[String] = {
    val firstRow = rdd.first()
    rdd.filter(!_.equals(firstRow))
  }

  def filterBreweries(rdd: RDD[(Int,Beers)],breweriesRDD: RDD[(Int,Breweries)]): RDD[(Int,(Beers,Breweries))] =
    rdd.join(breweriesRDD).map(x => (x._2._1.id,x._2))

  def createClassAvg(rdd: RDD[(Int, Double)],RANGE_SCORE_B: Broadcast[(Int,Int,Int)]): RDD[(Int, (Double, Int))] ={
      rdd.mapValues{
        case d if d <=RANGE_SCORE_B.value._1 => (d,RANGE_SCORE_B.value._1)
        case d if d <=RANGE_SCORE_B.value._2 => (d,RANGE_SCORE_B.value._2)
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
    rddFinal.join(breweries).mapValues(x=>(x._2.name,x._1._1,x._1._2,x._1._3)).coalesce(1).sortBy(_._2._4,ascending = false)
  }
 /* def verifyExistFile(): Unit ={
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    if( fs.exists(new Path("/path/on/hdfs/to/SUCCESS.txt"))){
      fs.delete(new Path("/path/on/hdfs/to/SUCCESS.txt"),false)
    }
  }*/

}
