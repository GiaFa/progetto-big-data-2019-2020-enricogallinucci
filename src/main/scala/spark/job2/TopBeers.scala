package spark.job2

import spark.{Beers, Breweries, Reviews, SessionSpark}
import org.apache.spark.rdd.RDD

object TopBeers extends SessionSpark{
  private val RANGE_SCORE = (2,4,5)
  private val RANGE_SCORE_B = sparkSession.sparkContext.broadcast(RANGE_SCORE)

  def toPrint(values:(Int,(String,Int,Int,Int))): String = {
    values._2._1.concat(" Quantità Birre Qualità Bassa : ").concat(values._2._2.toString)
      .concat(" Quantità Birre Qualità Medie : ").concat(values._2._3.toString)
      .concat(" Quantità Birre Qualità Alta : ").concat(values._2._4.toString)
  }

  def executeJob(nBirrerie: Int = 20, nClass: Int = 5, minAvgScore: Int = 2, minRecensioni: Int = 50): Unit = {
    val (beers,reviews,breweries) = readFile()
    val beersRDD = removeFirstRow(beers).map(Beers.extract).keyBy(_.brewery_id).persist().cache()
    val breweriesRDD = removeFirstRow(breweries).map(Breweries.extract).keyBy(_.id).persist().cache()
    val reviewsRDD = removeFirstRow(reviews).map(Reviews.extract).keyBy(_.beer_id).persist().cache()
    val reviewsRDDAveraged =  filterAndAvgReviews(reviewsRDD,minRecensioni)
    val reviewsAverageClass = createClassAvg(reviewsRDDAveraged)
    val beersAndBreweriesJoin = filterBreweries(beersRDD,breweriesRDD)
    val rddFinal =  countBeerPerBreweries(beersAndBreweriesJoin,reviewsAverageClass)
    searchNameBreweries(rddFinal,breweriesRDD).map(toPrint).saveAsTextFile("faspeeencina/datasets/output/project/spark/")
  }


  def removeFirstRow(rdd: RDD[String]): RDD[String] = {
    val firstRow = rdd.first()
    rdd.filter(!_.equals(firstRow))
  }

  def filterBreweries(rdd: RDD[(Int,Beers)],breweriesRDD: RDD[(Int,Breweries)]): RDD[(Int,(Beers,Breweries))] =
    rdd.join(breweriesRDD).map(x => (x._2._1.id,x._2))

  def filterAndAvgReviews(rdd: RDD[(Int,Reviews)], nReviews: Int):RDD[(Int,Double)] = {
    val map = rdd.mapValues(_ => 1L).reduceByKey(_+_).filter(_._2 >= nReviews)
    map.join(rdd).map(x => (x._1,x._2._2))
      .aggregateByKey((0.0,0.0))((avg,count) => (avg._1 + count.overall, avg._2+1),(temp,actual) => (temp._1+actual._1,temp._2+actual._2))
      .mapValues(x => x._1 / x._2)
  }
  def createClassAvg(rdd: RDD[(Int, Double)]): RDD[(Int, (Double, Int))] ={
      rdd.mapValues{
        case d if d <=RANGE_SCORE_B.value._1 => (d,RANGE_SCORE_B.value._1)
        case d if d <=RANGE_SCORE_B.value._2 => (d,RANGE_SCORE_B.value._2)
        case d => (d,RANGE_SCORE_B.value._3)
      }
  }
  def countBeerPerBreweries(beersAndBreweriesJoin: RDD[(Int, (Beers, Breweries))], reviewsAverageClass: RDD[(Int, (Double, Int))]): RDD[(Int, (Int, Int, Int))] = {
    beersAndBreweriesJoin.join(reviewsAverageClass).map(x => (x._2._1._2.id, (x._2._1._1,x._2._1._2,x._2._2._2)))
      .aggregateByKey((0,0,0))({
        case (avg,count) if count._3==RANGE_SCORE_B.value._1 => (avg._1+1,avg._2,avg._3)
        case (avg,count) if count._3==RANGE_SCORE_B.value._2 => (avg._1,avg._2+1,avg._3)
        case (avg,count) if count._3==RANGE_SCORE_B.value._3 => (avg._1,avg._2,avg._3+1)
      },(temp,actual) => (temp._1+actual._1,temp._2+actual._2,temp._3+actual._3))

  }
  def searchNameBreweries(rddFinal:RDD[(Int, (Int, Int, Int))], breweries: RDD[(Int, Breweries)]): RDD[(Int, (String, Int, Int, Int))] ={
    rddFinal.join(breweries).mapValues(x=>(x._2.name,x._1._1,x._1._2,x._1._3))
  }
 /* def verifyExistFile(): Unit ={
    val conf = sparkSession.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    if( fs.exists(new Path("/path/on/hdfs/to/SUCCESS.txt"))){
      fs.delete(new Path("/path/on/hdfs/to/SUCCESS.txt"),false)
    }
  }*/

}
